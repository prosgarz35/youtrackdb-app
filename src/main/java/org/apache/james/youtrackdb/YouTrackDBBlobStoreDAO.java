package org.apache.james.youtrackdb;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Collection;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.Map;
import java.util.Set;

import jakarta.inject.Inject;

import org.apache.james.blob.api.BlobId;
import org.apache.james.blob.api.BlobStoreDAO;
import org.apache.james.blob.api.BucketName;
import org.apache.james.blob.api.ObjectNotFoundException;
import org.apache.james.blob.api.ObjectStoreIOException;
import org.apache.james.filesystem.api.FileSystem;
import org.reactivestreams.Publisher;

import com.google.common.base.Preconditions;
import com.jetbrains.youtrackdb.api.exception.RecordDuplicatedException;
import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Tiered BlobStoreDAO for YouTrackDB. The tier is chosen from the payload size:
 * - up to 4 KB: stored raw in a {@code JamesBlob} vertex (INLINE_RAW);
 * - 4 KB to 64 KB: Zstd-compressed (level 1) in the vertex (INLINE_ZSTD);
 * - above 64 KB: streamed as Zstd to {@code var/blobs/{bucket}/ab/cd/ef/{blobId}} (FILE_ZSTD); the vertex only
 *   keeps the storage type. Heap usage stays bounded: a 64 KB probe buffer plus an 8 KB transfer buffer.
 *
 * Plain identifiers are expected to be content hashes (DeDuplicationBlobStore), so saving one that already exists
 * is a no-op. Identifiers chosen by the caller (not plain path segments) are overwritten when saved again.
 */
public class YouTrackDBBlobStoreDAO implements BlobStoreDAO {
    static final String CLASS_NAME = "JamesBlob";
    private static final String PROP_BUCKET = "bucket";
    private static final String PROP_BLOB_ID = "blobId";
    private static final String PROP_KEY = "bucketAndBlobId";
    private static final String PROP_PAYLOAD = "payload";
    private static final String PROP_STORAGE_TYPE = "storageType"; // "INLINE_RAW", "INLINE_ZSTD", "FILE_ZSTD"
    private static final String STORAGE_INLINE_RAW = "INLINE_RAW";
    private static final String STORAGE_INLINE_ZSTD = "INLINE_ZSTD";
    private static final String STORAGE_FILE_ZSTD = "FILE_ZSTD";

    // Legacy fallback compatibility
    private static final String LEGACY_STORAGE_INLINE = "INLINE";
    private static final String LEGACY_STORAGE_FILE = "FILE";

    private static final int TIER1_RAW_THRESHOLD = 4 * 1024;    // 4 KB: raw in DB (no compress, no dedup)
    private static final int TIER2_DB_THRESHOLD = 64 * 1024;   // 64 KB: Zstd in DB (page size limit)

    private final YTDBGraphTraversalSource g;
    private final BlobId.Factory blobIdFactory;
    private final File blobsDirectory;

    @Inject
    public YouTrackDBBlobStoreDAO(YTDBGraphTraversalSource g, BlobId.Factory blobIdFactory, FileSystem fileSystem) {
        this.g = g;
        this.blobIdFactory = blobIdFactory;
        try {
            this.blobsDirectory = new File(fileSystem.getBasedir(), "var/blobs");
            if (!this.blobsDirectory.exists()) {
                this.blobsDirectory.mkdirs();
            }
        } catch (Exception e) {
            throw new RuntimeException("Failed to initialize blobs directory", e);
        }
    }

    private String buildKey(BucketName bucketName, BlobId blobId) {
        return bucketName.asString() + "/" + blobId.asString();
    }

    /**
     * A plain id is a content hash (DeDuplicationBlobStore): saving it again is a no-op. Any other id is chosen
     * by the caller (a mail repository stores the same mail name again with new content): saving it again
     * replaces the stored content, like the memory, S3 and Cassandra implementations do.
     */
    private static boolean isOverwritable(BlobId blobId) {
        return !isPlainSegment(blobId.asString());
    }

    private void insertBlobRecord(BucketName bucketName, BlobId blobId, String storageType, byte[] payload) {
        String key = buildKey(bucketName, blobId);
        try {
            YouTrackDBTransactions.executeStrictTx(g, tx -> addBlobVertex(tx, bucketName, blobId, key, storageType, payload));
        } catch (RuntimeException e) {
            if (!YouTrackDBTransactions.hasCause(e, RecordDuplicatedException.class)) {
                throw e;
            }
            if (isOverwritable(blobId)) {
                YouTrackDBTransactions.executeStrictTx(g, tx -> {
                    var existing = tx.V().hasLabel(CLASS_NAME).has(PROP_KEY, key);
                    if (existing.hasNext()) {
                        var vertex = existing.next();
                        vertex.property(PROP_STORAGE_TYPE, storageType);
                        vertex.property(PROP_PAYLOAD, payload);
                    } else {
                        addBlobVertex(tx, bucketName, blobId, key, storageType, payload);
                    }
                });
            }
        }
    }

    private static void addBlobVertex(YTDBGraphTraversalSource tx, BucketName bucketName, BlobId blobId, String key,
                                      String storageType, byte[] payload) {
        tx.addV(CLASS_NAME)
            .property(PROP_BUCKET, bucketName.asString())
            .property(PROP_BLOB_ID, blobId.asString())
            .property(PROP_KEY, key)
            .property(PROP_STORAGE_TYPE, storageType)
            .property(PROP_PAYLOAD, payload)
            .iterate();
    }

    /** An overwritten blob that moved from the file tier to an inline tier must not leave its old file behind. */
    private void deleteStaleFile(BucketName bucketName, BlobId blobId) {
        if (isOverwritable(blobId)) {
            File stale = getFileForBlob(bucketName, blobId);
            if (stale.exists() && stale.delete()) {
                pruneEmptyParentDirectories(stale, pruneStopDir(bucketName, blobId));
            }
        }
    }

    /** Root (under var/blobs) of the files whose names cannot be used as path segments. */
    static final String HASHED_ROOT = ".hashed";

    /** A name is used as a path segment only if it cannot leave its directory or be mistaken for one. */
    private static boolean isPlainSegment(String name) {
        return !name.isEmpty()
            && !".".equals(name)
            && !name.contains("..")
            && name.indexOf('/') < 0
            && name.indexOf('\\') < 0
            && name.indexOf('\0') < 0;
    }

    private static boolean isPlainBucket(String bucket) {
        return isPlainSegment(bucket) && !HASHED_ROOT.equals(bucket);
    }

    static String sha256Hex(String value) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is required by the JDK", e);
        }
    }

    /**
     * Names of the files that must be kept by the orphan garbage collection: the plain blob ids, and the
     * hashed names used for ids that cannot be path segments (for example "var/mail/error/mailMetadata/x").
     */
    public static java.util.Set<String> fileNamesToKeep(Collection<String> blobIds) {
        java.util.Set<String> names = new HashSet<>();
        for (String blobId : blobIds) {
            names.add(blobId);
            names.add(sha256Hex(blobId));
        }
        return names;
    }

    /** var/blobs/{bucket}; a bucket that is not a plain segment gets a hashed directory instead of a traversal. */
    private File bucketDirOf(BucketName bucketName) {
        String bucket = bucketName.asString();
        if (isPlainBucket(bucket)) {
            return new File(blobsDirectory, bucket);
        }
        return new File(new File(blobsDirectory, HASHED_ROOT), "h-" + sha256Hex(bucket));
    }

    /** var/blobs/.hashed/{p-bucket | h-hash}: where the files of non-plain ids of this bucket live. */
    private File hashedBucketDirOf(BucketName bucketName) {
        String bucket = bucketName.asString();
        String bucketKey = isPlainBucket(bucket) ? "p-" + bucket : "h-" + sha256Hex(bucket);
        return new File(new File(blobsDirectory, HASHED_ROOT), bucketKey);
    }

    /** Directory above which empty parents are not pruned. */
    private File pruneStopDir(BucketName bucketName, BlobId blobId) {
        return isPlainSegment(blobId.asString()) ? bucketDirOf(bucketName) : hashedBucketDirOf(bucketName);
    }

    /**
     * Resolves the filesystem location for a blob.
     * A plain id uses 3-level directory sharding: var/blobs/{bucket}/{p1}/{p2}/{p3}/{blobId}
     * (blob "abcdef123456" -> var/blobs/{bucket}/ab/cd/ef/abcdef123456), and the flat legacy location
     * var/blobs/{bucket}/{blobId} is still read.
     * An id that cannot be a path segment (it contains "/", "\\" or ".."; the blob id factory of a mail
     * repository builds ids like "var/mail/error/mailMetadata/{name}") is stored under
     * var/blobs/.hashed/{bucket}/{sha256(id)}, so it can never leave the blobs directory.
     */
    private File getFileForBlob(BucketName bucketName, BlobId blobId) {
        String id = blobId.asString();
        if (!isPlainSegment(id)) {
            return new File(hashedBucketDirOf(bucketName), sha256Hex(id));
        }
        File bucketDir = bucketDirOf(bucketName);

        if (id.length() >= 6) {
            String p1 = id.substring(0, 2);
            String p2 = id.substring(2, 4);
            String p3 = id.substring(4, 6);
            File sharded = new File(new File(new File(bucketDir, p1), p2), p3);
            File shardedFile = new File(sharded, id);
            if (shardedFile.exists()) {
                return shardedFile;
            }
            // Check legacy flat location if file exists there
            File flatFile = new File(bucketDir, id);
            if (flatFile.exists()) {
                return flatFile;
            }
            // Return sharded destination for new writes
            return shardedFile;
        }

        return new File(bucketDir, id);
    }

    private static void deleteTree(File dir) {
        if (!dir.exists()) {
            return;
        }
        try (var stream = Files.walk(dir.toPath())) {
            stream.map(java.nio.file.Path::toFile)
                .sorted((o1, o2) -> -o1.compareTo(o2))
                .forEach(File::delete);
        } catch (Exception ignored) {
        }
    }

    private void pruneEmptyParentDirectories(File file, File stopDir) {
        try {
            File parent = file.getParentFile();
            while (parent != null && !parent.equals(stopDir) && parent.getAbsolutePath().startsWith(stopDir.getAbsolutePath())) {
                String[] list = parent.list();
                if (list != null && list.length == 0) {
                    if (!parent.delete()) {
                        break;
                    }
                    parent = parent.getParentFile();
                } else {
                    break;
                }
            }
        } catch (Exception ignored) {
        }
    }

    private record BlobMeta(String storageType, byte[] payload) {
    }

    /** Reads the vertex in a short transaction; disk IO and decompression happen after it is closed. */
    private BlobMeta loadMeta(String key) {
        return g.computeInTx(tx -> {
            var traversal = tx.V().hasLabel(CLASS_NAME).has(PROP_KEY, key);
            if (!traversal.hasNext()) {
                return null;
            }
            var vertex = traversal.next();
            String storageType = vertex.property(PROP_STORAGE_TYPE).isPresent() ? vertex.value(PROP_STORAGE_TYPE) : STORAGE_INLINE_RAW;
            byte[] payload = vertex.value(PROP_PAYLOAD);
            return new BlobMeta(storageType, payload);
        });
    }

    @Override
    public InputStreamBlob read(BucketName bucketName, BlobId blobId) throws ObjectStoreIOException, ObjectNotFoundException {
        return Mono.from(readReactive(bucketName, blobId)).block();
    }

    @Override
    public Publisher<InputStreamBlob> readReactive(BucketName bucketName, BlobId blobId) {
        return Mono.fromCallable(() -> {
            String key = buildKey(bucketName, blobId);
            BlobMeta meta = loadMeta(key);

            if (meta == null) {
                return null;
            }

            String storageType = meta.storageType();
            if (STORAGE_FILE_ZSTD.equals(storageType) || LEGACY_STORAGE_FILE.equals(storageType)) {
                File file = getFileForBlob(bucketName, blobId);
                if (!file.exists()) {
                    throw new ObjectNotFoundException("Blob file missing on disk: " + file.getAbsolutePath());
                }
                InputStream in = null;
                try {
                    in = new FileInputStream(file);
                    if (STORAGE_FILE_ZSTD.equals(storageType)) {
                        return InputStreamBlob.of(new com.github.luben.zstd.ZstdInputStream(in));
                    }
                    return InputStreamBlob.of(in);
                } catch (IOException e) {
                    if (in != null) {
                        try {
                            in.close();
                        } catch (IOException ignored) {}
                    }
                    throw new ObjectStoreIOException("Error opening blob file: " + file.getAbsolutePath(), e);
                }
            } else if (STORAGE_INLINE_ZSTD.equals(storageType)) {
                byte[] compressed = meta.payload();
                if (compressed == null || compressed.length == 0) {
                    return InputStreamBlob.of(new ByteArrayInputStream(new byte[0]));
                }
                try {
                    return InputStreamBlob.of(new com.github.luben.zstd.ZstdInputStream(new ByteArrayInputStream(compressed)));
                } catch (Exception e) {
                    throw new ObjectStoreIOException("Error decompressing inline blob: " + key, e);
                }
            } else {
                byte[] bytes = meta.payload();
                if (bytes == null) {
                    bytes = new byte[0];
                }
                return InputStreamBlob.of(new ByteArrayInputStream(bytes));
            }
        })
        .switchIfEmpty(Mono.error(() -> new ObjectNotFoundException("Blob not found: " + blobId.asString() + " in bucket: " + bucketName.asString())))
        .subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic());
    }

    @Override
    public Publisher<BytesBlob> readBytes(BucketName bucketName, BlobId blobId) {
        return Mono.fromCallable(() -> {
            String key = buildKey(bucketName, blobId);
            BlobMeta meta = loadMeta(key);

            if (meta == null) {
                return null;
            }

            String storageType = meta.storageType();
            if (STORAGE_INLINE_RAW.equals(storageType) || LEGACY_STORAGE_INLINE.equals(storageType)) {
                byte[] bytes = meta.payload();
                return BytesBlob.of(bytes != null ? bytes : new byte[0]);
            } else if (STORAGE_INLINE_ZSTD.equals(storageType)) {
                byte[] compressed = meta.payload();
                if (compressed == null || compressed.length == 0) {
                    return BytesBlob.of(new byte[0]);
                }
                long decompressedSize = com.github.luben.zstd.Zstd.decompressedSize(compressed);
                if (decompressedSize > 0 && decompressedSize <= TIER2_DB_THRESHOLD * 2) {
                    byte[] decompressed = com.github.luben.zstd.Zstd.decompress(compressed, (int) decompressedSize);
                    return BytesBlob.of(decompressed);
                }
                try (var is = new com.github.luben.zstd.ZstdInputStream(new ByteArrayInputStream(compressed))) {
                    return BytesBlob.of(is.readAllBytes());
                } catch (IOException e) {
                    throw new ObjectStoreIOException("Error decompressing inline blob: " + key, e);
                }
            } else if (STORAGE_FILE_ZSTD.equals(storageType) || LEGACY_STORAGE_FILE.equals(storageType)) {
                File file = getFileForBlob(bucketName, blobId);
                if (!file.exists()) {
                    throw new ObjectNotFoundException("Blob file missing on disk: " + file.getAbsolutePath());
                }
                try (InputStream in = new FileInputStream(file)) {
                    if (STORAGE_FILE_ZSTD.equals(storageType)) {
                        try (var zis = new com.github.luben.zstd.ZstdInputStream(in)) {
                            return BytesBlob.of(zis.readAllBytes());
                        }
                    }
                    return BytesBlob.of(in.readAllBytes());
                } catch (IOException e) {
                    throw new ObjectStoreIOException("Error reading blob file: " + file.getAbsolutePath(), e);
                }
            }
            return BytesBlob.of(new byte[0]);
        })
        .switchIfEmpty(Mono.error(() -> new ObjectNotFoundException("Blob not found: " + blobId.asString() + " in bucket: " + bucketName.asString())))
        .subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic());
    }

    @Override
    public Publisher<Void> save(BucketName bucketName, BlobId blobId, Blob blob) {
        Preconditions.checkNotNull(blob);
        return Mono.<Void>fromRunnable(() -> {
            try {
                try (InputStream in = blob.asInputStream().payload()) {
                    // Read up to TIER2_DB_THRESHOLD + 1 bytes to determine tier without buffering huge payloads
                    byte[] initialBuffer = new byte[TIER2_DB_THRESHOLD + 1];
                    int totalRead = 0;
                    while (totalRead < initialBuffer.length) {
                        int r = in.read(initialBuffer, totalRead, initialBuffer.length - totalRead);
                        if (r == -1) {
                            break;
                        }
                        totalRead += r;
                    }

                    if (totalRead <= TIER1_RAW_THRESHOLD) {
                        // Tier 1: <= 4 KB -> Inline raw into YouTrackDB
                        byte[] data = java.util.Arrays.copyOf(initialBuffer, totalRead);
                        insertBlobRecord(bucketName, blobId, STORAGE_INLINE_RAW, data);
                        deleteStaleFile(bucketName, blobId);
                    } else if (totalRead <= TIER2_DB_THRESHOLD) {
                        // Tier 2: 4 KB .. 64 KB -> Compress (Zstd level 1) and insert into DB
                        byte[] data = java.util.Arrays.copyOf(initialBuffer, totalRead);
                        byte[] compressed = com.github.luben.zstd.Zstd.compress(data, 1);
                        insertBlobRecord(bucketName, blobId, STORAGE_INLINE_ZSTD, compressed);
                        deleteStaleFile(bucketName, blobId);
                    } else {
                        // Tier 3: > 64 KB -> Stream to file storage with Zstd compression
                        File file = getFileForBlob(bucketName, blobId);
                        File parent = file.getParentFile();
                        if (!parent.exists()) {
                            parent.mkdirs();
                        }

                        boolean overwrite = isOverwritable(blobId);
                        if (overwrite || !file.exists()) {
                            File tempFile = new File(parent, file.getName() + ".tmp." + Thread.currentThread().threadId());
                            try (FileOutputStream fos = new FileOutputStream(tempFile);
                                 com.github.luben.zstd.ZstdOutputStream zos = new com.github.luben.zstd.ZstdOutputStream(fos, 1)) {
                                zos.write(initialBuffer, 0, totalRead);
                                byte[] transferBuf = new byte[8192];
                                int bytesRead;
                                while ((bytesRead = in.read(transferBuf)) != -1) {
                                    zos.write(transferBuf, 0, bytesRead);
                                }
                                zos.flush();
                                fos.getFD().sync();
                            }
                            try {
                                Files.move(tempFile.toPath(), file.toPath(), java.nio.file.StandardCopyOption.ATOMIC_MOVE, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                            } catch (Exception e) {
                                if (overwrite) {
                                    tempFile.delete();
                                    throw e;
                                }
                                if (!file.exists()) {
                                    Files.move(tempFile.toPath(), file.toPath(), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                                } else {
                                    tempFile.delete();
                                }
                            }
                        }

                        insertBlobRecord(bucketName, blobId, STORAGE_FILE_ZSTD, new byte[0]);
                    }
                }
            } catch (Exception e) {
                throw new ObjectStoreIOException("Error saving blob " + blobId.asString(), e);
            }
        }).subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic());
    }

    @Override
    public Publisher<Void> delete(BucketName bucketName, BlobId blobId) {
        return Mono.<Void>fromRunnable(() -> {
            String key = buildKey(bucketName, blobId);
            File file = getFileForBlob(bucketName, blobId);
            // Database first: a failure leaves an orphan file (collected by GC), not a record pointing to a missing file.
            YouTrackDBTransactions.executeStrictTx(g, tx ->
                tx.command("DELETE VERTEX JamesBlob WHERE bucketAndBlobId = :key", "key", key));
            if (file.exists()) {
                file.delete();
                pruneEmptyParentDirectories(file, pruneStopDir(bucketName, blobId));
            }
        }).subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic());
    }

    @Override
    public Publisher<Void> delete(BucketName bucketName, Collection<BlobId> blobIds) {
        return Mono.<Void>fromRunnable(() -> {
            Map<BlobId, File> files = new java.util.LinkedHashMap<>();
            for (BlobId blobId : blobIds) {
                files.put(blobId, getFileForBlob(bucketName, blobId));
            }
            YouTrackDBTransactions.executeStrictTx(g, tx -> {
                for (BlobId blobId : blobIds) {
                    tx.command("DELETE VERTEX JamesBlob WHERE bucketAndBlobId = :key", "key", buildKey(bucketName, blobId));
                }
            });
            files.forEach((blobId, file) -> {
                if (file.exists()) {
                    file.delete();
                    pruneEmptyParentDirectories(file, pruneStopDir(bucketName, blobId));
                }
            });
        }).subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic());
    }

    @Override
    public Publisher<Void> deleteBucket(BucketName bucketName) {
        return Mono.<Void>fromRunnable(() -> {
            YouTrackDBTransactions.executeStrictTx(g, tx ->
                tx.command("DELETE VERTEX JamesBlob WHERE bucket = :bucket", "bucket", bucketName.asString()));
            deleteTree(bucketDirOf(bucketName));
            deleteTree(hashedBucketDirOf(bucketName));
        }).subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic());
    }

    @Override
    public Publisher<BucketName> listBuckets() {
        return Mono.fromCallable(() -> {
            return g.computeInTx(tx -> {
                Set<BucketName> buckets = new HashSet<>();
                var list = tx.yql("SELECT DISTINCT(bucket) AS bucket FROM JamesBlob").toList();
                for (Object item : list) {
                    if (item instanceof Map<?, ?> m) {
                        Object b = m.get("bucket");
                        if (b != null) {
                            buckets.add(BucketName.of(b.toString()));
                        }
                    }
                }
                return buckets;
            });
        }).flatMapMany(Flux::fromIterable)
        .subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic());
    }

    @Override
    public Publisher<BlobId> listBlobs(BucketName bucketName) {
        return listBlobs(bucketName, null);
    }

    /**
     * Blobs whose id starts with {@code prefix}, as a range scan on the (bucket, blobId) index: no LIKE, so no
     * wildcard characters to escape. A null or empty prefix lists the whole bucket.
     */
    public Publisher<BlobId> listBlobs(BucketName bucketName, String prefix) {
        return Mono.fromCallable(() -> g.computeInTx(tx -> {
                Set<BlobId> blobIds = new HashSet<>();
                for (Object item : prefixQuery(tx, "blobId", bucketName, prefix)) {
                    if (item instanceof Map<?, ?> m && m.get(PROP_BLOB_ID) != null) {
                        blobIds.add(blobIdFactory.of(m.get(PROP_BLOB_ID).toString()));
                    }
                }
                return blobIds;
            }))
            .flatMapMany(Flux::fromIterable)
            .subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic());
    }

    public Mono<Long> countBlobs(BucketName bucketName, String prefix) {
        return Mono.fromCallable(() -> g.computeInTx(tx -> {
                var results = prefixQuery(tx, "count(*) AS cnt", bucketName, prefix);
                if (!results.isEmpty() && results.getFirst() instanceof Map<?, ?> m && m.get("cnt") instanceof Number n) {
                    return n.longValue();
                }
                return 0L;
            }))
            .subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic());
    }

    private static java.util.List<?> prefixQuery(YTDBGraphTraversalSource tx, String projection, BucketName bucketName, String prefix) {
        String select = "SELECT " + projection + " FROM JamesBlob WHERE bucket = :bucket";
        if (prefix == null || prefix.isEmpty()) {
            return tx.yql(select, "bucket", bucketName.asString()).toList();
        }
        return tx.yql(select + " AND blobId >= :lo AND blobId < :hi",
            "bucket", bucketName.asString(), "lo", prefix, "hi", prefixUpperBound(prefix)).toList();
    }

    /** The smallest string greater than every string starting with {@code prefix}. */
    static String prefixUpperBound(String prefix) {
        int last = prefix.length() - 1;
        return prefix.substring(0, last) + (char) (prefix.charAt(last) + 1);
    }
}
