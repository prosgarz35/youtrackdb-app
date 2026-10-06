package org.apache.james.youtrackdb;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Collection;
import java.util.HashSet;
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
import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Hybrid BlobStoreDAO for YouTrackDB:
 * - Small payloads (<= 16 KB) are stored directly inside YouTrackDB pages as inline byte arrays (low latency, 0 file IO).
 * - Large payloads (> 16 KB) are streamed to structured file storage (var/blobs/{bucket}/{blobId}), preventing
 *   WAL bloat, memory copy overhead and page fragmentation inside the database engine.
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
     * Resolves the filesystem location for a blob using 3-level directory sharding:
     * var/blobs/{bucket}/{p1}/{p2}/{p3}/{blobId}
     * For example, blob "abcdef123456" -> var/blobs/{bucket}/ab/cd/ef/abcdef123456
     * Also checks flat legacy location (var/blobs/{bucket}/{blobId}) for backward compatibility.
     */
    private File getFileForBlob(BucketName bucketName, BlobId blobId) {
        File bucketDir = new File(blobsDirectory, bucketName.asString());
        String id = blobId.asString();

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

    @Override
    public InputStreamBlob read(BucketName bucketName, BlobId blobId) throws ObjectStoreIOException, ObjectNotFoundException {
        return Mono.from(readReactive(bucketName, blobId)).block();
    }

    @Override
    public Publisher<InputStreamBlob> readReactive(BucketName bucketName, BlobId blobId) {
        return Mono.fromCallable(() -> {
            String key = buildKey(bucketName, blobId);
            return g.computeInTx(tx -> {
                var traversal = tx.V().hasLabel(CLASS_NAME).has(PROP_KEY, key);
                if (!traversal.hasNext()) {
                    return null;
                }
                Vertex v = traversal.next();
                String storageType = v.property(PROP_STORAGE_TYPE).isPresent() ? v.value(PROP_STORAGE_TYPE) : STORAGE_INLINE_RAW;

                if (STORAGE_FILE_ZSTD.equals(storageType) || LEGACY_STORAGE_FILE.equals(storageType)) {
                    File file = getFileForBlob(bucketName, blobId);
                    if (!file.exists()) {
                        throw new ObjectNotFoundException("Blob file missing on disk: " + file.getAbsolutePath());
                    }
                    try {
                        InputStream in = new FileInputStream(file);
                        if (STORAGE_FILE_ZSTD.equals(storageType)) {
                            return InputStreamBlob.of(new com.github.luben.zstd.ZstdInputStream(in));
                        }
                        return InputStreamBlob.of(in);
                    } catch (IOException e) {
                        throw new ObjectStoreIOException("Error opening blob file: " + file.getAbsolutePath(), e);
                    }
                } else if (STORAGE_INLINE_ZSTD.equals(storageType)) {
                    byte[] compressed = v.value(PROP_PAYLOAD);
                    if (compressed == null || compressed.length == 0) {
                        return InputStreamBlob.of(new ByteArrayInputStream(new byte[0]));
                    }
                    try {
                        return InputStreamBlob.of(new com.github.luben.zstd.ZstdInputStream(new ByteArrayInputStream(compressed)));
                    } catch (Exception e) {
                        throw new ObjectStoreIOException("Error decompressing inline blob: " + key, e);
                    }
                } else {
                    // STORAGE_INLINE_RAW or LEGACY_STORAGE_INLINE
                    byte[] bytes = v.value(PROP_PAYLOAD);
                    if (bytes == null) {
                        bytes = new byte[0];
                    }
                    return InputStreamBlob.of(new ByteArrayInputStream(bytes));
                }
            });
        })
        .onErrorResume(e -> {
            if (e.getCause() instanceof ObjectNotFoundException) {
                return Mono.error(e.getCause());
            }
            if (e.getCause() instanceof ObjectStoreIOException) {
                return Mono.error(e.getCause());
            }
            return Mono.error(e);
        })
        .switchIfEmpty(Mono.error(() -> new ObjectNotFoundException("Blob not found: " + blobId.asString() + " in bucket: " + bucketName.asString())))
        .subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic());
    }

    @Override
    public Publisher<BytesBlob> readBytes(BucketName bucketName, BlobId blobId) {
        return Mono.fromCallable(() -> {
            String key = buildKey(bucketName, blobId);
            return g.computeInTx(tx -> {
                var traversal = tx.V().hasLabel(CLASS_NAME).has(PROP_KEY, key);
                if (!traversal.hasNext()) {
                    return null;
                }
                Vertex v = traversal.next();
                String storageType = v.property(PROP_STORAGE_TYPE).isPresent() ? v.value(PROP_STORAGE_TYPE) : STORAGE_INLINE_RAW;

                if (STORAGE_INLINE_RAW.equals(storageType) || LEGACY_STORAGE_INLINE.equals(storageType)) {
                    byte[] bytes = v.value(PROP_PAYLOAD);
                    return BytesBlob.of(bytes != null ? bytes : new byte[0]);
                } else if (STORAGE_INLINE_ZSTD.equals(storageType)) {
                    byte[] compressed = v.value(PROP_PAYLOAD);
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
            });
        })
        .onErrorResume(e -> {
            if (e.getCause() instanceof ObjectNotFoundException) {
                return Mono.error(e.getCause());
            }
            if (e.getCause() instanceof ObjectStoreIOException) {
                return Mono.error(e.getCause());
            }
            return Mono.error(e);
        })
        .switchIfEmpty(Mono.error(() -> new ObjectNotFoundException("Blob not found: " + blobId.asString() + " in bucket: " + bucketName.asString())))
        .subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic());
    }

    @Override
    public Publisher<Void> save(BucketName bucketName, BlobId blobId, Blob blob) {
        Preconditions.checkNotNull(blob);
        return Mono.<Void>fromRunnable(() -> {
            try {
                byte[] data = blob.asInputStream().payload().readAllBytes();
                String key = buildKey(bucketName, blobId);

                if (data.length < TIER1_RAW_THRESHOLD) {
                    // Tier 1: < 4 KB -> Inline raw into YouTrackDB
                    g.executeInTx(tx -> {
                        var traversal = tx.V().hasLabel(CLASS_NAME).has(PROP_KEY, key);
                        if (!traversal.hasNext()) {
                            tx.addV(CLASS_NAME)
                                .property(PROP_BUCKET, bucketName.asString())
                                .property(PROP_BLOB_ID, blobId.asString())
                                .property(PROP_KEY, key)
                                .property(PROP_STORAGE_TYPE, STORAGE_INLINE_RAW)
                                .property(PROP_PAYLOAD, data)
                                .iterate();
                        }
                    });
                } else if (data.length <= TIER2_DB_THRESHOLD) {
                    // Tier 2: 4 KB .. 64 KB -> Compress (Zstd level 1 for lowest CPU overhead) and deduplicate in DB
                    g.executeInTx(tx -> {
                        var traversal = tx.V().hasLabel(CLASS_NAME).has(PROP_KEY, key);
                        if (!traversal.hasNext()) {
                            byte[] compressed = com.github.luben.zstd.Zstd.compress(data, 1);
                            tx.addV(CLASS_NAME)
                                .property(PROP_BUCKET, bucketName.asString())
                                .property(PROP_BLOB_ID, blobId.asString())
                                .property(PROP_KEY, key)
                                .property(PROP_STORAGE_TYPE, STORAGE_INLINE_ZSTD)
                                .property(PROP_PAYLOAD, compressed)
                                .iterate();
                        }
                    });
                } else {
                    // Tier 3: > 64 KB -> Compress (Zstd level 1) and stream to filesystem with atomic rename (deduplicated)
                    File file = getFileForBlob(bucketName, blobId);
                    File parent = file.getParentFile();
                    if (!parent.exists()) {
                        parent.mkdirs();
                    }

                    // Content-addressed deduplication: if file already exists with same blobId, skip disk write
                    if (!file.exists()) {
                        File tempFile = new File(parent, blobId.asString() + ".tmp." + Thread.currentThread().threadId());
                        try (FileOutputStream fos = new FileOutputStream(tempFile);
                             com.github.luben.zstd.ZstdOutputStream zos = new com.github.luben.zstd.ZstdOutputStream(fos, 1)) {
                            zos.write(data);
                            zos.flush();
                            fos.getFD().sync(); // Ensure durability
                        }
                        try {
                            Files.move(tempFile.toPath(), file.toPath(), java.nio.file.StandardCopyOption.ATOMIC_MOVE, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                        } catch (Exception e) {
                            if (!file.exists()) {
                                Files.move(tempFile.toPath(), file.toPath(), java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                            } else {
                                tempFile.delete();
                            }
                        }
                    }

                    g.executeInTx(tx -> {
                        var traversal = tx.V().hasLabel(CLASS_NAME).has(PROP_KEY, key);
                        if (!traversal.hasNext()) {
                            tx.addV(CLASS_NAME)
                                .property(PROP_BUCKET, bucketName.asString())
                                .property(PROP_BLOB_ID, blobId.asString())
                                .property(PROP_KEY, key)
                                .property(PROP_STORAGE_TYPE, STORAGE_FILE_ZSTD)
                                .property(PROP_PAYLOAD, new byte[0]) // Zero payload in DB page
                                .iterate();
                        }
                    });
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
            if (file.exists()) {
                file.delete();
                pruneEmptyParentDirectories(file, new File(blobsDirectory, bucketName.asString()));
            }
            g.executeInTx(tx -> {
                var traversal = tx.V().hasLabel(CLASS_NAME).has(PROP_KEY, key);
                while (traversal.hasNext()) {
                    traversal.next().remove();
                }
            });
        }).subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic());
    }

    @Override
    public Publisher<Void> delete(BucketName bucketName, Collection<BlobId> blobIds) {
        return Mono.<Void>fromRunnable(() -> {
            File bucketDir = new File(blobsDirectory, bucketName.asString());
            for (BlobId blobId : blobIds) {
                File file = getFileForBlob(bucketName, blobId);
                if (file.exists()) {
                    file.delete();
                    pruneEmptyParentDirectories(file, bucketDir);
                }
            }
            g.executeInTx(tx -> {
                for (BlobId blobId : blobIds) {
                    String key = buildKey(bucketName, blobId);
                    var traversal = tx.V().hasLabel(CLASS_NAME).has(PROP_KEY, key);
                    while (traversal.hasNext()) {
                        traversal.next().remove();
                    }
                }
            });
        }).subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic());
    }

    @Override
    public Publisher<Void> deleteBucket(BucketName bucketName) {
        return Mono.<Void>fromRunnable(() -> {
            File bucketDir = new File(blobsDirectory, bucketName.asString());
            if (bucketDir.exists()) {
                try {
                    Files.walk(bucketDir.toPath())
                        .map(java.nio.file.Path::toFile)
                        .sorted((o1, o2) -> -o1.compareTo(o2))
                        .forEach(File::delete);
                } catch (Exception ignored) {
                }
            }
            g.executeInTx(tx -> {
                var traversal = tx.V().hasLabel(CLASS_NAME).has(PROP_BUCKET, bucketName.asString());
                while (traversal.hasNext()) {
                    traversal.next().remove();
                }
            });
        }).subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic());
    }

    @Override
    public Publisher<BucketName> listBuckets() {
        return Mono.fromCallable(() -> {
            return g.computeInTx(tx -> {
                Set<BucketName> buckets = new HashSet<>();
                var traversal = tx.V().hasLabel(CLASS_NAME).<String>values(PROP_BUCKET);
                while (traversal.hasNext()) {
                    buckets.add(BucketName.of(traversal.next()));
                }
                return buckets;
            });
        }).flatMapMany(Flux::fromIterable)
        .subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic());
    }

    @Override
    public Publisher<BlobId> listBlobs(BucketName bucketName) {
        return Mono.fromCallable(() -> {
            return g.computeInTx(tx -> {
                Set<BlobId> blobIds = new HashSet<>();
                var traversal = tx.V().hasLabel(CLASS_NAME)
                    .has(PROP_BUCKET, bucketName.asString())
                    .<String>values(PROP_BLOB_ID);
                while (traversal.hasNext()) {
                    blobIds.add(blobIdFactory.of(traversal.next()));
                }
                return blobIds;
            });
        }).flatMapMany(Flux::fromIterable)
        .subscribeOn(reactor.core.scheduler.Schedulers.boundedElastic());
    }
}
