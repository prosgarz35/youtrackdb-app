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
    private static final String PROP_STORAGE_TYPE = "storageType"; // "INLINE" or "FILE"
    private static final String STORAGE_INLINE = "INLINE";
    private static final String STORAGE_FILE = "FILE";

    private static final int INLINE_THRESHOLD_BYTES = 16 * 1024; // 16 KB threshold

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

    private File getFileForBlob(BucketName bucketName, BlobId blobId) {
        File bucketDir = new File(blobsDirectory, bucketName.asString());
        return new File(bucketDir, blobId.asString());
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
                String storageType = v.property(PROP_STORAGE_TYPE).isPresent() ? v.value(PROP_STORAGE_TYPE) : STORAGE_INLINE;

                if (STORAGE_FILE.equals(storageType)) {
                    File file = getFileForBlob(bucketName, blobId);
                    if (!file.exists()) {
                        throw new RuntimeException(new ObjectNotFoundException("Blob file missing on disk: " + file.getAbsolutePath()));
                    }
                    try {
                        return InputStreamBlob.of(new FileInputStream(file));
                    } catch (IOException e) {
                        throw new RuntimeException(new ObjectStoreIOException("Error opening blob file: " + file.getAbsolutePath(), e));
                    }
                } else {
                    byte[] bytes = v.value(PROP_PAYLOAD);
                    if (bytes == null) {
                        bytes = new byte[0];
                    }
                    return InputStreamBlob.of(new ByteArrayInputStream(bytes));
                }
            });
        }).switchIfEmpty(Mono.error(() -> new ObjectNotFoundException("Blob not found: " + blobId.asString() + " in bucket: " + bucketName.asString())));
    }

    @Override
    public Publisher<BytesBlob> readBytes(BucketName bucketName, BlobId blobId) {
        return Mono.from(readReactive(bucketName, blobId))
            .flatMap(inputStreamBlob -> Mono.fromCallable(inputStreamBlob::asBytes));
    }

    @Override
    public Publisher<Void> save(BucketName bucketName, BlobId blobId, Blob blob) {
        Preconditions.checkNotNull(blob);
        return Mono.fromRunnable(() -> {
            try {
                byte[] data = blob.asInputStream().payload().readAllBytes();
                String key = buildKey(bucketName, blobId);

                if (data.length > INLINE_THRESHOLD_BYTES) {
                    // Stream to external file storage
                    File file = getFileForBlob(bucketName, blobId);
                    File parent = file.getParentFile();
                    if (!parent.exists()) {
                        parent.mkdirs();
                    }
                    try (FileOutputStream fos = new FileOutputStream(file)) {
                        fos.write(data);
                    }

                    g.executeInTx(tx -> {
                        var traversal = tx.V().hasLabel(CLASS_NAME).has(PROP_KEY, key);
                        Vertex v;
                        if (traversal.hasNext()) {
                            v = traversal.next();
                        } else {
                            v = tx.addV(CLASS_NAME)
                                .property(PROP_BUCKET, bucketName.asString())
                                .property(PROP_BLOB_ID, blobId.asString())
                                .property(PROP_KEY, key)
                                .next();
                        }
                        v.property(PROP_STORAGE_TYPE, STORAGE_FILE);
                        v.property(PROP_PAYLOAD, new byte[0]); // Clear inline binary
                    });
                } else {
                    // Inline directly in YouTrackDB record
                    g.executeInTx(tx -> {
                        var traversal = tx.V().hasLabel(CLASS_NAME).has(PROP_KEY, key);
                        Vertex v;
                        if (traversal.hasNext()) {
                            v = traversal.next();
                        } else {
                            v = tx.addV(CLASS_NAME)
                                .property(PROP_BUCKET, bucketName.asString())
                                .property(PROP_BLOB_ID, blobId.asString())
                                .property(PROP_KEY, key)
                                .next();
                        }
                        v.property(PROP_STORAGE_TYPE, STORAGE_INLINE);
                        v.property(PROP_PAYLOAD, data);
                    });
                }
            } catch (Exception e) {
                throw new ObjectStoreIOException("Error saving blob " + blobId.asString(), e);
            }
        });
    }

    @Override
    public Publisher<Void> delete(BucketName bucketName, BlobId blobId) {
        return Mono.fromRunnable(() -> {
            String key = buildKey(bucketName, blobId);
            File file = getFileForBlob(bucketName, blobId);
            if (file.exists()) {
                file.delete();
            }
            g.executeInTx(tx -> {
                var traversal = tx.V().hasLabel(CLASS_NAME).has(PROP_KEY, key);
                while (traversal.hasNext()) {
                    traversal.next().remove();
                }
            });
        });
    }

    @Override
    public Publisher<Void> delete(BucketName bucketName, Collection<BlobId> blobIds) {
        return Mono.fromRunnable(() -> {
            for (BlobId blobId : blobIds) {
                File file = getFileForBlob(bucketName, blobId);
                if (file.exists()) {
                    file.delete();
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
        });
    }

    @Override
    public Publisher<Void> deleteBucket(BucketName bucketName) {
        return Mono.fromRunnable(() -> {
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
        });
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
        }).flatMapMany(Flux::fromIterable);
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
        }).flatMapMany(Flux::fromIterable);
    }
}
