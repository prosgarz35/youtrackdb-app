package org.apache.james;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.james.blob.api.BlobId;
import org.apache.james.blob.api.BlobStoreDAO;
import org.apache.james.blob.api.BlobStoreDAO.BytesBlob;
import org.apache.james.blob.api.BucketName;
import org.apache.james.blob.api.PlainBlobId;
import org.apache.james.filesystem.api.FileSystem;
import org.apache.james.youtrackdb.YouTrackDBBlobStoreDAO;
import org.apache.james.youtrackdb.YouTrackDBTransactions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.jetbrains.youtrackdb.api.DatabaseType;
import com.jetbrains.youtrackdb.api.YouTrackDB;
import com.jetbrains.youtrackdb.api.YourTracks;
import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Blob ids that are not plain path segments (for example the ones a mail repository builds:
 * "var/mail/error/mailMetadata/{name}") and path safety of bucket names.
 *
 * NOT RUN YET. Written against the DAO in artifacts/blob_slash_fix/patched; it does not compile against
 * the DAO currently in the project (fileNamesToKeep does not exist there).
 */
public class YouTrackDBBlobPathTest {

    private static final BucketName BUCKET = BucketName.DEFAULT;
    private static final String SLASH_ID = "var/mail/error/mailMetadata/0c222abb-d115-4a88-9fbe-e65e951301f6";
    private static final String PLAIN_ID = "abcdef123456abcdef123456abcdef12";

    private interface WithDao {
        void run(YouTrackDBBlobStoreDAO dao, YTDBGraphTraversalSource g, Path baseDir) throws Exception;
    }

    private static void withDao(Path baseDir, WithDao body) throws Exception {
        File dbDir = baseDir.resolve("var").resolve("youtrackdb").toFile();
        dbDir.mkdirs();
        FileSystem fileSystem = new FileSystem() {
            @Override
            public InputStream getResource(String url) {
                throw new UnsupportedOperationException();
            }

            @Override
            public File getFile(String fileURL) throws FileNotFoundException {
                throw new FileNotFoundException(fileURL);
            }

            @Override
            public File getBasedir() {
                return baseDir.toFile();
            }
        };
        try (YouTrackDB ytdb = YourTracks.instance(dbDir.getAbsolutePath())) {
            ytdb.createIfNotExists("james", DatabaseType.DISK, "admin", "admin", "admin");
            try (YTDBGraphTraversalSource g = ytdb.openTraversal("james", "admin", "admin")) {
                YouTrackDBTransactions.executeStrictTx(g, tx -> {
                    tx.command("CREATE CLASS JamesBlob IF NOT EXISTS EXTENDS V");
                    tx.command("CREATE PROPERTY JamesBlob.bucketAndBlobId IF NOT EXISTS STRING");
                    tx.command("CREATE PROPERTY JamesBlob.bucket IF NOT EXISTS STRING");
                    tx.command("CREATE PROPERTY JamesBlob.blobId IF NOT EXISTS STRING");
                    tx.command("CREATE PROPERTY JamesBlob.storageType IF NOT EXISTS STRING");
                    tx.command("CREATE PROPERTY JamesBlob.payload IF NOT EXISTS BINARY");
                    tx.command("CREATE INDEX JamesBlob.bucketAndBlobId IF NOT EXISTS UNIQUE");
                });
                body.run(new YouTrackDBBlobStoreDAO(g, new PlainBlobId.Factory(), fileSystem), g, baseDir);
            }
        }
    }

    private static BlobId id(String value) {
        return new PlainBlobId.Factory().of(value);
    }

    private static byte[] randomBytes(int size) {
        byte[] bytes = new byte[size];
        new Random(42).nextBytes(bytes);
        return bytes;
    }

    private static byte[] compressibleBytes(int size) {
        byte[] bytes = new byte[size];
        for (int i = 0; i < size; i++) {
            bytes[i] = (byte) ('a' + (i % 7));
        }
        return bytes;
    }

    private static void save(YouTrackDBBlobStoreDAO dao, BucketName bucket, String blobId, byte[] payload) {
        Mono.from(dao.save(bucket, id(blobId), BytesBlob.of(payload))).block();
    }

    private static byte[] readBytes(YouTrackDBBlobStoreDAO dao, BucketName bucket, String blobId) {
        return Mono.from(dao.readBytes(bucket, id(blobId))).block().payload();
    }

    private static long recordCount(YTDBGraphTraversalSource g) {
        return g.computeInTx(tx -> tx.V().hasLabel("JamesBlob").count().next());
    }

    private static List<Path> regularFiles(Path dir) throws Exception {
        if (!Files.exists(dir)) {
            return List.of();
        }
        try (Stream<Path> stream = Files.walk(dir)) {
            return stream.filter(Files::isRegularFile).collect(Collectors.toList());
        }
    }

    @Test
    @DisplayName("A slash id round-trips in the three tiers: raw (3 KB), Zstd in the database (20 KB), file (200 KB)")
    void slashIdRoundTripsInAllTiers(@TempDir Path baseDir) throws Exception {
        withDao(baseDir, (dao, g, base) -> {
            byte[] small = randomBytes(3 * 1024);
            byte[] medium = compressibleBytes(20 * 1024);
            byte[] large = randomBytes(200 * 1024);

            save(dao, BUCKET, SLASH_ID + "/small", small);
            save(dao, BUCKET, SLASH_ID + "/medium", medium);
            save(dao, BUCKET, SLASH_ID + "/large", large);

            assertThat(readBytes(dao, BUCKET, SLASH_ID + "/small")).isEqualTo(small);
            assertThat(readBytes(dao, BUCKET, SLASH_ID + "/medium")).isEqualTo(medium);
            assertThat(readBytes(dao, BUCKET, SLASH_ID + "/large")).isEqualTo(large);
            try (InputStream in = Mono.from(dao.readReactive(BUCKET, id(SLASH_ID + "/large"))).block().payload()) {
                assertThat(in.readAllBytes()).isEqualTo(large);
            }
            assertThat(recordCount(g)).isEqualTo(3L);
        });
    }

    @Test
    @DisplayName("A large slash id is stored under var/blobs/.hashed, never under a directory built from the id")
    void largeSlashIdIsStoredUnderTheHashedRoot(@TempDir Path baseDir) throws Exception {
        withDao(baseDir, (dao, g, base) -> {
            save(dao, BUCKET, SLASH_ID, randomBytes(200 * 1024));

            Path blobs = base.resolve("var").resolve("blobs");
            List<Path> files = regularFiles(blobs);
            assertThat(files).hasSize(1);
            assertThat(files.getFirst()).startsWith(blobs.resolve(".hashed"));
            assertThat(files.getFirst().getFileName().toString())
                .isEqualTo(YouTrackDBBlobStoreDAO.fileNamesToKeep(Set.of(SLASH_ID)).stream()
                    .filter(name -> !name.equals(SLASH_ID)).findFirst().orElseThrow());
            assertThat(blobs.resolve("default").resolve("var")).doesNotExist();
            assertThat(files).noneMatch(path -> path.getFileName().toString().contains(".tmp."));
        });
    }

    @Test
    @DisplayName("Deleting a slash id removes the record and the file")
    void deleteRemovesRecordAndFile(@TempDir Path baseDir) throws Exception {
        withDao(baseDir, (dao, g, base) -> {
            save(dao, BUCKET, SLASH_ID, randomBytes(200 * 1024));
            assertThat(regularFiles(base.resolve("var").resolve("blobs"))).hasSize(1);

            Mono.from(dao.delete(BUCKET, id(SLASH_ID))).block();

            assertThat(recordCount(g)).isZero();
            assertThat(regularFiles(base.resolve("var").resolve("blobs"))).isEmpty();
        });
    }

    @Test
    @DisplayName("Deleting a collection of ids (plain and slash) removes every record and file")
    void deleteCollectionRemovesEverything(@TempDir Path baseDir) throws Exception {
        withDao(baseDir, (dao, g, base) -> {
            save(dao, BUCKET, SLASH_ID + "/1", randomBytes(200 * 1024));
            save(dao, BUCKET, PLAIN_ID, randomBytes(200 * 1024));
            save(dao, BUCKET, SLASH_ID + "/2", compressibleBytes(1024));
            assertThat(recordCount(g)).isEqualTo(3L);

            Mono.from(dao.delete(BUCKET, List.of(id(SLASH_ID + "/1"), id(PLAIN_ID), id(SLASH_ID + "/2")))).block();

            assertThat(recordCount(g)).isZero();
            assertThat(regularFiles(base.resolve("var").resolve("blobs"))).isEmpty();
        });
    }

    @Test
    @DisplayName("A plain id keeps the sharded layout var/blobs/{bucket}/ab/cd/ef/{id}")
    void plainIdKeepsTheShardedLayout(@TempDir Path baseDir) throws Exception {
        withDao(baseDir, (dao, g, base) -> {
            save(dao, BUCKET, PLAIN_ID, randomBytes(200 * 1024));

            Path expected = base.resolve("var").resolve("blobs").resolve("default")
                .resolve("ab").resolve("cd").resolve("ef").resolve(PLAIN_ID);
            assertThat(expected).isRegularFile();
        });
    }

    @Test
    @DisplayName("listBlobs returns the slash ids as they were saved")
    void listBlobsReturnsSlashIds(@TempDir Path baseDir) throws Exception {
        withDao(baseDir, (dao, g, base) -> {
            save(dao, BUCKET, SLASH_ID, compressibleBytes(1024));

            List<String> ids = Flux.from(dao.listBlobs(BUCKET)).map(BlobId::asString).collectList().block();

            assertThat(ids).containsExactly(SLASH_ID);
        });
    }

    @Test
    @DisplayName("deleteBucket with a traversal name does not delete anything outside var/blobs")
    void deleteBucketWithTraversalNameStaysInsideBlobs(@TempDir Path baseDir) throws Exception {
        withDao(baseDir, (dao, g, base) -> {
            Path sentinel = base.resolve("var").resolve("keep.txt");
            Files.writeString(sentinel, "must survive");
            save(dao, BUCKET, PLAIN_ID, randomBytes(200 * 1024));

            Mono.from(dao.deleteBucket(BucketName.of(".."))).block();

            assertThat(sentinel).exists();
            assertThat(base.resolve("var").resolve("youtrackdb")).exists();
            assertThat(readBytes(dao, BUCKET, PLAIN_ID)).hasSize(200 * 1024);
        });
    }

    @Test
    @DisplayName("deleteBucket removes the plain files and the hashed files of the bucket")
    void deleteBucketRemovesBothLayouts(@TempDir Path baseDir) throws Exception {
        withDao(baseDir, (dao, g, base) -> {
            BucketName other = BucketName.of("other");
            save(dao, other, PLAIN_ID, randomBytes(200 * 1024));
            save(dao, other, SLASH_ID, randomBytes(200 * 1024));
            save(dao, BUCKET, PLAIN_ID, randomBytes(200 * 1024));

            Mono.from(dao.deleteBucket(other)).block();

            assertThat(recordCount(g)).isEqualTo(1L);
            assertThat(regularFiles(base.resolve("var").resolve("blobs"))).hasSize(1);
        });
    }

    @Test
    @DisplayName("fileNamesToKeep keeps both the plain id and the hashed name")
    void fileNamesToKeepContainsBothNames(@TempDir Path baseDir) {
        Set<String> names = YouTrackDBBlobStoreDAO.fileNamesToKeep(Set.of(PLAIN_ID, SLASH_ID));

        assertThat(names).contains(PLAIN_ID, SLASH_ID);
        assertThat(names).hasSize(4);
    }
}
