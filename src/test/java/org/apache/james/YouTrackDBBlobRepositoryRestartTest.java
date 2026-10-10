package org.apache.james;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import jakarta.mail.Session;
import jakarta.mail.internet.MimeMessage;

import org.apache.james.blob.api.BucketName;
import org.apache.james.blob.api.PlainBlobId;
import org.apache.james.filesystem.api.FileSystem;
import org.apache.james.mailrepository.api.MailKey;
import org.apache.james.mailrepository.api.MailRepository;
import org.apache.james.mailrepository.api.MailRepositoryPath;
import org.apache.james.mailrepository.api.MailRepositoryUrl;
import org.apache.james.mailrepository.api.Protocol;
import org.apache.james.server.core.MailImpl;
import org.apache.james.youtrackdb.YouTrackDBBlobMailRepositoryFactory;
import org.apache.james.youtrackdb.YouTrackDBBlobStoreDAO;
import org.apache.james.youtrackdb.YouTrackDBTransactions;
import org.apache.mailet.Mail;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.jetbrains.youtrackdb.api.DatabaseType;
import com.jetbrains.youtrackdb.api.YouTrackDB;
import com.jetbrains.youtrackdb.api.YourTracks;
import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;

/**
 * What the blob mail repositories must guarantee after the database is closed and opened again (RFC 5321 6.1: a
 * mail accepted and parked in an error repository is not lost by a restart), and how size()/list() scale.
 *
 * The restart is the one the other tests of this project use: the YouTrackDB instance is closed and a new one
 * is opened on the same directory. The JVM is not killed, so this does not prove anything about a crash.
 * NOT RUN YET.
 */
public class YouTrackDBBlobRepositoryRestartTest {
    private static final Session SESSION = Session.getInstance(new Properties());
    private static final String ERROR = "var/mail/error";
    private static final String DENIED = "var/mail/relay-denied";

    private interface WithRepositories {
        void run(YTDBGraphTraversalSource g, YouTrackDBBlobMailRepositoryFactory factory) throws Exception;
    }

    private static void withRepositories(Path workingDir, WithRepositories body) throws Exception {
        File dbDir = workingDir.resolve("var").resolve("youtrackdb").toFile();
        dbDir.mkdirs();
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
                    tx.command("CREATE INDEX JamesBlob.bucketAndBlobIdRange IF NOT EXISTS ON JamesBlob (bucket, blobId) NOTUNIQUE");
                });
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
                        return workingDir.toFile();
                    }
                };
                PlainBlobId.Factory blobIdFactory = new PlainBlobId.Factory();
                body.run(g, new YouTrackDBBlobMailRepositoryFactory(
                    new YouTrackDBBlobStoreDAO(g, blobIdFactory, fileSystem), blobIdFactory, BucketName.DEFAULT));
            }
        }
    }

    private static MailRepository repository(YouTrackDBBlobMailRepositoryFactory factory, String path) {
        return factory.create(MailRepositoryUrl.fromPathAndProtocol(new Protocol("blob"), MailRepositoryPath.from(path)));
    }

    private static Mail mail(String name, String subject, String body) throws Exception {
        MimeMessage message = new MimeMessage(SESSION);
        message.setSubject(subject);
        message.setText(body);
        message.saveChanges();
        return MailImpl.builder()
            .name(name)
            .sender("sender@restart.local")
            .addRecipient("recipient@restart.local")
            .mimeMessage(message)
            .build();
    }

    private static byte[] bytesOf(Mail mail) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        mail.getMessage().writeTo(out);
        return out.toByteArray();
    }

    private static List<MailKey> keysOf(MailRepository repository) throws Exception {
        List<MailKey> keys = new ArrayList<>();
        repository.list().forEachRemaining(keys::add);
        return keys;
    }

    private static long blobCount(YTDBGraphTraversalSource g) {
        return g.computeInTx(tx -> tx.V().hasLabel("JamesBlob").count().next());
    }

    @Test
    @DisplayName("Mails of every size tier, an overwritten mail and two repositories survive a restart; removeAll stays removed")
    void mailsSurviveARestart(@TempDir Path workingDir) throws Exception {
        Map<String, String> bodies = Map.of(
            "tiny", "a few bytes",
            "medium", "medium body line\r\n".repeat(1_000),
            "huge", "a line of a big mail, with some entropy 0123456789 abcdefghij\r\n".repeat(3_000)
        );

        Map<MailKey, byte[]> expectedError = new HashMap<>();
        Map<MailKey, byte[]> expectedDenied = new HashMap<>();

        withRepositories(workingDir, (g, factory) -> {
            MailRepository error = repository(factory, ERROR);
            MailRepository denied = repository(factory, DENIED);
            for (Map.Entry<String, String> body : bodies.entrySet()) {
                Mail mail = mail("error-" + body.getKey(), "subject " + body.getKey(), body.getValue());
                expectedError.put(error.store(mail), bytesOf(mail));
            }
            Mail first = mail("overwritten", "first version", "first");
            error.store(first);
            Mail second = mail("overwritten", "second version", "second, longer");
            expectedError.put(error.store(second), bytesOf(second));

            Mail other = mail("denied-1", "denied", "relay denied body");
            expectedDenied.put(denied.store(other), bytesOf(other));

            assertThat(error.size()).isEqualTo(4L);
            // metadata + header + body for each of the 5 mails, the overwrite left nothing behind
            assertThat(blobCount(g)).isEqualTo(5L * 3L);
        });

        withRepositories(workingDir, (g, factory) -> {
            MailRepository error = repository(factory, ERROR);
            MailRepository denied = repository(factory, DENIED);

            assertThat(error.size()).isEqualTo(4L);
            assertThat(keysOf(error)).containsExactlyInAnyOrderElementsOf(expectedError.keySet());
            for (Map.Entry<MailKey, byte[]> expected : expectedError.entrySet()) {
                assertThat(bytesOf(error.retrieve(expected.getKey())))
                    .as("mail %s after restart", expected.getKey().asString())
                    .isEqualTo(expected.getValue());
            }
            assertThat(denied.size()).isEqualTo(1L);
            assertThat(keysOf(denied)).containsExactlyInAnyOrderElementsOf(expectedDenied.keySet());
            assertThat(blobCount(g)).isEqualTo(5L * 3L);

            // An overwrite made after the restart still replaces the previous version without a leak.
            Mail third = mail("overwritten", "third version", "third");
            MailKey key = error.store(third);
            assertThat(bytesOf(error.retrieve(key))).isEqualTo(bytesOf(third));
            assertThat(error.size()).isEqualTo(4L);
            assertThat(blobCount(g)).isEqualTo(5L * 3L);

            error.removeAll();
            assertThat(error.size()).isZero();
            assertThat(denied.size()).isEqualTo(1L);
            assertThat(blobCount(g)).isEqualTo(3L);
        });

        withRepositories(workingDir, (g, factory) -> {
            assertThat(repository(factory, ERROR).size()).isZero();
            assertThat(keysOf(repository(factory, DENIED))).containsExactlyInAnyOrderElementsOf(expectedDenied.keySet());
            assertThat(blobCount(g)).isEqualTo(3L);
        });
    }

    /**
     * Not an assertion on speed (timings of a shared machine are not stable): it prints how size() and list() of a
     * small repository behave next to a big one. With the (bucket, blobId) range index the small repository must
     * stay fast whatever the size of its neighbour. Size: -Dblobrepo.bench.mails=N (default 500 mails).
     */
    @Test
    @DisplayName("size()/list() of a small repository next to a big one (prints timings)")
    void sizeAndListScaling(@TempDir Path workingDir) throws Exception {
        int big = Integer.getInteger("blobrepo.bench.mails", 500);
        int small = 10;

        withRepositories(workingDir, (g, factory) -> {
            MailRepository bigRepository = repository(factory, ERROR);
            MailRepository smallRepository = repository(factory, DENIED);
            for (int i = 0; i < big; i++) {
                bigRepository.store(mail("big-" + i, "subject " + i, "body " + i));
            }
            for (int i = 0; i < small; i++) {
                smallRepository.store(mail("small-" + i, "subject " + i, "body " + i));
            }

            // warm-up, so that the first query does not pay for class loading
            smallRepository.size();
            keysOf(smallRepository);

            long t0 = System.nanoTime();
            long smallSize = smallRepository.size();
            long t1 = System.nanoTime();
            int smallListed = keysOf(smallRepository).size();
            long t2 = System.nanoTime();
            long bigSize = bigRepository.size();
            long t3 = System.nanoTime();
            int bigListed = keysOf(bigRepository).size();
            long t4 = System.nanoTime();

            System.out.printf("BLOB-REPO-BENCH blobs=%d  small(%d mails): size=%d ms, list=%d ms | big(%d mails): size=%d ms, list=%d ms%n",
                blobCount(g), small, (t1 - t0) / 1_000_000, (t2 - t1) / 1_000_000,
                big, (t3 - t2) / 1_000_000, (t4 - t3) / 1_000_000);

            assertThat(smallSize).isEqualTo(small);
            assertThat(smallListed).isEqualTo(small);
            assertThat(bigSize).isEqualTo(big);
            assertThat(bigListed).isEqualTo(big);
        });
    }
}
