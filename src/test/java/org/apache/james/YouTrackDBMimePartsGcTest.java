package org.apache.james;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import jakarta.mail.Session;
import jakarta.mail.internet.MimeMessage;

import org.apache.james.blob.api.BucketName;
import org.apache.james.blob.api.BlobStoreDAO.BytesBlob;
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
import org.apache.james.youtrackdb.YouTrackDBMimePartsGc;
import org.apache.james.youtrackdb.YouTrackDBTransactions;
import org.apache.mailet.Mail;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.reactivestreams.Publisher;

import com.jetbrains.youtrackdb.api.DatabaseType;
import com.jetbrains.youtrackdb.api.YouTrackDB;
import com.jetbrains.youtrackdb.api.YourTracks;
import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;

import reactor.core.publisher.Mono;

/**
 * The garbage collection of MIME parts that no mail refers to, and the question it raises: can two mails of one
 * repository share a part (content addressed ids)? NOT RUN YET.
 */
public class YouTrackDBMimePartsGcTest {
    private static final Session SESSION = Session.getInstance(new Properties());
    private static final String ERROR = "var/mail/error";
    private static final String DENIED = "var/mail/relay-denied";

    private interface Body {
        void run(YTDBGraphTraversalSource g, YouTrackDBBlobStoreDAO dao, YouTrackDBBlobMailRepositoryFactory factory,
                 YouTrackDBMimePartsGc gc) throws Exception;
    }

    private static void run(Path workingDir, Body body) throws Exception {
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
                YouTrackDBBlobStoreDAO dao = new YouTrackDBBlobStoreDAO(g, blobIdFactory, fileSystem);
                body.run(g, dao, new YouTrackDBBlobMailRepositoryFactory(dao, blobIdFactory, BucketName.DEFAULT),
                    new YouTrackDBMimePartsGc(dao, BucketName.DEFAULT, java.time.Duration.ZERO));
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
        return MailImpl.builder().name(name).sender("sender@gc.local").addRecipient("recipient@gc.local").mimeMessage(message).build();
    }

    private static long blobCount(YTDBGraphTraversalSource g) {
        return g.computeInTx(tx -> tx.V().hasLabel("JamesBlob").count().next());
    }

    /** What a crash between the two steps of MailRepository.remove() leaves: the metadata is gone, the parts stay. */
    private static void deleteOnlyTheMetadata(YouTrackDBBlobStoreDAO dao, MailKey key) {
        Mono.from(dao.delete(BucketName.DEFAULT, new PlainBlobId(key.asString()))).block();
    }

    @Test
    void orphanedPartsShouldBeDeletedByTheSecondRunOnly(@TempDir Path workingDir) throws Exception {
        run(workingDir, (g, dao, factory, gc) -> {
            MailRepository error = repository(factory, ERROR);
            MailKey kept = error.store(mail("kept", "kept", "kept body"));
            MailKey crashed = error.store(mail("crashed", "crashed", "crashed body"));
            deleteOnlyTheMetadata(dao, crashed);
            assertThat(blobCount(g)).isEqualTo(5L);

            YouTrackDBMimePartsGc.Result first = gc.collect();
            assertThat(first.deletedParts()).isZero();
            assertThat(first.pendingParts()).isEqualTo(2L);
            assertThat(blobCount(g)).as("the first run only remembers").isEqualTo(5L);

            YouTrackDBMimePartsGc.Result second = gc.collect();
            assertThat(second.deletedParts()).isEqualTo(2L);
            assertThat(second.pendingParts()).isZero();
            assertThat(blobCount(g)).isEqualTo(3L);
            assertThat(error.retrieve(kept).getMessage().getContent()).isEqualTo("kept body");
            assertThat(error.size()).isEqualTo(1L);
        });
    }

    @Test
    void referencedPartsShouldNeverBeDeleted(@TempDir Path workingDir) throws Exception {
        run(workingDir, (g, dao, factory, gc) -> {
            MailRepository error = repository(factory, ERROR);
            MailRepository denied = repository(factory, DENIED);
            error.store(mail("a", "a", "body a"));
            error.store(mail("b", "b", "body b"));
            denied.store(mail("c", "c", "body c"));

            for (int run = 0; run < 3; run++) {
                YouTrackDBMimePartsGc.Result result = gc.collect();
                assertThat(result.deletedParts()).isZero();
                assertThat(result.pendingParts()).isZero();
            }
            assertThat(blobCount(g)).isEqualTo(9L);
            assertThat(error.size()).isEqualTo(2L);
            assertThat(denied.size()).isEqualTo(1L);
        });
    }

    /** The mail is being stored: its parts are saved, its metadata is not yet. The second run must find it complete. */
    @Test
    void partsOfAMailWhoseMetadataAppearsBetweenTwoRunsShouldSurvive(@TempDir Path workingDir) throws Exception {
        run(workingDir, (g, dao, factory, gc) -> {
            MailRepository error = repository(factory, ERROR);
            Mail mail = mail("in-flight", "in flight", "body in flight");
            MailKey key = error.store(mail);
            byte[] metadata = Mono.from(dao.readBytes(BucketName.DEFAULT, new PlainBlobId(key.asString()))).block().payload();
            deleteOnlyTheMetadata(dao, key);

            assertThat(gc.collect().pendingParts()).isEqualTo(2L);
            Mono.from((Publisher<Void>) dao.save(BucketName.DEFAULT, new PlainBlobId(key.asString()), BytesBlob.of(metadata))).block();

            YouTrackDBMimePartsGc.Result second = gc.collect();
            assertThat(second.deletedParts()).isZero();
            assertThat(second.pendingParts()).isZero();
            assertThat(error.size()).isEqualTo(1L);
            assertThat(error.retrieve(key).getMessage().getSubject()).isEqualTo("in flight");
        });
    }

    @Test
    void nothingShouldBeDeletedWhenAMetadataBlobCannotBeParsed(@TempDir Path workingDir) throws Exception {
        run(workingDir, (g, dao, factory, gc) -> {
            MailRepository error = repository(factory, ERROR);
            MailKey crashed = error.store(mail("crashed", "crashed", "crashed body"));
            deleteOnlyTheMetadata(dao, crashed);
            MailKey broken = error.store(mail("broken", "broken", "broken body"));
            Mono.from((Publisher<Void>) dao.save(BucketName.DEFAULT, new PlainBlobId(broken.asString()), BytesBlob.of("not json".getBytes()))).block();

            assertThatThrownBy(gc::collect).hasMessageContaining("nothing is deleted");
            assertThatThrownBy(gc::collect).hasMessageContaining("nothing is deleted");
            assertThat(blobCount(g)).isEqualTo(5L);
        });
    }

    /**
     * Not a GC test. If the ids of the parts depend on their content (a deduplicating store), two mails with the same
     * body share the body blob, and remove(), which deletes the parts of the mail it removes, breaks the other mail.
     */
    @Test
    void removingAMailShouldNotBreakAnotherMailWithTheSameBody(@TempDir Path workingDir) throws Exception {
        run(workingDir, (g, dao, factory, gc) -> {
            MailRepository error = repository(factory, ERROR);
            MailKey first = error.store(mail("first", "same subject", "exactly the same body"));
            MailKey second = error.store(mail("second", "same subject", "exactly the same body"));
            List<String> blobs = new ArrayList<>();
            g.computeInTx(tx -> tx.V().hasLabel("JamesBlob").values("blobId").toList()).forEach(id -> blobs.add(String.valueOf(id)));
            System.out.println("PARTS-SHARING blobs after two identical-body mails: " + blobs.size() + " " + blobs);

            error.remove(first);

            assertThat(error.size()).isEqualTo(1L);
            assertThat(error.retrieve(second).getMessage().getContent()).isEqualTo("exactly the same body");
        });
    }
}
