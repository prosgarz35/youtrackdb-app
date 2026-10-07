package org.apache.james;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.apache.james.blob.api.BlobId;
import org.apache.james.blob.api.BucketName;
import org.apache.james.blob.api.PlainBlobId;
import org.apache.james.filesystem.api.FileSystem;
import org.apache.james.mailrepository.MailRepositoryContract;
import org.apache.james.mailrepository.api.MailKey;
import org.apache.james.mailrepository.api.MailRepository;
import org.apache.james.mailrepository.api.MailRepositoryPath;
import org.apache.james.mailrepository.api.MailRepositoryUrl;
import org.apache.james.mailrepository.api.Protocol;
import org.apache.james.youtrackdb.YouTrackDBBlobMailRepositoryFactory;
import org.apache.james.youtrackdb.YouTrackDBBlobStoreDAO;
import org.apache.james.youtrackdb.YouTrackDBTransactions;
import org.apache.mailet.Mail;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import org.reactivestreams.Publisher;

import com.jetbrains.youtrackdb.api.DatabaseType;
import com.jetbrains.youtrackdb.api.YouTrackDB;
import com.jetbrains.youtrackdb.api.YourTracks;
import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;

import reactor.core.publisher.Mono;

/**
 * James' own MailRepository contract, run against BlobMailRepository on top of the YouTrackDB blob store.
 * It is the same wiring as BlobMailRepositoryTest in James, with the in-memory DAO replaced.
 *
 * Besides the contract: isolation of repositories with similar or wildcard-looking paths, the index used by the
 * prefix queries, and the overwrite of a mail whose old parts cannot be deleted.
 */
class YouTrackDBBlobMailRepositoryContractTest implements MailRepositoryContract {

    @TempDir
    Path baseDir;

    private YouTrackDB youTrackDB;
    private YTDBGraphTraversalSource g;
    private YouTrackDBBlobMailRepositoryFactory factory;
    private FileSystem fileSystem;
    private PlainBlobId.Factory blobIdFactory;

    @BeforeEach
    void setUp() throws Exception {
        File dbDir = baseDir.resolve("var").resolve("youtrackdb").toFile();
        dbDir.mkdirs();
        youTrackDB = YourTracks.instance(dbDir.getAbsolutePath());
        youTrackDB.createIfNotExists("james", DatabaseType.DISK, "admin", "admin", "admin");
        g = youTrackDB.openTraversal("james", "admin", "admin");
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
        fileSystem = new FileSystem() {
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
        blobIdFactory = new PlainBlobId.Factory();
        factory = new YouTrackDBBlobMailRepositoryFactory(
            new YouTrackDBBlobStoreDAO(g, blobIdFactory, fileSystem), blobIdFactory, BucketName.DEFAULT);
    }

    @AfterEach
    void tearDown() {
        g.close();
        youTrackDB.close();
    }

    private MailRepository repositoryAt(MailRepositoryPath path) {
        return factory.create(MailRepositoryUrl.fromPathAndProtocol(new Protocol("blob"), path));
    }

    @Override
    public MailRepository retrieveRepository() {
        return repositoryAt(MailRepositoryPath.from("var/mail/error"));
    }

    @Override
    public MailRepository retrieveRepository(MailRepositoryPath path) {
        return repositoryAt(path);
    }

    @Test
    @Disabled("The contract compares the reported keys with the mail names; the keys of this repository are "
        + "blob paths. removeAllShouldReportTheKeysReturnedByStore checks the same thing with the real keys.")
    @Override
    public void removeAllShouldReportTheRemovedMails() {
    }

    @Test
    void removeAllShouldReportTheKeysReturnedByStore() throws Exception {
        MailRepository testee = retrieveRepository();
        MailKey key1 = testee.store(createMail(MAIL_1));
        MailKey key2 = testee.store(createMail(MAIL_2));

        List<MailKey> removed = new ArrayList<>();
        testee.removeAll(removed::add);

        assertThat(removed).containsExactlyInAnyOrder(key1, key2);
    }

    @Test
    void removeAllShouldNotRemoveTheMailsOfAnotherRepository() throws Exception {
        MailRepository error = repositoryAt(MailRepositoryPath.from("var/mail/error"));
        MailRepository denied = repositoryAt(MailRepositoryPath.from("var/mail/relay-denied"));
        error.store(createMail(MAIL_1));
        MailKey deniedKey = denied.store(createMail(MAIL_2));

        error.removeAll();

        assertThat(error.size()).isZero();
        assertThat(denied.size()).isEqualTo(1L);
        assertThat(denied.retrieve(deniedKey)).isNotNull();
    }

    private void assertIsolated(String pathA, String pathB) throws Exception {
        MailRepository a = repositoryAt(MailRepositoryPath.from(pathA));
        MailRepository b = repositoryAt(MailRepositoryPath.from(pathB));
        MailKey keyA = a.store(createMail(MAIL_1));
        MailKey keyB = b.store(createMail(MAIL_2));

        assertThat(a.size()).as(pathA).isEqualTo(1L);
        assertThat(b.size()).as(pathB).isEqualTo(1L);
        List<MailKey> listedA = new ArrayList<>();
        a.list().forEachRemaining(listedA::add);
        List<MailKey> listedB = new ArrayList<>();
        b.list().forEachRemaining(listedB::add);
        assertThat(listedA).containsExactly(keyA);
        assertThat(listedB).containsExactly(keyB);

        a.removeAll();
        assertThat(a.size()).isZero();
        assertThat(b.size()).isEqualTo(1L);
    }

    @Test
    void repositoriesWithSimilarPathsShouldBeIsolated() throws Exception {
        assertIsolated("var/mail/repo", "var/mail/repo-extended");
        assertIsolated("var/mail/a_b", "var/mail/axb");
        assertIsolated("var/mail/Case", "var/mail/case");
    }

    /** '%' and '?' are wildcards of LIKE: the prefix query must not interpret them. */
    @Test
    void repositoriesWithWildcardCharactersInTheirPathShouldBeIsolated() throws Exception {
        assertIsolated("var/mail/a%b", "var/mail/axxb");
        assertIsolated("var/mail/c?d", "var/mail/cxd");
    }

    /** The point of the (bucket, blobId) index. The plan format is the engine's: the test prints it on failure. */
    @Test
    void prefixQueryShouldUseTheRangeIndex() {
        List<?> results = g.computeInTx(tx -> tx.yql(
            "EXPLAIN SELECT blobId FROM JamesBlob WHERE bucket = :bucket AND blobId >= :lo AND blobId < :hi",
            "bucket", "default", "lo", "var/mail/error/", "hi", "var/mail/error0").toList());
        String plan = String.valueOf(results);

        assertThat(plan).as("plan: " + plan).contains("JamesBlob.bucketAndBlobIdRange");
    }

    @Test
    void overwriteShouldSucceedWhenDeletingTheOldPartsFails() throws Exception {
        YouTrackDBBlobStoreDAO failingDelete = new YouTrackDBBlobStoreDAO(g, blobIdFactory, fileSystem) {
            @Override
            public Publisher<Void> delete(BucketName bucketName, BlobId blobId) {
                return Mono.error(new IllegalStateException("disk failure"));
            }
        };
        MailRepository testee = new YouTrackDBBlobMailRepositoryFactory(failingDelete, blobIdFactory, BucketName.DEFAULT)
            .create(MailRepositoryUrl.fromPathAndProtocol(new Protocol("blob"), MailRepositoryPath.from("var/mail/error")));
        MailKey key = testee.store(createMail(MAIL_1));

        Mail updated = createMail(MAIL_1, "modified content");
        testee.store(updated);

        assertThat(testee.size()).isEqualTo(1L);
        assertThat(testee.retrieve(key)).satisfies(actual -> checkMailEquality(actual, updated));
    }

    @Test
    void storingMessageWithSameKeyTwiceShouldNotLeakOldBodyBlobs() throws Exception {
        MailRepository testee = retrieveRepository();

        testee.store(createMail(MAIL_1));
        long blobsAfterFirstStore = g.computeInTx(tx -> tx.V().hasLabel("JamesBlob").count().next());
        assertThat(blobsAfterFirstStore).isEqualTo(3L);

        testee.store(createMail(MAIL_1, "Different Body Content Here"));
        long blobsAfterOverwrite = g.computeInTx(tx -> tx.V().hasLabel("JamesBlob").count().next());

        assertThat(blobsAfterOverwrite).isEqualTo(blobsAfterFirstStore);
        assertThat(testee.size()).isEqualTo(1L);
    }
}
