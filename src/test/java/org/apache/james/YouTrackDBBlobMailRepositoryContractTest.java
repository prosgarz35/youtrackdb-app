package org.apache.james;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.jetbrains.youtrackdb.api.DatabaseType;
import com.jetbrains.youtrackdb.api.YouTrackDB;
import com.jetbrains.youtrackdb.api.YourTracks;
import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;

/**
 * James' own MailRepository contract, run against BlobMailRepository on top of the YouTrackDB blob store.
 * It is the same wiring as BlobMailRepositoryTest in James, with the in-memory DAO replaced.
 *
 * NOT RUN YET. It needs the patched YouTrackDBBlobStoreDAO (blob_repo_wiring/patched): the contract tests
 * "storingMessageWithSameKeyTwiceShouldUpdate..." are expected to fail with the DAO that ignores a second save
 * of the same id.
 */
class YouTrackDBBlobMailRepositoryContractTest implements MailRepositoryContract {

    @TempDir
    Path baseDir;

    private YouTrackDB youTrackDB;
    private YTDBGraphTraversalSource g;
    private YouTrackDBBlobMailRepositoryFactory factory;

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
                return baseDir.toFile();
            }
        };
        PlainBlobId.Factory blobIdFactory = new PlainBlobId.Factory();
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

    @Test
    void storingMessageWithSameKeyTwiceShouldNotLeakOldBodyBlobs() throws Exception {
        MailRepository testee = retrieveRepository();

        // First store: should create 3 blobs (metadata, header, body)
        MailKey key = testee.store(createMail(MAIL_1));
        long blobsAfterFirstStore = g.computeInTx(tx -> tx.V().hasLabel("JamesBlob").count().next());
        assertThat(blobsAfterFirstStore).isEqualTo(3L);

        // Store again under the same key with different content
        testee.store(createMail(MAIL_1, "Different Body Content Here"));
        long blobsAfterOverwrite = g.computeInTx(tx -> tx.V().hasLabel("JamesBlob").count().next());

        // Overwrite must replace metadata and clean up old header/body parts, leaving exactly 3 blobs
        assertThat(blobsAfterOverwrite).isEqualTo(blobsAfterFirstStore);
        assertThat(testee.size()).isEqualTo(1L);
    }

    @Test
    void likeWildcardEscapingShouldNotMatchOtherRepositories() throws Exception {
        MailRepository repoA_B = repositoryAt(MailRepositoryPath.from("var/mail/a_b"));
        MailRepository repoAxB = repositoryAt(MailRepositoryPath.from("var/mail/axb"));

        repoA_B.store(createMail(MAIL_1));
        repoAxB.store(createMail(MAIL_2));

        assertThat(repoA_B.size()).isEqualTo(1L);
        assertThat(repoAxB.size()).isEqualTo(1L);

        List<MailKey> keysA_B = new ArrayList<>();
        repoA_B.list().forEachRemaining(keysA_B::add);
        assertThat(keysA_B).hasSize(1);

        List<MailKey> keysAxB = new ArrayList<>();
        repoAxB.list().forEachRemaining(keysAxB::add);
        assertThat(keysAxB).hasSize(1);
    }

    @Test
    void metadataPrefixTrailingSlashIsolation() throws Exception {
        MailRepository repo1 = repositoryAt(MailRepositoryPath.from("var/mail/repo"));
        MailRepository repo2 = repositoryAt(MailRepositoryPath.from("var/mail/repo-extended"));

        repo1.store(createMail(MAIL_1));
        repo2.store(createMail(MAIL_2));

        assertThat(repo1.size()).isEqualTo(1L);
        assertThat(repo2.size()).isEqualTo(1L);
    }
}

