package org.apache.james;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.nio.file.Path;
import java.time.Duration;

import org.apache.james.core.builder.MimeMessageBuilder;
import org.apache.james.queue.api.MailQueue;
import org.apache.james.queue.api.MailQueueItemDecoratorFactory;
import org.apache.james.queue.api.MailQueueName;
import org.apache.james.queue.api.ManageableMailQueue;
import org.apache.james.server.core.MailImpl;
import org.apache.james.youtrackdb.YouTrackDBMailQueueFactory;
import org.apache.james.youtrackdb.YouTrackDBTransactions;
import org.apache.mailet.Mail;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.jetbrains.youtrackdb.api.DatabaseType;
import com.jetbrains.youtrackdb.api.YouTrackDB;
import com.jetbrains.youtrackdb.api.YourTracks;
import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;

import reactor.core.publisher.Mono;

/**
 * Regression tests for DELETE VERTEX commands used by the application.
 *
 * NOT RUN YET. Written against the engine API read in J:\youtrackdb (develop):
 * YTDBGraphTraversalSource.command(String, Object...) takes alternating key/value pairs
 * and throws IllegalArgumentException when the number of arguments is odd.
 */
public class YouTrackDBDeleteCommandsTest {

    private interface WithGraph {
        void run(YTDBGraphTraversalSource g) throws Exception;
    }

    private static void withGraph(Path workingDir, WithGraph body) throws Exception {
        File dbDir = workingDir.resolve("var").resolve("youtrackdb").toFile();
        dbDir.mkdirs();
        try (YouTrackDB ytdb = YourTracks.instance(dbDir.getAbsolutePath())) {
            ytdb.createIfNotExists("james", DatabaseType.DISK, "admin", "admin", "admin");
            try (YTDBGraphTraversalSource g = ytdb.openTraversal("james", "admin", "admin")) {
                YouTrackDBTransactions.executeStrictTx(g, tx -> {
                    tx.command("CREATE CLASS JamesBlob IF NOT EXISTS EXTENDS V");
                    tx.command("CREATE PROPERTY JamesBlob.bucketAndBlobId IF NOT EXISTS STRING");
                    tx.command("CREATE PROPERTY JamesBlob.bucket IF NOT EXISTS STRING");
                    tx.command("CREATE CLASS JamesDomain IF NOT EXISTS EXTENDS V");
                    tx.command("CREATE PROPERTY JamesDomain.domain IF NOT EXISTS STRING");
                    tx.command("CREATE INDEX JamesDomain.domain IF NOT EXISTS UNIQUE");
                    tx.command("CREATE CLASS JamesRRTMapping IF NOT EXISTS EXTENDS V");
                    tx.command("CREATE PROPERTY JamesRRTMapping.source IF NOT EXISTS STRING");
                    tx.command("CREATE PROPERTY JamesRRTMapping.mapping IF NOT EXISTS STRING");
                    tx.command("CREATE CLASS JamesQueueItem IF NOT EXISTS EXTENDS V");
                    tx.command("CREATE PROPERTY JamesQueueItem.enqueueId IF NOT EXISTS STRING");
                    tx.command("CREATE PROPERTY JamesQueueItem.queueName IF NOT EXISTS STRING");
                    tx.command("CREATE PROPERTY JamesQueueItem.mailName IF NOT EXISTS STRING");
                    tx.command("CREATE PROPERTY JamesQueueItem.nextDelivery IF NOT EXISTS LONG");
                    tx.command("CREATE PROPERTY JamesQueueItem.serializedMail IF NOT EXISTS BINARY");
                    tx.command("CREATE INDEX JamesQueueItem.enqueueId IF NOT EXISTS UNIQUE");
                    tx.command("CREATE INDEX JamesQueueItem.queueAndMail IF NOT EXISTS ON JamesQueueItem (queueName, mailName) NOTUNIQUE");
                });
                body.run(g);
            }
        }
    }

    private static long count(YTDBGraphTraversalSource g, String label) {
        return g.computeInTx(tx -> tx.V().hasLabel(label).count().next());
    }

    private static void addBlob(YTDBGraphTraversalSource g, String bucket, String key) {
        YouTrackDBTransactions.executeStrictTx(g, tx -> tx.addV("JamesBlob")
            .property("bucket", bucket)
            .property("bucketAndBlobId", key)
            .iterate());
    }

    @Test
    @DisplayName("Characterization: command() with one positional '?' argument is rejected by the engine")
    void positionalSingleArgumentIsRejected(@TempDir Path workingDir) throws Exception {
        withGraph(workingDir, g -> {
            addBlob(g, "b", "b/x");

            assertThatThrownBy(() -> YouTrackDBTransactions.executeStrictTx(g, tx ->
                tx.command("DELETE VERTEX JamesBlob WHERE bucketAndBlobId = ?", "b/x")))
                .isInstanceOf(IllegalArgumentException.class);

            assertThat(count(g, "JamesBlob")).isEqualTo(1L);
        });
    }

    @Test
    @DisplayName("Blob delete command with a named parameter removes only the matching record")
    void namedParameterDeletesOneBlob(@TempDir Path workingDir) throws Exception {
        withGraph(workingDir, g -> {
            addBlob(g, "b", "b/x");
            addBlob(g, "b", "b/y");

            YouTrackDBTransactions.executeStrictTx(g, tx ->
                tx.command("DELETE VERTEX JamesBlob WHERE bucketAndBlobId = :key", "key", "b/x"));

            assertThat(count(g, "JamesBlob")).isEqualTo(1L);
            boolean exists = g.computeInTx(tx -> tx.V().hasLabel("JamesBlob").has("bucketAndBlobId", "b/y").hasNext());
            assertThat(exists).isTrue();
        });
    }

    @Test
    @DisplayName("Bucket delete command removes every blob of the bucket and keeps other buckets")
    void namedParameterDeletesWholeBucket(@TempDir Path workingDir) throws Exception {
        withGraph(workingDir, g -> {
            addBlob(g, "a", "a/1");
            addBlob(g, "a", "a/2");
            addBlob(g, "c", "c/1");

            YouTrackDBTransactions.executeStrictTx(g, tx ->
                tx.command("DELETE VERTEX JamesBlob WHERE bucket = :bucket", "bucket", "a"));

            assertThat(count(g, "JamesBlob")).isEqualTo(1L);
        });
    }

    @Test
    @DisplayName("RRT delete command with two named parameters removes exactly one mapping")
    void twoNamedParametersDeleteOneMapping(@TempDir Path workingDir) throws Exception {
        withGraph(workingDir, g -> {
            for (String[] row : new String[][] {{"s1", "m1"}, {"s1", "m2"}, {"s2", "m1"}}) {
                YouTrackDBTransactions.executeStrictTx(g, tx -> tx.addV("JamesRRTMapping")
                    .property("source", row[0])
                    .property("mapping", row[1])
                    .iterate());
            }

            YouTrackDBTransactions.executeStrictTx(g, tx ->
                tx.command("DELETE VERTEX JamesRRTMapping WHERE source = :source AND mapping = :mapping",
                    "source", "s1", "mapping", "m1"));

            assertThat(count(g, "JamesRRTMapping")).isEqualTo(2L);
            boolean mappingExists = g.computeInTx(tx -> tx.V().hasLabel("JamesRRTMapping")
                .has("source", "s1").has("mapping", "m1").hasNext());
            assertThat(mappingExists).isFalse();
        });
    }

    @Test
    @DisplayName("Domain delete command removes exact domain and preserves other domains")
    void domainDeleteCommandRemovesExactDomain(@TempDir Path workingDir) throws Exception {
        withGraph(workingDir, g -> {
            for (String d : new String[] {"example.org", "domain.test"}) {
                YouTrackDBTransactions.executeStrictTx(g, tx -> tx.addV("JamesDomain")
                    .property("domain", d)
                    .iterate());
            }

            YouTrackDBTransactions.executeStrictTx(g, tx ->
                tx.command("DELETE VERTEX JamesDomain WHERE domain = :domain", "domain", "example.org"));

            assertThat(count(g, "JamesDomain")).isEqualTo(1L);
            boolean preserved = g.computeInTx(tx -> tx.V().hasLabel("JamesDomain")
                .has("domain", "domain.test").hasNext());
            assertThat(preserved).isTrue();
        });
    }

    // ---- Queue, through the real application class ------------------------------------

    private static YouTrackDBMailQueueFactory newFactory(YTDBGraphTraversalSource g) {
        return new YouTrackDBMailQueueFactory(
            (queueItem, mailQueueName) -> new MailQueueItemDecoratorFactory.MailQueueItemDecorator(queueItem) {
                @Override
                public Mail getMail() {
                    return mailQueueItem.getMail();
                }

                @Override
                public void done(MailQueue.MailQueueItem.CompletionStatus status) throws MailQueue.MailQueueException {
                    mailQueueItem.done(status);
                }
            }, g);
    }

    private static Mail mail(String name) throws Exception {
        return MailImpl.builder()
            .name(name)
            .sender("sender@acid.local")
            .addRecipient("recipient@acid.local")
            .mimeMessage(MimeMessageBuilder.mimeMessageBuilder().setSubject(name).setText("body " + name))
            .build();
    }

    @Test
    @DisplayName("Queue clear(): removes memory and database records")
    void queueClearRemovesDatabaseRecords(@TempDir Path workingDir) throws Exception {
        withGraph(workingDir, g -> {
            YouTrackDBMailQueueFactory factory = newFactory(g);
            ManageableMailQueue queue = factory.createQueue(MailQueueName.of("spool"));
            queue.enQueue(mail("m1"));
            queue.enQueue(mail("m2"));
            assertThat(count(g, "JamesQueueItem")).isEqualTo(2L);

            long cleared = queue.clear();

            assertThat(cleared).isEqualTo(2L);
            assertThat(count(g, "JamesQueueItem")).isZero();
            factory.clean();
        });
    }

    @Test
    @DisplayName("Queue RETRY: the same mail name can be enqueued again and stays a single record")
    void queueRetryKeepsSingleRecord(@TempDir Path workingDir) throws Exception {
        withGraph(workingDir, g -> {
            YouTrackDBMailQueueFactory factory = newFactory(g);
            ManageableMailQueue queue = factory.createQueue(MailQueueName.of("spool"));
            queue.enQueue(mail("retry-me"));

            MailQueue.MailQueueItem item = Mono.from(queue.deQueue()).block(Duration.ofSeconds(10));
            assertThat(item).isNotNull();
            item.done(MailQueue.MailQueueItem.CompletionStatus.RETRY);

            assertThat(count(g, "JamesQueueItem")).isEqualTo(1L);
            assertThat(queue.getSize()).isEqualTo(1L);
            factory.clean();
        });
    }

    @Test
    @DisplayName("Queue SUCCESS and REJECT: the database record is removed synchronously")
    void queueSuccessAndRejectRemoveRecord(@TempDir Path workingDir) throws Exception {
        withGraph(workingDir, g -> {
            YouTrackDBMailQueueFactory factory = newFactory(g);
            ManageableMailQueue queue = factory.createQueue(MailQueueName.of("spool"));
            queue.enQueue(mail("ok"));
            queue.enQueue(mail("ko"));

            MailQueue.MailQueueItem first = Mono.from(queue.deQueue()).block(Duration.ofSeconds(10));
            first.done(MailQueue.MailQueueItem.CompletionStatus.SUCCESS);
            assertThat(count(g, "JamesQueueItem")).isEqualTo(1L);

            MailQueue.MailQueueItem second = Mono.from(queue.deQueue()).block(Duration.ofSeconds(10));
            second.done(MailQueue.MailQueueItem.CompletionStatus.REJECT);
            assertThat(count(g, "JamesQueueItem")).isZero();
            factory.clean();
        });
    }

    @Test
    @DisplayName("Queue remove(Name): removes the matching record only")
    void queueRemoveByNameRemovesOneRecord(@TempDir Path workingDir) throws Exception {
        withGraph(workingDir, g -> {
            YouTrackDBMailQueueFactory factory = newFactory(g);
            ManageableMailQueue queue = factory.createQueue(MailQueueName.of("spool"));
            queue.enQueue(mail("keep"));
            queue.enQueue(mail("drop"));

            long removed = queue.remove(ManageableMailQueue.Type.Name, "drop");

            assertThat(removed).isEqualTo(1L);
            assertThat(count(g, "JamesQueueItem")).isEqualTo(1L);
            factory.clean();
        });
    }
}
