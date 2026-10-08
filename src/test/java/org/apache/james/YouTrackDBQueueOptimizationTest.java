package org.apache.james;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Properties;

import jakarta.mail.Session;
import jakarta.mail.internet.MimeMessage;

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

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Tests for the queue optimizations: one MIME serialization, plain-insert enqueue, one-transaction flush.
 *
 * NOT RUN YET. The expected bytes are "what Jakarta Mail writes for the same message before it enters the
 * queue", so the tests check that the queue does not change the message, not that Jakarta Mail is stable.
 */
public class YouTrackDBQueueOptimizationTest {

    private static final MailQueueName QUEUE = MailQueueName.of("spool");
    private static final Session SESSION = Session.getInstance(new Properties());

    private interface WithDb {
        void run(YTDBGraphTraversalSource g) throws Exception;
    }

    private static void withDb(Path workingDir, WithDb body) throws Exception {
        File dbDir = workingDir.resolve("var").resolve("youtrackdb").toFile();
        dbDir.mkdirs();
        try (YouTrackDB ytdb = YourTracks.instance(dbDir.getAbsolutePath())) {
            ytdb.createIfNotExists("james", DatabaseType.DISK, "admin", "admin", "admin");
            try (YTDBGraphTraversalSource g = ytdb.openTraversal("james", "admin", "admin")) {
                YouTrackDBTransactions.executeStrictTx(g, tx -> {
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

    private static byte[] emlBytes() throws IOException {
        try (InputStream in = YouTrackDBQueueOptimizationTest.class.getResourceAsStream("/eml/htmlMail.eml")) {
            assertThat(in).as("/eml/htmlMail.eml on the test classpath").isNotNull();
            return in.readAllBytes();
        }
    }

    private static MimeMessage parse(byte[] raw) throws Exception {
        return new MimeMessage(SESSION, new ByteArrayInputStream(raw));
    }

    private static byte[] bytesOf(MimeMessage message) throws Exception {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        message.writeTo(out);
        return out.toByteArray();
    }

    private static Mail mail(String name, MimeMessage message) throws Exception {
        return MailImpl.builder()
            .name(name)
            .sender("sender@acid.local")
            .addRecipient("recipient@acid.local")
            .mimeMessage(message)
            .build();
    }

    private static long count(YTDBGraphTraversalSource g) {
        return g.computeInTx(tx -> tx.V().hasLabel("JamesQueueItem").count().next());
    }

    private static long nextDeliveryOf(YTDBGraphTraversalSource g, String mailName) {
        return g.computeInTx(tx -> tx.V().hasLabel("JamesQueueItem").has("mailName", mailName).next().<Long>value("nextDelivery"));
    }

    @Test
    @DisplayName("The MIME bytes are unchanged by the queue, in memory and after a restart")
    void mimeBytesSurviveQueueAndRestart(@TempDir Path workingDir) throws Exception {
        byte[] raw = emlBytes();
        byte[] expected = bytesOf(parse(raw));

        withDb(workingDir, g -> {
            YouTrackDBMailQueueFactory factory = newFactory(g);
            ManageableMailQueue queue = factory.createQueue(QUEUE);
            queue.enQueue(mail("m-a", parse(raw)));
            queue.enQueue(mail("m-b", parse(raw)));

            List<MailQueue.MailQueueItem> items = Flux.from(queue.deQueue()).take(2).collectList().block(Duration.ofSeconds(10));
            assertThat(items).hasSize(2);
            for (MailQueue.MailQueueItem item : items) {
                assertThat(bytesOf(item.getMail().getMessage())).as("in-memory copy of %s", item.getMail().getName()).isEqualTo(expected);
            }
            // not completed on purpose: both records must still be in the database
            assertThat(count(g)).isEqualTo(2L);
            factory.clean();
        });

        withDb(workingDir, g -> {
            YouTrackDBMailQueueFactory factory = newFactory(g);
            ManageableMailQueue queue = factory.createQueue(QUEUE);
            assertThat(queue.getSize()).isEqualTo(2L);

            List<MailQueue.MailQueueItem> items = Flux.from(queue.deQueue()).take(2).collectList().block(Duration.ofSeconds(10));
            assertThat(items).hasSize(2);
            for (MailQueue.MailQueueItem item : items) {
                assertThat(bytesOf(item.getMail().getMessage())).as("recovered copy of %s", item.getMail().getName()).isEqualTo(expected);
                item.done(MailQueue.MailQueueItem.CompletionStatus.SUCCESS);
            }
            assertThat(count(g)).isZero();
            factory.clean();
        });
    }

    @Test
    @DisplayName("Enqueueing the same name twice creates distinct records by enqueueId and does not fail")
    void sameNameTwiceKeepsSingleRecord(@TempDir Path workingDir) throws Exception {
        byte[] raw = emlBytes();
        withDb(workingDir, g -> {
            YouTrackDBMailQueueFactory factory = newFactory(g);
            ManageableMailQueue queue = factory.createQueue(QUEUE);

            queue.enQueue(mail("dup", parse(raw)));
            queue.enQueue(mail("dup", parse(raw)));

            assertThat(count(g)).isEqualTo(2L);
            factory.clean();
        });
    }

    @Test
    @DisplayName("flush() makes delayed mails available now and updates their delivery time in the database")
    void flushMakesDelayedMailsAvailable(@TempDir Path workingDir) throws Exception {
        byte[] raw = emlBytes();
        withDb(workingDir, g -> {
            YouTrackDBMailQueueFactory factory = newFactory(g);
            ManageableMailQueue queue = factory.createQueue(QUEUE);
            queue.enQueue(mail("late", parse(raw)), Duration.ofHours(1));

            assertThatThrownBy(() -> Flux.from(queue.deQueue()).next().block(Duration.ofMillis(500)))
                .isInstanceOf(IllegalStateException.class);
            long before = nextDeliveryOf(g, "late");

            assertThat(queue.flush()).isEqualTo(1L);

            assertThat(nextDeliveryOf(g, "late")).isLessThan(before);
            MailQueue.MailQueueItem item = Mono.from(queue.deQueue()).block(Duration.ofSeconds(10));
            assertThat(item).isNotNull();
            assertThat(item.getMail().getName()).isEqualTo("late");
            assertThat(count(g)).isEqualTo(1L);
            factory.clean();
        });
    }

    @Test
    @DisplayName("flush() on an empty queue returns 0")
    void flushOnEmptyQueue(@TempDir Path workingDir) throws Exception {
        withDb(workingDir, g -> {
            YouTrackDBMailQueueFactory factory = newFactory(g);
            ManageableMailQueue queue = factory.createQueue(QUEUE);

            assertThat(queue.flush()).isZero();
            factory.clean();
        });
    }
}
