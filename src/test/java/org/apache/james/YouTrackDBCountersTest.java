package org.apache.james;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.nio.file.Path;
import java.util.Optional;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.james.core.Username;
import org.apache.james.mailbox.MessageUid;
import org.apache.james.mailbox.ModSeq;
import org.apache.james.mailbox.model.Mailbox;
import org.apache.james.mailbox.model.MailboxPath;
import org.apache.james.mailbox.model.UidValidity;
import org.apache.james.youtrackdb.YouTrackDBMailboxMapper;
import org.apache.james.youtrackdb.YouTrackDBModSeqProvider;
import org.apache.james.youtrackdb.YouTrackDBUidProvider;
import org.apache.james.youtrackdb.YouTrackDBTransactions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.jetbrains.youtrackdb.api.DatabaseType;
import com.jetbrains.youtrackdb.api.YouTrackDB;
import com.jetbrains.youtrackdb.api.YourTracks;
import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;

public class YouTrackDBCountersTest {

    @TempDir
    Path tempDir;

    private YouTrackDB ytdb;
    private YTDBGraphTraversalSource g;
    private YouTrackDBMailboxMapper mailboxMapper;
    private YouTrackDBUidProvider uidProvider;
    private YouTrackDBModSeqProvider modSeqProvider;

    @BeforeEach
    void setUp() {
        File dbDir = tempDir.resolve("var").resolve("youtrackdb").toFile();
        dbDir.mkdirs();
        ytdb = YourTracks.instance(dbDir.getAbsolutePath());
        ytdb.createIfNotExists("james", DatabaseType.DISK, "admin", "admin", "admin");
        g = ytdb.openTraversal("james", "admin", "admin");

        YouTrackDBTransactions.executeStrictTx(g, tx -> {
            tx.command("CREATE CLASS JamesMailbox IF NOT EXISTS EXTENDS V");
            tx.command("CREATE PROPERTY JamesMailbox.mailboxId IF NOT EXISTS STRING");
            tx.command("CREATE PROPERTY JamesMailbox.namespace IF NOT EXISTS STRING");
            tx.command("CREATE PROPERTY JamesMailbox.user IF NOT EXISTS STRING");
            tx.command("CREATE PROPERTY JamesMailbox.name IF NOT EXISTS STRING");
            tx.command("CREATE PROPERTY JamesMailbox.uidValidity IF NOT EXISTS LONG");
            tx.command("CREATE PROPERTY JamesMailbox.lastUid IF NOT EXISTS LONG");
            tx.command("CREATE PROPERTY JamesMailbox.highestModSeq IF NOT EXISTS LONG");
            tx.command("CREATE PROPERTY JamesMailbox.acl IF NOT EXISTS STRING");
            tx.command("CREATE INDEX JamesMailbox.mailboxId IF NOT EXISTS UNIQUE");
            tx.command("CREATE INDEX JamesMailbox.path IF NOT EXISTS ON JamesMailbox (namespace, user, name) UNIQUE");
        });

        mailboxMapper = new YouTrackDBMailboxMapper(g);
        uidProvider = new YouTrackDBUidProvider(g);
        modSeqProvider = new YouTrackDBModSeqProvider(g);
    }

    @AfterEach
    void tearDown() {
        if (g != null) {
            try {
                g.close();
            } catch (Exception ignored) {
            }
        }
        if (ytdb != null) {
            try {
                ytdb.close();
            } catch (Exception ignored) {
            }
        }
    }

    @Test
    void uidsShouldBeStrictlyIncreasingAndPersistent() throws Exception {
        MailboxPath path = MailboxPath.forUser(Username.of("alice"), "INBOX");
        Mailbox mailbox = mailboxMapper.create(path, UidValidity.of(100L)).block();

        assertThat(uidProvider.lastUid(mailbox)).isEmpty();

        MessageUid uid1 = uidProvider.nextUid(mailbox);
        MessageUid uid2 = uidProvider.nextUid(mailbox);
        MessageUid uid3 = uidProvider.nextUid(mailbox);

        assertThat(uid1).isEqualTo(MessageUid.of(1L));
        assertThat(uid2).isEqualTo(MessageUid.of(2L));
        assertThat(uid3).isEqualTo(MessageUid.of(3L));

        assertThat(uidProvider.lastUid(mailbox)).contains(MessageUid.of(3L));
    }

    @Test
    void modSeqsShouldBeStrictlyIncreasingAndPersistent() throws Exception {
        MailboxPath path = MailboxPath.forUser(Username.of("alice"), "Sent");
        Mailbox mailbox = mailboxMapper.create(path, UidValidity.of(200L)).block();

        assertThat(modSeqProvider.highestModSeq(mailbox)).isEqualTo(ModSeq.first());

        ModSeq modSeq1 = modSeqProvider.nextModSeq(mailbox);
        ModSeq modSeq2 = modSeqProvider.nextModSeq(mailbox);
        ModSeq modSeq3 = modSeqProvider.nextModSeq(mailbox);

        assertThat(modSeq1).isEqualTo(ModSeq.of(1L));
        assertThat(modSeq2).isEqualTo(ModSeq.of(2L));
        assertThat(modSeq3).isEqualTo(ModSeq.of(3L));

        assertThat(modSeqProvider.highestModSeq(mailbox)).isEqualTo(ModSeq.of(3L));
    }

    @Test
    void countersShouldRemainIsolatedPerMailbox() throws Exception {
        Mailbox inbox = mailboxMapper.create(MailboxPath.forUser(Username.of("alice"), "INBOX"), UidValidity.of(1L)).block();
        Mailbox trash = mailboxMapper.create(MailboxPath.forUser(Username.of("alice"), "Trash"), UidValidity.of(2L)).block();

        MessageUid inboxUid = uidProvider.nextUid(inbox);
        MessageUid trashUid = uidProvider.nextUid(trash);

        assertThat(inboxUid).isEqualTo(MessageUid.of(1L));
        assertThat(trashUid).isEqualTo(MessageUid.of(1L));

        ModSeq inboxModSeq = modSeqProvider.nextModSeq(inbox);
        ModSeq trashModSeq = modSeqProvider.nextModSeq(trash);

        assertThat(inboxModSeq).isEqualTo(ModSeq.of(1L));
        assertThat(trashModSeq).isEqualTo(ModSeq.of(1L));
    }

    @Test
    void concurrentUidAllocationShouldProduceUniqueMonotonicUids() throws Exception {
        Mailbox mailbox = mailboxMapper.create(MailboxPath.forUser(Username.of("alice"), "ConcurrentBox"), UidValidity.of(300L)).block();

        int threadCount = 8;
        int allocationsPerThread = 25;
        int totalAllocations = threadCount * allocationsPerThread;

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        ConcurrentLinkedQueue<MessageUid> allocatedUids = new ConcurrentLinkedQueue<>();

        ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < allocationsPerThread; j++) {
                        allocatedUids.add(uidProvider.nextUid(mailbox));
                    }
                } catch (Throwable t) {
                    errors.add(t);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        boolean completed = doneLatch.await(30, TimeUnit.SECONDS);
        executor.shutdown();
        assertThat(completed).as("Threads should complete within 30 seconds").isTrue();

        if (!errors.isEmpty()) {
            errors.peek().printStackTrace();
        }
        assertThat(errors).isEmpty();
        assertThat(allocatedUids).hasSize(totalAllocations);
        java.util.Set<Long> uniqueLongs = allocatedUids.stream()
            .map(MessageUid::asLong)
            .collect(java.util.stream.Collectors.toSet());
        assertThat(uniqueLongs).hasSize(totalAllocations);

        // Verify lastUid equals totalAllocations
        assertThat(uidProvider.lastUid(mailbox)).contains(MessageUid.of(totalAllocations));
    }

    @Test
    void concurrentModSeqAllocationShouldProduceUniqueMonotonicModSeqs() throws Exception {
        Mailbox mailbox = mailboxMapper.create(MailboxPath.forUser(Username.of("alice"), "ConcurrentModSeqBox"), UidValidity.of(300L)).block();

        int threadCount = 8;
        int allocationsPerThread = 25;
        int totalAllocations = threadCount * allocationsPerThread;

        ExecutorService executor = Executors.newFixedThreadPool(threadCount);
        CountDownLatch startLatch = new CountDownLatch(1);
        CountDownLatch doneLatch = new CountDownLatch(threadCount);
        ConcurrentLinkedQueue<ModSeq> allocatedModSeqs = new ConcurrentLinkedQueue<>();
        ConcurrentLinkedQueue<Throwable> errors = new ConcurrentLinkedQueue<>();

        for (int i = 0; i < threadCount; i++) {
            executor.submit(() -> {
                try {
                    startLatch.await();
                    for (int j = 0; j < allocationsPerThread; j++) {
                        allocatedModSeqs.add(modSeqProvider.nextModSeq(mailbox));
                    }
                } catch (Throwable t) {
                    errors.add(t);
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        boolean completed = doneLatch.await(30, TimeUnit.SECONDS);
        executor.shutdown();
        assertThat(completed).as("Threads should complete within 30 seconds").isTrue();

        if (!errors.isEmpty()) {
            errors.peek().printStackTrace();
        }
        assertThat(errors).isEmpty();
        assertThat(allocatedModSeqs).hasSize(totalAllocations);
        java.util.Set<Long> uniqueLongs = allocatedModSeqs.stream()
            .map(ModSeq::asLong)
            .collect(java.util.stream.Collectors.toSet());
        assertThat(uniqueLongs).hasSize(totalAllocations);

        assertThat(modSeqProvider.highestModSeq(mailbox)).isEqualTo(ModSeq.of(totalAllocations));
    }

    @Test
    void isRetryableConflictShouldClassifyEngineExceptions() {
        com.jetbrains.youtrackdb.internal.core.id.RecordId dummyRid =
            new com.jetbrains.youtrackdb.internal.core.id.RecordId(1, 1L);
        com.jetbrains.youtrackdb.api.exception.ConcurrentModificationException cme =
            new com.jetbrains.youtrackdb.api.exception.ConcurrentModificationException("testDb", dummyRid, 1L, 2L, 1);
        com.jetbrains.youtrackdb.internal.core.exception.ConcurrentCreateException cce =
            new com.jetbrains.youtrackdb.internal.core.exception.ConcurrentCreateException("testDb", dummyRid, dummyRid);
        com.jetbrains.youtrackdb.internal.core.exception.CommandInterruptedException cie =
            new com.jetbrains.youtrackdb.internal.core.exception.CommandInterruptedException("testDb", "interrupted");

        assertThat(YouTrackDBTransactions.isRetryableConflict(cme)).isTrue();
        assertThat(YouTrackDBTransactions.isRetryableConflict(new RuntimeException("wrapped", cme))).isTrue();
        assertThat(YouTrackDBTransactions.isRetryableConflict(cce)).isTrue();
        assertThat(YouTrackDBTransactions.isRetryableConflict(cie)).isFalse();
    }

    @Test
    void retryOnConflictShouldRetryAndSucceedWhenConflictResolves() throws Exception {
        com.jetbrains.youtrackdb.internal.core.id.RecordId dummyRid =
            new com.jetbrains.youtrackdb.internal.core.id.RecordId(1, 1L);
        com.jetbrains.youtrackdb.api.exception.ConcurrentModificationException cme =
            new com.jetbrains.youtrackdb.api.exception.ConcurrentModificationException("testDb", dummyRid, 1L, 2L, 1);

        java.util.concurrent.atomic.AtomicInteger attempts = new java.util.concurrent.atomic.AtomicInteger();
        String result = YouTrackDBTransactions.retryOnConflict(3, () -> {
            if (attempts.incrementAndGet() < 3) {
                throw new RuntimeException("optimistic lock conflict", cme);
            }
            return "success";
        });

        assertThat(result).isEqualTo("success");
        assertThat(attempts.get()).isEqualTo(3);
    }

    @Test
    void retryOnConflictShouldThrowWhenRetriesExhausted() {
        com.jetbrains.youtrackdb.internal.core.id.RecordId dummyRid =
            new com.jetbrains.youtrackdb.internal.core.id.RecordId(1, 1L);
        com.jetbrains.youtrackdb.api.exception.ConcurrentModificationException cme =
            new com.jetbrains.youtrackdb.api.exception.ConcurrentModificationException("testDb", dummyRid, 1L, 2L, 1);

        java.util.concurrent.atomic.AtomicInteger attempts = new java.util.concurrent.atomic.AtomicInteger();
        assertThatThrownBy(() -> YouTrackDBTransactions.retryOnConflict(2, () -> {
            attempts.incrementAndGet();
            throw new RuntimeException("persistent conflict", cme);
        })).hasCause(cme);

        assertThat(attempts.get()).isEqualTo(2);
    }

    @Test
    void retryOnConflictShouldFailFastOnNonRetryableException() {
        com.jetbrains.youtrackdb.internal.core.exception.CommandInterruptedException cie =
            new com.jetbrains.youtrackdb.internal.core.exception.CommandInterruptedException("testDb", "interrupted");

        java.util.concurrent.atomic.AtomicInteger attempts = new java.util.concurrent.atomic.AtomicInteger();
        assertThatThrownBy(() -> YouTrackDBTransactions.retryOnConflict(5, () -> {
            attempts.incrementAndGet();
            throw cie;
        })).isEqualTo(cie);

        assertThat(attempts.get()).isEqualTo(1);
    }
}
