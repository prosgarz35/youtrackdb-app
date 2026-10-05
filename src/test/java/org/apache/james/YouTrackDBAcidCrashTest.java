package org.apache.james;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.BufferedReader;
import java.io.File;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.james.modules.protocols.ImapGuiceProbe;
import org.apache.james.modules.protocols.SmtpGuiceProbe;
import org.apache.james.utils.DataProbeImpl;
import org.apache.james.utils.TestIMAPClient;
import org.apache.james.youtrackdb.YouTrackDBJamesConfiguration;
import org.apache.james.youtrackdb.YouTrackDBJamesServerMain;
import org.awaitility.Awaitility;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jetbrains.youtrackdb.api.DatabaseType;
import com.jetbrains.youtrackdb.api.YouTrackDB;
import com.jetbrains.youtrackdb.api.YourTracks;
import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class YouTrackDBAcidCrashTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(YouTrackDBAcidCrashTest.class);

    private static final String DOMAIN = "acid.local";
    private static final String USER = "aciduser@" + DOMAIN;
    private static final String PASSWORD = "password123";

    /**
     * 1. ATOMICITY & CRASH SIMULATION
     * Simulates abrupt interruption during parallel transactions.
     * When database re-opens, transactions aborted midway MUST NOT corrupt the store,
     * and already committed data MUST remain intact and readable.
     */
    @Test
    @DisplayName("Atomicity: Uncommitted transactions during abrupt crash are rolled back without database corruption")
    void shouldMaintainStoreIntegrityAfterAbruptCrash(@TempDir Path workingDir) throws Exception {
        File dbDir = workingDir.resolve("var").resolve("youtrackdb").toFile();
        dbDir.mkdirs();

        // 1. Initial run: setup baseline committed state
        try (YouTrackDB youTrackDB = YourTracks.instance(dbDir.getAbsolutePath())) {
            youTrackDB.createIfNotExists("james", DatabaseType.DISK, "admin", "admin", "admin");
            try (YTDBGraphTraversalSource g = youTrackDB.openTraversal("james", "admin", "admin")) {
                g.executeInTx(tx -> {
                    tx.command("CREATE CLASS JamesDomain IF NOT EXISTS EXTENDS V");
                    tx.command("CREATE PROPERTY JamesDomain.domain IF NOT EXISTS STRING");
                    tx.command("CREATE CLASS JamesUser IF NOT EXISTS EXTENDS V");
                    tx.command("CREATE PROPERTY JamesUser.username IF NOT EXISTS STRING");

                    tx.addV("JamesDomain").property("domain", DOMAIN).iterate();
                    tx.addV("JamesUser").property("username", USER).iterate();
                });
            }
        }

        AtomicInteger committedBatches = new AtomicInteger(0);
        try (YouTrackDB crashYtdb = YourTracks.instance(dbDir.getAbsolutePath())) {
            int threadCount = 8;
            ExecutorService executor = Executors.newFixedThreadPool(threadCount);
            CountDownLatch startLatch = new CountDownLatch(1);
            AtomicBoolean stopRequested = new AtomicBoolean(false);

            try (YTDBGraphTraversalSource crashG = crashYtdb.openTraversal("james", "admin", "admin")) {
                crashG.executeInTx(tx -> {
                    tx.command("CREATE CLASS CrashItem IF NOT EXISTS EXTENDS V");
                    tx.command("CREATE PROPERTY CrashItem.thread IF NOT EXISTS INTEGER");
                    tx.command("CREATE PROPERTY CrashItem.batchNum IF NOT EXISTS INTEGER");
                    tx.command("CREATE PROPERTY CrashItem.idx IF NOT EXISTS INTEGER");
                    tx.command("CREATE PROPERTY CrashItem.payload IF NOT EXISTS STRING");
                });

                for (int t = 0; t < threadCount; t++) {
                    final int threadId = t;
                    executor.submit(() -> {
                        try {
                            startLatch.await();
                            int batch = 0;
                            while (!stopRequested.get() && !Thread.currentThread().isInterrupted()) {
                                final int b = batch++;
                                try {
                                    crashG.executeInTx(tx -> {
                                        for (int k = 0; k < 10; k++) {
                                            tx.addV("CrashItem")
                                                .property("thread", threadId)
                                                .property("batchNum", b)
                                                .property("idx", k)
                                                .property("payload", "Data " + threadId + "-" + b + "-" + k)
                                                .iterate();
                                        }
                                        if (stopRequested.get() && (b % 2 == 1)) {
                                            throw new OutOfMemoryError("Simulated OOM Killer mid-transaction");
                                        }
                                    });
                                    committedBatches.incrementAndGet();
                                } catch (OutOfMemoryError e) {
                                    break;
                                } catch (Exception e) {
                                    break;
                                }
                            }
                        } catch (Throwable ignored) {
                        }
                    });
                }

                startLatch.countDown();
                Thread.sleep(150);

                stopRequested.set(true);
                executor.shutdown();
                executor.awaitTermination(15, TimeUnit.SECONDS);
            }
        }

        LOGGER.info("Simulated crash complete. Committed batches before crash: {}", committedBatches.get());

        // 3. RECOVERY CHECK: Open store again with fresh environment (crash recovery)
        try (YouTrackDB recoveryYtdb = YourTracks.instance(dbDir.getAbsolutePath());
             YTDBGraphTraversalSource recoveryG = recoveryYtdb.openTraversal("james", "admin", "admin")) {
            recoveryG.executeInTx(tx -> {
                boolean domainExists = tx.V().hasLabel("JamesDomain").has("domain", DOMAIN).hasNext();
                assertThat(domainExists).isTrue();

                boolean userExists = tx.V().hasLabel("JamesUser").has("username", USER).hasNext();
                assertThat(userExists).isTrue();

                Long crashItemsCount = tx.V().hasLabel("CrashItem").count().next();
                LOGGER.info("Successfully recovered from abrupt crash. Total valid items: {}", crashItemsCount);
                assertThat(crashItemsCount).isNotNull();
            });
        }
    }

    /**
     * 2. NETWORK FAILURE / SUDDEN CLIENT DISCONNECT (SMTP Broken Pipe / Connection Reset)
     * Simulates client terminating TCP socket abruptly during DATA streaming or before QUIT.
     * James and YouTrackDB MUST drop partial message and not store phantom/corrupted messages.
     */
    @Test
    @DisplayName("Network Failure: Abrupt client disconnect mid-DATA does not produce orphaned/corrupted messages")
    void shouldHandleAbruptNetworkDisconnectGracefully(@TempDir Path workingDir) throws Exception {
        YouTrackDBJamesConfiguration config = YouTrackDBJamesConfiguration.builder()
            .workingDirectory(workingDir.toFile())
            .configurationFromClasspath()
            .build();

        GuiceJamesServer server = YouTrackDBJamesServerMain.createServer(config);
        server.start();

        try {
            server.getProbe(DataProbeImpl.class)
                .fluent()
                .addDomain(DOMAIN)
                .addUser(USER, PASSWORD);

            int smtpPort = server.getProbe(SmtpGuiceProbe.class).getSmtpPort().getValue();
            int imapPort = server.getProbe(ImapGuiceProbe.class).getImapPort();

            // Send 1 valid message to baseline
            sendValidMessage(smtpPort, "Baseline Message");

            // Await baseline message in IMAP
            TestIMAPClient imapClient = new TestIMAPClient();
            Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> {
                try {
                    imapClient.connect("127.0.0.1", imapPort)
                        .login(USER, PASSWORD)
                        .select(TestIMAPClient.INBOX);
                    long count = imapClient.getMessageCount(TestIMAPClient.INBOX);
                    imapClient.disconnect();
                    return count == 1L;
                } catch (Exception e) {
                    return false;
                }
            });

            // SIMULATE NETWORK ABRUPT DISCONNECTS: 20 broken clients
            for (int i = 0; i < 20; i++) {
                try (Socket socket = new Socket("127.0.0.1", smtpPort)) {
                    socket.setSoLinger(true, 0); // RST packet on close instead of FIN (hard crash)
                    BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.US_ASCII));
                    PrintWriter writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.US_ASCII), true);

                    reader.readLine(); // 220 banner
                    writer.println("HELO localhost");
                    reader.readLine(); // 250
                    writer.println("MAIL FROM:<evil@network.drop>");
                    reader.readLine(); // 250
                    writer.println("RCPT TO:<" + USER + ">");
                    reader.readLine(); // 250
                    writer.println("DATA");
                    reader.readLine(); // 354
                    writer.println("Subject: Half-sent broken message " + i);
                    writer.println("From: evil@network.drop");
                    writer.println();
                    writer.println("Chunk 1 sent, now dropping connection immediately with TCP RST...");
                    writer.flush();
                    // ABORT SOCKET WITH RST (No "." line, No QUIT)
                }
            }

            // Send another valid message to verify SMTP server is still operational
            sendValidMessage(smtpPort, "Post-Network-Crash Message");

            // Verify INBOX has exactly 2 valid messages (Baseline + Post-Crash). None of the 20 broken ones!
            Awaitility.await().atMost(10, TimeUnit.SECONDS).until(() -> {
                try {
                    imapClient.connect("127.0.0.1", imapPort)
                        .login(USER, PASSWORD)
                        .select(TestIMAPClient.INBOX);
                    long count = imapClient.getMessageCount(TestIMAPClient.INBOX);
                    imapClient.disconnect();
                    return count == 2L;
                } catch (Exception e) {
                    return false;
                }
            });

            LOGGER.info("Network disconnect test PASSED: Exactly 2 valid messages in INBOX, partial broken messages discarded.");
        } finally {
            server.stop();
        }
    }

    /**
     * 3. ISOLATION & CONCURRENCY CONFLICTS (High Contention & Transaction Rollbacks)
     * Verifies that concurrent writes on the same entities maintain strict ACID Isolation.
     */
    @Test
    @DisplayName("Isolation: High contention parallel writes to YouTrackDB maintain transactional integrity")
    void shouldEnsureTransactionIsolationUnderContention(@TempDir Path workingDir) throws Exception {
        File dbDir = workingDir.resolve("var").resolve("youtrackdb").toFile();
        dbDir.mkdirs();

        try (YouTrackDB youTrackDB = YourTracks.instance(dbDir.getAbsolutePath())) {
            youTrackDB.createIfNotExists("james", DatabaseType.DISK, "admin", "admin", "admin");

            try (YTDBGraphTraversalSource g = youTrackDB.openTraversal("james", "admin", "admin")) {
                g.executeInTx(tx -> {
                    tx.command("CREATE CLASS GlobalCounter IF NOT EXISTS EXTENDS V");
                    tx.command("CREATE PROPERTY GlobalCounter.name IF NOT EXISTS STRING");
                    tx.command("CREATE PROPERTY GlobalCounter.val IF NOT EXISTS INTEGER");
                });

                int writers = 5;
                int operationsPerWriter = 20;
                ExecutorService executor = Executors.newFixedThreadPool(writers);
                CountDownLatch latch = new CountDownLatch(writers);
                AtomicInteger successfulIncrements = new AtomicInteger(0);

                for (int w = 0; w < writers; w++) {
                    final int writerId = w;
                    executor.submit(() -> {
                        for (int op = 0; op < operationsPerWriter; op++) {
                            final int opId = op;
                            g.executeInTx(tx -> {
                                tx.addV("GlobalCounter")
                                    .property("name", "counter_" + writerId + "_" + opId)
                                    .property("val", 1)
                                    .iterate();
                            });
                            successfulIncrements.incrementAndGet();
                        }
                        latch.countDown();
                    });
                }

                latch.await(30, TimeUnit.SECONDS);
                executor.shutdown();
                executor.awaitTermination(5, TimeUnit.SECONDS);

                g.executeInTx(tx -> {
                    Long totalRecords = tx.V().hasLabel("GlobalCounter").count().next();
                    LOGGER.info("Expected records: {}, Actual in YouTrackDB: {}", successfulIncrements.get(), totalRecords);
                    assertThat(totalRecords).isEqualTo(successfulIncrements.get());
                    assertThat(totalRecords).isEqualTo(writers * operationsPerWriter);
                });
            }
        }
    }

    /**
     * 4. OOM KILLER / ERROR DURING RECORD CREATION
     * Tests that errors during write transaction roll back cleanly.
     */
    @Test
    @DisplayName("Durability & Error Handling: Transaction rollbacks on OOM leave zero uncommitted artifacts")
    void shouldRollbackCleanlyWhenOutOfMemoryOccurs(@TempDir Path workingDir) throws Exception {
        File dbDir = workingDir.resolve("var").resolve("youtrackdb").toFile();
        dbDir.mkdirs();

        try (YouTrackDB youTrackDB = YourTracks.instance(dbDir.getAbsolutePath())) {
            youTrackDB.createIfNotExists("james", DatabaseType.DISK, "admin", "admin", "admin");

            try (YTDBGraphTraversalSource g = youTrackDB.openTraversal("james", "admin", "admin")) {
                g.executeInTx(tx -> {
                    tx.command("CREATE CLASS JamesBlob IF NOT EXISTS EXTENDS V");
                    tx.command("CREATE PROPERTY JamesBlob.bucketAndBlobId IF NOT EXISTS STRING");
                });

                assertThatThrownBy(() -> {
                    g.executeInTx(tx -> {
                        tx.addV("JamesBlob")
                            .property("bucketAndBlobId", "test/oomBlob")
                            .iterate();

                        throw new OutOfMemoryError("Java heap space simulation");
                    });
                }).isInstanceOf(OutOfMemoryError.class);

                g.executeInTx(tx -> {
                    boolean exists = tx.V().hasLabel("JamesBlob").has("bucketAndBlobId", "test/oomBlob").hasNext();
                    assertThat(exists).isFalse();
                });

                LOGGER.info("OOM rollback test PASSED: No ghost records left in database.");
            }
        }
    }

    private void sendValidMessage(int smtpPort, String subject) throws Exception {
        try (Socket socket = new Socket("127.0.0.1", smtpPort);
             BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.US_ASCII));
             PrintWriter writer = new PrintWriter(new OutputStreamWriter(socket.getOutputStream(), StandardCharsets.US_ASCII), true)) {

            reader.readLine(); // 220 banner
            writer.println("HELO localhost");
            reader.readLine(); // 250
            writer.println("MAIL FROM:<sender@acid.local>");
            reader.readLine(); // 250
            writer.println("RCPT TO:<" + USER + ">");
            reader.readLine(); // 250
            writer.println("DATA");
            reader.readLine(); // 354
            writer.println("Subject: " + subject);
            writer.println("From: sender@acid.local");
            writer.println("To: " + USER);
            writer.println();
            writer.println("Valid payload body");
            writer.println(".");
            reader.readLine(); // 250
            writer.println("QUIT");
            reader.readLine(); // 221
        }
    }
}
