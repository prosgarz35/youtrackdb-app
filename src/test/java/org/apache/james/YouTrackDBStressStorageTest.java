package org.apache.james;

import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.james.modules.protocols.SmtpGuiceProbe;
import org.apache.james.utils.DataProbeImpl;
import org.apache.james.youtrackdb.YouTrackDBJamesConfiguration;
import org.apache.james.youtrackdb.YouTrackDBJamesServerMain;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 1 Million Messages Stress & Three-Tier Storage Benchmark.
 *
 * Tiers:
 * 1. 250,000 x 1 KB  (INLINE_RAW: raw vertex property in DB, no compression)
 * 2. 250,000 x 4 KB  (INLINE_RAW boundary / early INLINE_ZSTD)
 * 3. 250,000 x 32 KB (INLINE_ZSTD: compressed in vertex, within 64 KB page limit)
 * 4. 250,000 x 65 KB (FILE_ZSTD: external hierarchical filesystem storage var/blobs)
 *
 * Configurable via System Properties:
 * -Dstress.messages.per.tier=250000 (default: 250,000 -> 1M total)
 * -Dstress.concurrency=32          (default: 32 concurrent virtual threads)
 */
@Tag("benchmark")
public class YouTrackDBStressStorageTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(YouTrackDBStressStorageTest.class);

    @RegisterExtension
    static JamesServerExtension jamesServerExtension = new JamesServerBuilder<YouTrackDBJamesConfiguration>(tmpDir ->
        YouTrackDBJamesConfiguration.builder()
            .workingDirectory(tmpDir)
            .configurationFromClasspath()
            .build())
        .server(YouTrackDBJamesServerMain::createServer)
        .lifeCycle(JamesServerExtension.Lifecycle.PER_CLASS)
        .build();

    static final String DOMAIN = "stresstest.local";
    static final String USER = "bench@" + DOMAIN;
    static final String PASSWORD = "secretpassword";

    record TierConfig(String name, int targetSizeBytes, String storageTypeDesc) {}

    record TierResult(
        String tierName,
        int targetSizeBytes,
        String storageType,
        int totalMessages,
        int successCount,
        int failureCount,
        long durationMs,
        double throughput,
        long minLatencyMs,
        double avgLatencyMs,
        long p50Ms,
        long p95Ms,
        long p99Ms,
        long maxLatencyMs
    ) {}

    @Test
    void runStressStorageBenchmark(GuiceJamesServer jamesServer) throws Exception {
        int messagesPerTier = Integer.getInteger("stress.messages.per.tier", 250_000);
        int concurrency = Integer.getInteger("stress.concurrency", 8);

        jamesServer.getProbe(DataProbeImpl.class)
            .fluent()
            .addDomain(DOMAIN)
            .addUser(USER, PASSWORD);

        int smtpPort = jamesServer.getProbe(SmtpGuiceProbe.class).getSmtpPort().getValue();

        LOGGER.info("=== PRE-HEATING SMTP & STORAGE PIPELINE (50 messages) ===");
        runWarmup(smtpPort, 50);
        LOGGER.info("=== PRE-HEAT COMPLETE ===");

        TierConfig[] tiers = new TierConfig[] {
            new TierConfig("Tier 1: 1 KB", 1024, "INLINE_RAW (≤4 KB)"),
            new TierConfig("Tier 2: 4 KB", 4 * 1024, "INLINE_RAW boundary (4 KB)"),
            new TierConfig("Tier 3: 32 KB", 32 * 1024, "INLINE_ZSTD (4..64 KB)"),
            new TierConfig("Tier 4: 65 KB", 65 * 1024, "FILE_ZSTD (>64 KB)")
        };

        List<TierResult> results = new ArrayList<>();

        LOGGER.info("=== STARTING 1M STRESS TEST: {} msgs/tier x 4 tiers = {} total messages, Concurrency: {} ===",
            messagesPerTier, messagesPerTier * 4, concurrency);

        for (TierConfig tier : tiers) {
            TierResult res = executeTierBenchmark(smtpPort, tier, messagesPerTier, concurrency);
            results.add(res);
            // Brief cooldown between tiers for GC and flush stabilization
            System.gc();
            Thread.sleep(2000);
        }

        printReport(results, messagesPerTier * 4);
    }

    private void runWarmup(int smtpPort, int count) {
        byte[] payload = generatePayload(1024);
        for (int i = 0; i < count; i++) {
            sendSmtpMessage(smtpPort, "warmup-" + i, payload);
        }
    }

    private TierResult executeTierBenchmark(int smtpPort, TierConfig tier, int totalMessages, int concurrency) throws Exception {
        LOGGER.info(">>> BEGIN STAGE: {} (Payload: {} bytes, Storage: {}) - {} messages <<<",
            tier.name(), tier.targetSizeBytes(), tier.storageTypeDesc(), totalMessages);

        byte[] payload = generatePayload(tier.targetSizeBytes());
        ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
        Semaphore semaphore = new Semaphore(concurrency);
        CountDownLatch latch = new CountDownLatch(totalMessages);

        List<Long> latenciesMs = Collections.synchronizedList(new ArrayList<>(totalMessages));
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);

        long startNs = System.nanoTime();

        for (int i = 0; i < totalMessages; i++) {
            final int index = i;
            semaphore.acquire();
            executor.submit(() -> {
                long t0 = System.nanoTime();
                try {
                    boolean ok = sendSmtpMessage(smtpPort, "msg-" + tier.targetSizeBytes() + "-" + index, payload);
                    if (ok) {
                        long latency = (System.nanoTime() - t0) / 1_000_000;
                        latenciesMs.add(latency);
                        successCount.incrementAndGet();
                    } else {
                        failureCount.incrementAndGet();
                    }
                } catch (Exception e) {
                    failureCount.incrementAndGet();
                } finally {
                    semaphore.release();
                    latch.countDown();
                }
            });

            if ((i + 1) % 50_000 == 0) {
                long elapsedSec = (System.nanoTime() - startNs) / 1_000_000_000;
                double curRate = elapsedSec > 0 ? (i + 1.0) / elapsedSec : 0;
                LOGGER.info("Progress [{}] : {}/{} sent (~{}/s)", tier.name(), (i + 1), totalMessages, String.format(Locale.ROOT, "%.1f", curRate));
            }
        }

        latch.await(30, TimeUnit.MINUTES);
        executor.shutdown();
        executor.awaitTermination(30, TimeUnit.SECONDS);

        long durationMs = (System.nanoTime() - startNs) / 1_000_000;
        double throughput = (successCount.get() * 1000.0) / (durationMs > 0 ? durationMs : 1);

        List<Long> sorted = new ArrayList<>(latenciesMs);
        Collections.sort(sorted);

        long min = sorted.isEmpty() ? 0 : sorted.getFirst();
        long max = sorted.isEmpty() ? 0 : sorted.getLast();
        double avg = sorted.stream().mapToLong(Long::longValue).average().orElse(0.0);
        long p50 = sorted.isEmpty() ? 0 : sorted.get((int) (sorted.size() * 0.50));
        long p95 = sorted.isEmpty() ? 0 : sorted.get((int) (sorted.size() * 0.95));
        long p99 = sorted.isEmpty() ? 0 : sorted.get((int) (sorted.size() * 0.99));

        LOGGER.info("<<< FINISHED STAGE: {} in {} ms. Throughput: {} msgs/s. P50: {} ms, P95: {} ms, P99: {} ms >>>",
            tier.name(), durationMs, String.format(Locale.ROOT, "%.2f", throughput), p50, p95, p99);

        return new TierResult(
            tier.name(),
            tier.targetSizeBytes(),
            tier.storageTypeDesc(),
            totalMessages,
            successCount.get(),
            failureCount.get(),
            durationMs,
            throughput,
            min,
            avg,
            p50,
            p95,
            p99,
            max
        );
    }

    private static boolean sendSmtpMessage(int smtpPort, String messageId, byte[] payload) {
        try (Socket socket = new Socket("127.0.0.1", smtpPort);
             BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.US_ASCII));
             OutputStream rawOut = socket.getOutputStream();
             BufferedOutputStream bufferedOut = new BufferedOutputStream(rawOut, 65536);
             PrintWriter writer = new PrintWriter(bufferedOut, false)) {

            reader.readLine(); // 220 banner

            writer.print("HELO localhost\r\n");
            writer.flush();
            reader.readLine(); // 250

            writer.print("MAIL FROM:<stress-sender@stresstest.local>\r\n");
            writer.flush();
            reader.readLine(); // 250

            writer.print("RCPT TO:<" + USER + ">\r\n");
            writer.flush();
            reader.readLine(); // 250

            writer.print("DATA\r\n");
            writer.flush();
            reader.readLine(); // 354

            // Write headers
            writer.print("Subject: Stress Benchmark " + messageId + "\r\n");
            writer.print("From: stress-sender@stresstest.local\r\n");
            writer.print("To: " + USER + "\r\n");
            writer.print("Message-ID: <" + messageId + "@stresstest.local>\r\n");
            writer.print("\r\n");
            writer.flush();

            // Stream binary ASCII payload directly
            bufferedOut.write(payload);
            bufferedOut.write("\r\n.\r\n".getBytes(StandardCharsets.US_ASCII));
            bufferedOut.flush();

            String dataAck = reader.readLine(); // 250 OK
            boolean success = dataAck != null && dataAck.startsWith("250");

            writer.print("QUIT\r\n");
            writer.flush();
            reader.readLine(); // 221

            return success;
        } catch (Exception e) {
            return false;
        }
    }

    private static byte[] generatePayload(int targetBytes) {
        byte[] buffer = new byte[targetBytes];
        // Fill with repetitive text with some variability to allow realistic ~40% Zstd compression
        String pattern = "YouTrackDB James High-Throughput Mail Storage Engine Compression Line.\r\n";
        byte[] patternBytes = pattern.getBytes(StandardCharsets.US_ASCII);
        int offset = 0;
        while (offset < targetBytes) {
            int toCopy = Math.min(patternBytes.length, targetBytes - offset);
            System.arraycopy(patternBytes, 0, buffer, offset, toCopy);
            offset += toCopy;
        }
        return buffer;
    }

    private static void printReport(List<TierResult> results, int grandTotal) {
        System.out.println();
        System.out.println("=========================================================================================================================");
        System.out.println("                                YOUTRACKDB 3-TIER STORAGE & COMPRESSION STRESS BENCHMARK                                 ");
        System.out.println("=========================================================================================================================");
        System.out.println(String.format(Locale.ROOT,
            "%-15s | %-18s | %-10s | %-8s | %-12s | %-10s | %-8s | %-8s | %-8s | %-8s",
            "Tier Stage", "Storage Mechanism", "Payload", "Success", "Time (sec)", "Throughput", "P50 ms", "P95 ms", "P99 ms", "Max ms"));
        System.out.println("-------------------------------------------------------------------------------------------------------------------------");

        long totalDurationMs = 0;
        int totalSuccess = 0;
        int totalFail = 0;

        for (TierResult r : results) {
            totalDurationMs += r.durationMs();
            totalSuccess += r.successCount();
            totalFail += r.failureCount();

            System.out.println(String.format(Locale.ROOT,
                "%-15s | %-18s | %-10s | %-8d | %-12.2f | %-10.1f | %-8d | %-8d | %-8d | %-8d",
                r.tierName(),
                r.storageType(),
                formatBytes(r.targetSizeBytes()),
                r.successCount(),
                r.durationMs() / 1000.0,
                r.throughput(),
                r.p50Ms(),
                r.p95Ms(),
                r.p99Ms(),
                r.maxLatencyMs()
            ));
        }

        double grandThroughput = (totalSuccess * 1000.0) / (totalDurationMs > 0 ? totalDurationMs : 1);

        System.out.println("=========================================================================================================================");
        System.out.println(String.format(Locale.ROOT, "SUMMARY: Total Processed: %d / %d (Failures: %d) | Total Time: %.2f s | Overall Throughput: %.1f msgs/s",
            totalSuccess, grandTotal, totalFail, totalDurationMs / 1000.0, grandThroughput));
        System.out.println("=========================================================================================================================");
        System.out.println();
    }

    private static String formatBytes(long bytes) {
        if (bytes >= 1024 * 1024) {
            return String.format(Locale.ROOT, "%.1f MB", bytes / (1024.0 * 1024.0));
        }
        return String.format(Locale.ROOT, "%.1f KB", bytes / 1024.0);
    }
}
