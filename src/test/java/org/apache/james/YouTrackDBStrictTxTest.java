package org.apache.james;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.nio.file.Path;

import org.apache.james.youtrackdb.YouTrackDBTransactions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.jetbrains.youtrackdb.api.DatabaseType;
import com.jetbrains.youtrackdb.api.YouTrackDB;
import com.jetbrains.youtrackdb.api.YourTracks;
import com.jetbrains.youtrackdb.api.exception.RecordDuplicatedException;
import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;

/**
 * Regression tests for commit-failure propagation.
 * NOT RUN YET: written against the engine API read in J:\youtrackdb (main).
 */
public class YouTrackDBStrictTxTest {

    private static final String CLASS = "StrictItem";

    private interface WithGraph {
        void run(YTDBGraphTraversalSource g) throws Exception;
    }

    private static void withGraph(Path workingDir, WithGraph body) throws Exception {
        File dbDir = workingDir.resolve("var").resolve("youtrackdb").toFile();
        dbDir.mkdirs();
        try (YouTrackDB ytdb = YourTracks.instance(dbDir.getAbsolutePath())) {
            ytdb.createIfNotExists("james", DatabaseType.DISK, "admin", "admin", "admin");
            try (YTDBGraphTraversalSource g = ytdb.openTraversal("james", "admin", "admin")) {
                g.executeInTx(tx -> {
                    tx.command("CREATE CLASS " + CLASS + " IF NOT EXISTS EXTENDS V");
                    tx.command("CREATE PROPERTY " + CLASS + ".key IF NOT EXISTS STRING");
                    tx.command("CREATE INDEX " + CLASS + ".key IF NOT EXISTS ON " + CLASS + " (key) UNIQUE");
                });
                body.run(g);
            }
        }
    }

    private static long count(YTDBGraphTraversalSource g) {
        return g.computeInTx(tx -> tx.V().hasLabel(CLASS).count().next());
    }

    @Test
    @DisplayName("Strict tx: unique index violation at commit reaches the caller")
    void strictTxPropagatesCommitFailure(@TempDir Path workingDir) throws Exception {
        withGraph(workingDir, g -> {
            YouTrackDBTransactions.executeStrictTx(g, tx -> tx.addV(CLASS).property("key", "a").iterate());

            assertThatThrownBy(() ->
                YouTrackDBTransactions.executeStrictTx(g, tx -> tx.addV(CLASS).property("key", "a").iterate()))
                .satisfies(e -> assertThat(YouTrackDBTransactions.hasCause(e, RecordDuplicatedException.class))
                    .as("cause chain of %s", e)
                    .isTrue());

            assertThat(count(g)).isEqualTo(1L);
        });
    }

    // Note: upstream YouTrackDB 0.5.0-SNAPSHOT plain g.executeInTx swallows commit failures in finishTx().
    // YouTrackDBTransactions.executeStrictTx explicitly calls tx.tx().commit(), ensuring all commit failures
    // (such as unique index conflicts) are reliably propagated to the caller without requiring custom engine patches.

    @Test
    @DisplayName("Strict tx: exception inside the lambda rolls everything back")
    void strictTxRollsBackOnLambdaException(@TempDir Path workingDir) throws Exception {
        withGraph(workingDir, g -> {
            assertThatThrownBy(() ->
                YouTrackDBTransactions.executeStrictTx(g, tx -> {
                    tx.addV(CLASS).property("key", "b").iterate();
                    throw new IllegalStateException("boom");
                }))
                .isInstanceOf(IllegalStateException.class);

            assertThat(count(g)).isZero();
        });
    }

    @Test
    @DisplayName("Strict tx: computeStrictTx commits and returns the lambda result")
    void computeStrictTxReturnsResult(@TempDir Path workingDir) throws Exception {
        withGraph(workingDir, g -> {
            String result = YouTrackDBTransactions.computeStrictTx(g, tx -> {
                tx.addV(CLASS).property("key", "c").iterate();
                return "done";
            });

            assertThat(result).isEqualTo("done");
            assertThat(count(g)).isEqualTo(1L);
        });
    }

    @Test
    @DisplayName("Strict tx: a checked exception is thrown as is, without wrapping")
    void strictTxKeepsCheckedException(@TempDir Path workingDir) throws Exception {
        withGraph(workingDir, g ->
            assertThatThrownBy(() ->
                YouTrackDBTransactions.executeStrictTx(g, tx -> {
                    throw new java.io.IOException("checked");
                }))
                .isInstanceOf(java.io.IOException.class));
    }

    @Test
    @DisplayName("retryOnConflict: successfully returns on first attempt without conflicts")
    void retryOnConflictSucceedsImmediately() throws Exception {
        java.util.concurrent.atomic.AtomicInteger attempts = new java.util.concurrent.atomic.AtomicInteger();
        String result = YouTrackDBTransactions.retryOnConflict(() -> {
            attempts.incrementAndGet();
            return "ok";
        });
        assertThat(result).isEqualTo("ok");
        assertThat(attempts.get()).isEqualTo(1);
    }

    @Test
    @DisplayName("retryOnConflict: retries upon ConcurrentModificationException and succeeds")
    void retryOnConflictRetriesAndSucceeds() throws Exception {
        java.util.concurrent.atomic.AtomicInteger attempts = new java.util.concurrent.atomic.AtomicInteger();
        String result = YouTrackDBTransactions.retryOnConflict(5, () -> {
            if (attempts.incrementAndGet() < 3) {
                throw new com.jetbrains.youtrackdb.api.exception.ConcurrentModificationException(
                    "james", new com.jetbrains.youtrackdb.internal.core.id.RecordId(1, 1), 1L, 2L, 1);
            }
            return "recovered";
        });
        assertThat(result).isEqualTo("recovered");
        assertThat(attempts.get()).isEqualTo(3);
    }

    @Test
    @DisplayName("retryOnConflict: does not retry non-retryable exceptions")
    void retryOnConflictFailsFastOnNonRetryableException() {
        java.util.concurrent.atomic.AtomicInteger attempts = new java.util.concurrent.atomic.AtomicInteger();
        assertThatThrownBy(() ->
            YouTrackDBTransactions.retryOnConflict(5, () -> {
                attempts.incrementAndGet();
                throw new IllegalArgumentException("fatal validation error");
            }))
            .isInstanceOf(IllegalArgumentException.class);

        assertThat(attempts.get()).isEqualTo(1);
    }

    @Test
    @DisplayName("retryOnConflict: rethrows conflict exception when max retries exhausted")
    void retryOnConflictExhaustsRetries() {
        java.util.concurrent.atomic.AtomicInteger attempts = new java.util.concurrent.atomic.AtomicInteger();
        assertThatThrownBy(() ->
            YouTrackDBTransactions.retryOnConflict(3, () -> {
                attempts.incrementAndGet();
                throw new com.jetbrains.youtrackdb.api.exception.ConcurrentModificationException(
                    "james", new com.jetbrains.youtrackdb.internal.core.id.RecordId(1, 1), 1L, 2L, 1);
            }))
            .isInstanceOf(com.jetbrains.youtrackdb.api.exception.ConcurrentModificationException.class);

        assertThat(attempts.get()).isEqualTo(3);
    }
}
