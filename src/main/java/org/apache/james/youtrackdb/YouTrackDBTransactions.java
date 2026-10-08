package org.apache.james.youtrackdb;

import org.apache.commons.lang3.function.FailableConsumer;
import org.apache.commons.lang3.function.FailableFunction;

import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;

/**
 * Transactions with strict commit semantics on YouTrackDB.
 *
 * <p>YTDBTransaction.finishTx catches and only logs exceptions thrown by commit(). Committing
 * explicitly inside the transaction scope makes any commit failure (unique index violation,
 * WAL / IO error) propagate to the caller. After a successful or failed explicit commit the
 * transaction is closed, so finishTx has nothing left to do.
 *
 * <p>Use these methods for WRITES only. Read-only code can keep using executeInTx / computeInTx.
 * Do not call them while another transaction is already open on the same thread: the explicit
 * commit would commit the outer transaction too.
 *
 * <p>Signatures mirror the engine's Failable* API, so checked exceptions (DomainListException,
 * UsersRepositoryException...) can be thrown straight out of the lambda without being wrapped in
 * RuntimeException and unwrapped through getCause().
 */
public final class YouTrackDBTransactions {

    private YouTrackDBTransactions() {
    }

    public static <X extends Exception> void executeStrictTx(YTDBGraphTraversalSource g,
                                                             FailableConsumer<YTDBGraphTraversalSource, X> action) throws X {
        g.executeInTx(tx -> {
            action.accept(tx);
            tx.tx().commit();
        });
    }

    public static <R, X extends Exception> R computeStrictTx(YTDBGraphTraversalSource g,
                                                             FailableFunction<YTDBGraphTraversalSource, R, X> action) throws X {
        return g.computeInTx(tx -> {
            R result = action.apply(tx);
            tx.tx().commit();
            return result;
        });
    }

    /** True if {@code type} appears anywhere in the cause chain of {@code t}. */
    public static boolean hasCause(Throwable t, Class<? extends Throwable> type) {
        for (Throwable current = t; current != null; current = current.getCause()) {
            if (type.isInstance(current)) {
                return true;
            }
            if (current.getCause() == current) {
                break;
            }
        }
        return false;
    }

    /**
     * Determines whether an engine exception is an optimistic conflict eligible for retry.
     * Retries are limited to concurrent record updates/creates:
     * - ConcurrentModificationException (public API: optimistic lock conflict on update)
     * - ConcurrentCreateException (internal: simultaneous record creation conflict)
     *
     * Other NeedRetryException subclasses (such as CommandInterruptedException or LinksConsistencyException)
     * are intentional cancellations or corruption errors and must not be retried.
     */
    public static boolean isRetryableConflict(Throwable t) {
        return hasCause(t, com.jetbrains.youtrackdb.api.exception.ConcurrentModificationException.class)
            || hasCause(t, com.jetbrains.youtrackdb.internal.core.exception.ConcurrentCreateException.class);
    }

    /**
     * Executes an action with automatic retry upon encountering retryable conflicts
     * (ConcurrentModificationException or ConcurrentCreateException).
     *
     * @param maxRetries maximum number of attempts
     * @param action the action to execute
     * @return result of the action
     * @throws Exception if max attempts are exhausted or a non-retryable exception occurs
     */
    public static <T> T retryOnConflict(int maxRetries, java.util.concurrent.Callable<T> action) throws Exception {
        if (maxRetries <= 0) {
            throw new IllegalArgumentException("maxRetries must be positive: " + maxRetries);
        }
        for (int attempt = 1; attempt <= maxRetries; attempt++) {
            try {
                return action.call();
            } catch (Exception e) {
                if (!isRetryableConflict(e) || attempt == maxRetries) {
                    throw e;
                }
                long sleepMs = 5L * attempt + java.util.concurrent.ThreadLocalRandom.current().nextInt(15);
                try {
                    Thread.sleep(sleepMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw ie;
                }
            }
        }
        throw new IllegalStateException("Exhausted retries without result or exception");
    }
    /** Executes a YQL query and extracts all rows as a list of Maps (DRY). */
    public static java.util.List<java.util.Map<String, Object>> queryRows(YTDBGraphTraversalSource g, String query, Object... params) {
        return g.computeInTx(tx -> {
            var list = tx.yql(query, params).toList();
            java.util.List<java.util.Map<String, Object>> rows = new java.util.ArrayList<>(list.size());
            for (Object item : list) {
                if (item instanceof java.util.Map<?, ?> m) {
                    @SuppressWarnings("unchecked")
                    java.util.Map<String, Object> casted = (java.util.Map<String, Object>) m;
                    rows.add(casted);
                } else if (item instanceof org.apache.tinkerpop.gremlin.structure.Vertex v) {
                    java.util.Map<String, Object> map = new java.util.HashMap<>();
                    v.properties().forEachRemaining(p -> map.put(p.key(), p.value()));
                    rows.add(map);
                }
            }
            return rows;
        });
    }
}
