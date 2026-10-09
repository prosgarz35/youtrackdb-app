package org.apache.james.youtrackdb;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;

import org.apache.james.mailbox.exception.MailboxException;
import org.apache.james.mailbox.exception.MailboxNotFoundException;
import org.apache.james.mailbox.model.MailboxId;

import com.google.common.util.concurrent.Striped;
import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;

/**
 * Shared thread-safe counter operations for mailbox UID and MODSEQ counters.
 * Addresses M1 (queryRowsInTx inside computeStrictTx), M2 (bounded Striped locks instead of leaking ConcurrentHashMap),
 * and M3 (DRY consolidation for counters).
 */
public final class YouTrackDBMailboxCounters {

    private static final int STRIPES = 256;
    private static final Striped<java.util.concurrent.locks.Lock> STRIPED_LOCKS = Striped.lazyWeakLock(STRIPES);

    private YouTrackDBMailboxCounters() {
    }

    public static long increment(YTDBGraphTraversalSource g, MailboxId mailboxId, String propertyName) throws MailboxException {
        Objects.requireNonNull(g, "g must not be null");
        Objects.requireNonNull(mailboxId, "mailboxId must not be null");
        Objects.requireNonNull(propertyName, "propertyName must not be null");

        java.util.concurrent.locks.Lock lock = STRIPED_LOCKS.get(mailboxId.serialize());
        lock.lock();
        try {
            return YouTrackDBTransactions.retryOnConflict(() ->
                YouTrackDBTransactions.computeStrictTx(g, tx -> {
                    List<Map<String, Object>> rows = YouTrackDBTransactions.queryRowsInTx(tx,
                        "SELECT " + propertyName + " FROM JamesMailbox WHERE mailboxId = :id",
                        "id", mailboxId.serialize());
                    if (rows.isEmpty()) {
                        throw new MailboxNotFoundException(mailboxId);
                    }

                    Object currentVal = rows.getFirst().get(propertyName);
                    long current = currentVal instanceof Number ? ((Number) currentVal).longValue() : 0L;
                    long next = current + 1L;

                    tx.command("UPDATE JamesMailbox SET " + propertyName + " = :next WHERE mailboxId = :id",
                        "next", next,
                        "id", mailboxId.serialize());

                    return next;
                })
            );
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw new MailboxException("Interrupted while allocating " + propertyName, ie);
        } catch (MailboxException me) {
            throw me;
        } catch (Exception e) {
            throw new MailboxException("Failed to allocate next " + propertyName + " for mailbox " + mailboxId.serialize(), e);
        } finally {
            lock.unlock();
        }
    }

    public static OptionalLong read(YTDBGraphTraversalSource g, MailboxId mailboxId, String propertyName) throws MailboxException {
        Objects.requireNonNull(g, "g must not be null");
        Objects.requireNonNull(mailboxId, "mailboxId must not be null");
        Objects.requireNonNull(propertyName, "propertyName must not be null");

        try {
            List<Map<String, Object>> rows = YouTrackDBTransactions.queryRows(g,
                "SELECT " + propertyName + " FROM JamesMailbox WHERE mailboxId = :id",
                "id", mailboxId.serialize());
            if (rows.isEmpty()) {
                throw new MailboxNotFoundException(mailboxId);
            }

            Object currentVal = rows.getFirst().get(propertyName);
            if (currentVal instanceof Number num) {
                return OptionalLong.of(num.longValue());
            }
            return OptionalLong.empty();
        } catch (Exception e) {
            if (e instanceof MailboxException me) {
                throw me;
            }
            throw new MailboxException("Failed to read " + propertyName + " for mailbox " + mailboxId.serialize(), e);
        }
    }
}
