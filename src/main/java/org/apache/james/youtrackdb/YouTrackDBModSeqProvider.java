package org.apache.james.youtrackdb;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import org.apache.james.mailbox.ModSeq;
import org.apache.james.mailbox.exception.MailboxException;
import org.apache.james.mailbox.exception.MailboxNotFoundException;
import org.apache.james.mailbox.model.Mailbox;
import org.apache.james.mailbox.model.MailboxId;
import org.apache.james.mailbox.store.mail.ModSeqProvider;

import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;

@Singleton
public class YouTrackDBModSeqProvider implements ModSeqProvider {

    private final YTDBGraphTraversalSource g;

    private final java.util.concurrent.ConcurrentHashMap<String, Object> mailboxLocks = new java.util.concurrent.ConcurrentHashMap<>();

    @Inject
    public YouTrackDBModSeqProvider(YTDBGraphTraversalSource g) {
        this.g = Objects.requireNonNull(g, "g must not be null");
    }

    @Override
    public ModSeq nextModSeq(Mailbox mailbox) throws MailboxException {
        return nextModSeq(mailbox.getMailboxId());
    }

    @Override
    public ModSeq nextModSeq(MailboxId mailboxId) throws MailboxException {
        Object lock = mailboxLocks.computeIfAbsent(mailboxId.serialize(), k -> new Object());
        synchronized (lock) {
            int maxRetries = 10;
            for (int attempt = 1; attempt <= maxRetries; attempt++) {
                try {
                    return YouTrackDBTransactions.computeStrictTx(g, tx -> {
                    List<Map<String, Object>> rows = YouTrackDBTransactions.queryRows(tx,
                        "SELECT highestModSeq FROM JamesMailbox WHERE mailboxId = :id",
                        "id", mailboxId.serialize());
                    if (rows.isEmpty()) {
                        throw new MailboxNotFoundException(mailboxId);
                    }

                    Object currentVal = rows.get(0).get("highestModSeq");
                    long current = currentVal instanceof Number ? ((Number) currentVal).longValue() : 0L;
                    long next = current + 1L;

                    tx.command("UPDATE JamesMailbox SET highestModSeq = :next WHERE mailboxId = :id",
                        "next", next,
                        "id", mailboxId.serialize());

                    return ModSeq.of(next);
                });
            } catch (Exception e) {
                if (e instanceof MailboxNotFoundException) {
                    throw (MailboxNotFoundException) e;
                }
                if (attempt == maxRetries) {
                    if (e instanceof MailboxException) {
                        throw (MailboxException) e;
                    }
                    throw new MailboxException("Failed to allocate next MODSEQ for mailbox " + mailboxId.serialize(), e);
                }
                    try {
                        long sleepMs = 5L * attempt + java.util.concurrent.ThreadLocalRandom.current().nextInt(15);
                        Thread.sleep(sleepMs);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        throw new MailboxException("Interrupted while allocating MODSEQ", ie);
                    }
                }
            }
            throw new MailboxException("Failed to allocate next MODSEQ after retries for mailbox " + mailboxId.serialize());
        }
    }

    @Override
    public ModSeq highestModSeq(Mailbox mailbox) throws MailboxException {
        return highestModSeq(mailbox.getMailboxId());
    }

    @Override
    public ModSeq highestModSeq(MailboxId mailboxId) throws MailboxException {
        try {
            List<Map<String, Object>> rows = YouTrackDBTransactions.queryRows(g,
                "SELECT highestModSeq FROM JamesMailbox WHERE mailboxId = :id",
                "id", mailboxId.serialize());
            if (rows.isEmpty()) {
                throw new MailboxNotFoundException(mailboxId);
            }

            Object currentVal = rows.get(0).get("highestModSeq");
            long current = currentVal instanceof Number ? ((Number) currentVal).longValue() : 0L;
            return ModSeq.of(current);
        } catch (Exception e) {
            if (e instanceof MailboxException) {
                throw (MailboxException) e;
            }
            throw new MailboxException("Failed to read highest MODSEQ for mailbox " + mailboxId.serialize(), e);
        }
    }
}
