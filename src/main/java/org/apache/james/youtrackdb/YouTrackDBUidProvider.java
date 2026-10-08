package org.apache.james.youtrackdb;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import org.apache.james.mailbox.MessageUid;
import org.apache.james.mailbox.exception.MailboxException;
import org.apache.james.mailbox.exception.MailboxNotFoundException;
import org.apache.james.mailbox.model.Mailbox;
import org.apache.james.mailbox.model.MailboxId;
import org.apache.james.mailbox.store.mail.UidProvider;

import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;

@Singleton
public class YouTrackDBUidProvider implements UidProvider {

    private final YTDBGraphTraversalSource g;

    private final java.util.concurrent.ConcurrentHashMap<String, Object> mailboxLocks = new java.util.concurrent.ConcurrentHashMap<>();

    @Inject
    public YouTrackDBUidProvider(YTDBGraphTraversalSource g) {
        this.g = Objects.requireNonNull(g, "g must not be null");
    }

    @Override
    public MessageUid nextUid(Mailbox mailbox) throws MailboxException {
        return nextUid(mailbox.getMailboxId());
    }

    @Override
    public MessageUid nextUid(MailboxId mailboxId) throws MailboxException {
        Object lock = mailboxLocks.computeIfAbsent(mailboxId.serialize(), k -> new Object());
        synchronized (lock) {
            int maxRetries = 10;
            for (int attempt = 1; attempt <= maxRetries; attempt++) {
                try {
                    return YouTrackDBTransactions.computeStrictTx(g, tx -> {
                    List<Map<String, Object>> rows = YouTrackDBTransactions.queryRows(tx,
                        "SELECT lastUid FROM JamesMailbox WHERE mailboxId = :id",
                        "id", mailboxId.serialize());
                    if (rows.isEmpty()) {
                        throw new MailboxNotFoundException(mailboxId);
                    }

                    Object currentVal = rows.get(0).get("lastUid");
                    long last = currentVal instanceof Number ? ((Number) currentVal).longValue() : 0L;
                    long next = last + 1L;

                    tx.command("UPDATE JamesMailbox SET lastUid = :next WHERE mailboxId = :id",
                        "next", next,
                        "id", mailboxId.serialize());

                    return MessageUid.of(next);
                });
            } catch (Exception e) {
                if (e instanceof MailboxNotFoundException) {
                    throw (MailboxNotFoundException) e;
                }
                boolean isRetryable = YouTrackDBTransactions.hasCause(e, com.jetbrains.youtrackdb.internal.common.concur.NeedRetryException.class);
                if (!isRetryable || attempt == maxRetries) {
                    if (e instanceof MailboxException) {
                        throw (MailboxException) e;
                    }
                    throw new MailboxException("Failed to allocate next UID for mailbox " + mailboxId.serialize(), e);
                }
                try {
                    long sleepMs = 5L * attempt + java.util.concurrent.ThreadLocalRandom.current().nextInt(15);
                    Thread.sleep(sleepMs);
                } catch (InterruptedException ie) {
                    Thread.currentThread().interrupt();
                    throw new MailboxException("Interrupted while allocating UID", ie);
                }
            }
        }
        throw new MailboxException("Failed to allocate next UID after retries for mailbox " + mailboxId.serialize());
    }
    }

    @Override
    public Optional<MessageUid> lastUid(Mailbox mailbox) throws MailboxException {
        try {
            List<Map<String, Object>> rows = YouTrackDBTransactions.queryRows(g,
                "SELECT lastUid FROM JamesMailbox WHERE mailboxId = :id",
                "id", mailbox.getMailboxId().serialize());
            if (rows.isEmpty()) {
                throw new MailboxNotFoundException(mailbox.getMailboxId());
            }

            Object currentVal = rows.get(0).get("lastUid");
            long last = currentVal instanceof Number ? ((Number) currentVal).longValue() : 0L;
            if (last <= 0L) {
                return Optional.empty();
            }
            return Optional.of(MessageUid.of(last));
        } catch (Exception e) {
            if (e instanceof MailboxException) {
                throw (MailboxException) e;
            }
            throw new MailboxException("Failed to read last UID for mailbox " + mailbox.getMailboxId().serialize(), e);
        }
    }
}
