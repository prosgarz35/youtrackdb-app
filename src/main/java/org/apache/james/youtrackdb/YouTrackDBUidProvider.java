package org.apache.james.youtrackdb;

import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import org.apache.james.mailbox.MessageUid;
import org.apache.james.mailbox.exception.MailboxException;
import org.apache.james.mailbox.model.Mailbox;
import org.apache.james.mailbox.model.MailboxId;
import org.apache.james.mailbox.store.mail.UidProvider;

import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;

@Singleton
public class YouTrackDBUidProvider implements UidProvider {

    private static final String PROP_LAST_UID = "lastUid";

    private final YTDBGraphTraversalSource g;

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
        long next = YouTrackDBMailboxCounters.increment(g, mailboxId, PROP_LAST_UID);
        return MessageUid.of(next);
    }

    @Override
    public Optional<MessageUid> lastUid(Mailbox mailbox) throws MailboxException {
        OptionalLong val = YouTrackDBMailboxCounters.read(g, mailbox.getMailboxId(), PROP_LAST_UID);
        if (val.isPresent() && val.getAsLong() > 0L) {
            return Optional.of(MessageUid.of(val.getAsLong()));
        }
        return Optional.empty();
    }
}
