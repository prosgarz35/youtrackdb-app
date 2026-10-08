package org.apache.james.youtrackdb;

import java.util.Objects;
import java.util.OptionalLong;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import org.apache.james.mailbox.ModSeq;
import org.apache.james.mailbox.exception.MailboxException;
import org.apache.james.mailbox.model.Mailbox;
import org.apache.james.mailbox.model.MailboxId;
import org.apache.james.mailbox.store.mail.ModSeqProvider;

import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;

@Singleton
public class YouTrackDBModSeqProvider implements ModSeqProvider {

    private static final String PROP_HIGHEST_MOD_SEQ = "highestModSeq";

    private final YTDBGraphTraversalSource g;

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
        long next = YouTrackDBMailboxCounters.increment(g, mailboxId, PROP_HIGHEST_MOD_SEQ);
        return ModSeq.of(next);
    }

    @Override
    public ModSeq highestModSeq(Mailbox mailbox) throws MailboxException {
        return highestModSeq(mailbox.getMailboxId());
    }

    @Override
    public ModSeq highestModSeq(MailboxId mailboxId) throws MailboxException {
        OptionalLong val = YouTrackDBMailboxCounters.read(g, mailboxId, PROP_HIGHEST_MOD_SEQ);
        return ModSeq.of(val.orElse(0L));
    }
}
