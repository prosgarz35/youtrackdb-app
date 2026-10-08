package org.apache.james.youtrackdb;

import java.time.Clock;
import java.util.Objects;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import org.apache.james.mailbox.MailboxSession;
import org.apache.james.mailbox.inmemory.mail.InMemoryAnnotationMapper;
import org.apache.james.mailbox.inmemory.mail.InMemoryAttachmentMapper;
import org.apache.james.mailbox.inmemory.mail.InMemoryMessageIdMapper;
import org.apache.james.mailbox.store.MailboxSessionMapperFactory;
import org.apache.james.mailbox.store.mail.AnnotationMapper;
import org.apache.james.mailbox.store.mail.AttachmentMapper;
import org.apache.james.mailbox.store.mail.AttachmentMapperFactory;
import org.apache.james.mailbox.store.mail.MailboxMapper;
import org.apache.james.mailbox.store.mail.MessageIdMapper;
import org.apache.james.mailbox.store.mail.MessageMapper;
import org.apache.james.mailbox.store.mail.ModSeqProvider;
import org.apache.james.mailbox.store.mail.UidProvider;
import org.apache.james.mailbox.store.user.SubscriptionMapper;

import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;

@Singleton
public class YouTrackDBMailboxSessionMapperFactory extends MailboxSessionMapperFactory implements AttachmentMapperFactory {

    private final MailboxMapper mailboxMapper;
    private final SubscriptionMapper subscriptionMapper;
    private final AttachmentMapper attachmentMapper;
    private final AnnotationMapper annotationMapper;
    private final UidProvider uidProvider;
    private final ModSeqProvider modSeqProvider;
    private final Clock clock;
    private final YTDBGraphTraversalSource g;

    @Inject
    public YouTrackDBMailboxSessionMapperFactory(Clock clock, YTDBGraphTraversalSource g) {
        this.clock = Objects.requireNonNull(clock, "clock must not be null");
        this.g = Objects.requireNonNull(g, "g must not be null");

        this.mailboxMapper = new YouTrackDBMailboxMapper(g);
        this.subscriptionMapper = new YouTrackDBSubscriptionMapper(g);
        this.uidProvider = new YouTrackDBUidProvider(g);
        this.modSeqProvider = new YouTrackDBModSeqProvider(g);

        this.attachmentMapper = new InMemoryAttachmentMapper();
        this.annotationMapper = new YouTrackDBAnnotationMapper(g);
    }

    @Override
    public MailboxMapper createMailboxMapper(MailboxSession session) {
        return mailboxMapper;
    }

    @Override
    public MessageMapper createMessageMapper(MailboxSession session) {
        return new YouTrackDBMessageMapper(session, uidProvider, modSeqProvider, clock, g);
    }

    @Override
    public MessageIdMapper createMessageIdMapper(MailboxSession session) {
        return new YouTrackDBMessageIdMapper(mailboxMapper, createMessageMapper(session), g);
    }

    @Override
    public SubscriptionMapper createSubscriptionMapper(MailboxSession session) {
        return subscriptionMapper;
    }

    @Override
    public AttachmentMapper createAttachmentMapper(MailboxSession session) {
        return attachmentMapper;
    }

    @Override
    public AnnotationMapper createAnnotationMapper(MailboxSession session) {
        return annotationMapper;
    }

    @Override
    public UidProvider getUidProvider(MailboxSession session) {
        return uidProvider;
    }

    @Override
    public ModSeqProvider getModSeqProvider(MailboxSession session) {
        return modSeqProvider;
    }

    @Override
    public AttachmentMapper getAttachmentMapper(MailboxSession session) {
        return attachmentMapper;
    }
}
