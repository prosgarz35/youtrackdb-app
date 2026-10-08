package org.apache.james.youtrackdb;

import java.time.Clock;
import java.util.EnumSet;

import jakarta.inject.Inject;

import org.apache.james.events.EventBus;
import org.apache.james.mailbox.MailboxPathLocker;
import org.apache.james.mailbox.MailboxSession;
import org.apache.james.mailbox.SessionProvider;
import org.apache.james.mailbox.model.Mailbox;
import org.apache.james.mailbox.model.MessageId;
import org.apache.james.mailbox.store.MailboxManagerConfiguration;
import org.apache.james.mailbox.store.MailboxSessionMapperFactory;
import org.apache.james.mailbox.store.PreDeletionHooks;
import org.apache.james.mailbox.store.StoreMailboxAnnotationManager;
import org.apache.james.mailbox.store.StoreMailboxManager;
import org.apache.james.mailbox.store.StoreMessageManager;
import org.apache.james.mailbox.store.StoreRightManager;
import org.apache.james.mailbox.store.mail.ThreadIdGuessingAlgorithm;
import org.apache.james.mailbox.store.mail.model.impl.MessageParser;
import org.apache.james.mailbox.store.quota.QuotaComponents;
import org.apache.james.mailbox.store.search.MessageSearchIndex;

public class YouTrackDBMailboxManager extends StoreMailboxManager {

    public static final EnumSet<MailboxCapabilities> MAILBOX_CAPABILITIES = EnumSet.of(
        MailboxCapabilities.Move,
        MailboxCapabilities.UserFlag,
        MailboxCapabilities.Namespace,
        MailboxCapabilities.Annotation,
        MailboxCapabilities.ACL,
        MailboxCapabilities.Quota);

    public static final EnumSet<MessageCapabilities> MESSAGE_CAPABILITIES = EnumSet.of(
        MessageCapabilities.UniqueID);

    @Inject
    public YouTrackDBMailboxManager(MailboxSessionMapperFactory mailboxSessionMapperFactory,
                                   SessionProvider sessionProvider,
                                   MailboxPathLocker locker,
                                   MessageParser messageParser,
                                   MessageId.Factory messageIdFactory,
                                   EventBus eventBus,
                                   StoreMailboxAnnotationManager annotationManager,
                                   StoreRightManager storeRightManager,
                                   QuotaComponents quotaComponents,
                                   MessageSearchIndex searchIndex,
                                   MailboxManagerConfiguration configuration,
                                   PreDeletionHooks preDeletionHooks,
                                   ThreadIdGuessingAlgorithm threadIdGuessingAlgorithm,
                                   Clock clock) {
        super(mailboxSessionMapperFactory, sessionProvider, locker, messageParser, messageIdFactory,
            annotationManager, eventBus, storeRightManager, quotaComponents, searchIndex,
            configuration, preDeletionHooks, threadIdGuessingAlgorithm, clock);
    }

    @Override
    public EnumSet<MailboxCapabilities> getSupportedMailboxCapabilities() {
        return MAILBOX_CAPABILITIES;
    }

    @Override
    public EnumSet<MessageCapabilities> getSupportedMessageCapabilities() {
        return MESSAGE_CAPABILITIES;
    }

    @Override
    protected StoreMessageManager createMessageManager(Mailbox mailbox, MailboxSession mailboxSession) {
        return new YouTrackDBMessageManager(getMapperFactory(),
            getMessageSearchIndex(),
            getEventBus(),
            getLocker(),
            mailbox,
            getQuotaComponents().getQuotaManager(),
            getQuotaComponents().getQuotaRootResolver(),
            getMessageIdFactory(),
            configuration.getBatchSizes(),
            getStoreRightManager(),
            getPreDeletionHooks(),
            getThreadIdGuessingAlgorithm(),
            getClock());
    }
}
