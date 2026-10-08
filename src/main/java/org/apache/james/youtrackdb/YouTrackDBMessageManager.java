package org.apache.james.youtrackdb;

import java.time.Clock;

import jakarta.mail.Flags;
import jakarta.mail.Flags.Flag;

import org.apache.james.events.EventBus;
import org.apache.james.mailbox.MailboxPathLocker;
import org.apache.james.mailbox.MailboxSession;
import org.apache.james.mailbox.model.Mailbox;
import org.apache.james.mailbox.model.MessageId;
import org.apache.james.mailbox.quota.QuotaManager;
import org.apache.james.mailbox.quota.QuotaRootResolver;
import org.apache.james.mailbox.store.BatchSizes;
import org.apache.james.mailbox.store.MailboxSessionMapperFactory;
import org.apache.james.mailbox.store.MessageFactory;
import org.apache.james.mailbox.store.MessageStorer;
import org.apache.james.mailbox.store.PreDeletionHooks;
import org.apache.james.mailbox.store.StoreMessageManager;
import org.apache.james.mailbox.store.StoreRightManager;
import org.apache.james.mailbox.store.mail.ThreadIdGuessingAlgorithm;
import org.apache.james.mailbox.store.search.MessageSearchIndex;

public class YouTrackDBMessageManager extends StoreMessageManager {

    public YouTrackDBMessageManager(MailboxSessionMapperFactory mapperFactory,
                                   MessageSearchIndex index,
                                   EventBus eventBus,
                                   MailboxPathLocker locker,
                                   Mailbox mailbox,
                                   QuotaManager quotaManager,
                                   QuotaRootResolver quotaRootResolver,
                                   MessageId.Factory messageIdFactory,
                                   BatchSizes batchSizes,
                                   StoreRightManager storeRightManager,
                                   PreDeletionHooks preDeletionHooks,
                                   ThreadIdGuessingAlgorithm threadIdGuessingAlgorithm,
                                   Clock clock) {
        super(YouTrackDBMailboxManager.MESSAGE_CAPABILITIES,
            mapperFactory,
            index,
            eventBus,
            locker,
            mailbox,
            quotaManager,
            quotaRootResolver,
            batchSizes,
            storeRightManager,
            preDeletionHooks,
            new MessageStorer.WithoutAttachment(mapperFactory, messageIdFactory,
                new MessageFactory.StoreMessageFactory(), threadIdGuessingAlgorithm, clock));
    }

    @Override
    public Flags getPermanentFlags(MailboxSession mailboxSession) {
        Flags flags = new Flags(super.getPermanentFlags(mailboxSession));
        flags.add(Flag.USER);
        return flags;
    }
}
