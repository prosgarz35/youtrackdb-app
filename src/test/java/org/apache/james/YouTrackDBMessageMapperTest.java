package org.apache.james;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import jakarta.mail.Flags;
import jakarta.mail.Flags.Flag;

import org.apache.james.core.Username;
import org.apache.james.mailbox.MailboxSession;
import org.apache.james.mailbox.MessageManager.FlagsUpdateMode;
import org.apache.james.mailbox.MessageUid;
import org.apache.james.mailbox.ModSeq;
import org.apache.james.mailbox.model.ByteContent;
import org.apache.james.mailbox.model.Mailbox;
import org.apache.james.mailbox.model.MailboxId;
import org.apache.james.mailbox.model.MailboxPath;
import org.apache.james.mailbox.model.MessageMetaData;
import org.apache.james.mailbox.model.MessageRange;
import org.apache.james.mailbox.model.ThreadId;
import org.apache.james.mailbox.model.UidValidity;
import org.apache.james.mailbox.model.UpdatedFlags;
import org.apache.james.mailbox.store.FlagsUpdateCalculator;
import org.apache.james.mailbox.store.mail.MessageMapper.FetchType;
import org.apache.james.mailbox.store.mail.model.MailboxMessage;
import org.apache.james.mailbox.store.mail.model.impl.SimpleMailboxMessage;
import org.apache.james.youtrackdb.YouTrackDBMailboxMapper;
import org.apache.james.youtrackdb.YouTrackDBMessageId;
import org.apache.james.youtrackdb.YouTrackDBMessageMapper;
import org.apache.james.youtrackdb.YouTrackDBModSeqProvider;
import org.apache.james.youtrackdb.YouTrackDBTransactions;
import org.apache.james.youtrackdb.YouTrackDBUidProvider;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.google.common.collect.ImmutableList;
import com.jetbrains.youtrackdb.api.DatabaseType;
import com.jetbrains.youtrackdb.api.YouTrackDB;
import com.jetbrains.youtrackdb.api.YourTracks;
import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;

public class YouTrackDBMessageMapperTest {

    @TempDir
    Path tempDir;

    private YouTrackDB ytdb;
    private YTDBGraphTraversalSource g;
    private YouTrackDBMailboxMapper mailboxMapper;
    private YouTrackDBUidProvider uidProvider;
    private YouTrackDBModSeqProvider modSeqProvider;
    private YouTrackDBMessageMapper messageMapper;
    private Mailbox mailbox;

    @BeforeEach
    void setUp() throws Exception {
        File dbDir = tempDir.resolve("var").resolve("youtrackdb").toFile();
        dbDir.mkdirs();
        ytdb = YourTracks.instance(dbDir.getAbsolutePath());
        ytdb.createIfNotExists("james", DatabaseType.DISK, "admin", "admin", "admin");
        g = ytdb.openTraversal("james", "admin", "admin");

        YouTrackDBTransactions.executeStrictTx(g, tx -> {
            tx.command("CREATE CLASS JamesMailbox IF NOT EXISTS EXTENDS V");
            tx.command("CREATE PROPERTY JamesMailbox.mailboxId IF NOT EXISTS STRING");
            tx.command("CREATE PROPERTY JamesMailbox.namespace IF NOT EXISTS STRING");
            tx.command("CREATE PROPERTY JamesMailbox.user IF NOT EXISTS STRING");
            tx.command("CREATE PROPERTY JamesMailbox.name IF NOT EXISTS STRING");
            tx.command("CREATE PROPERTY JamesMailbox.uidValidity IF NOT EXISTS LONG");
            tx.command("CREATE PROPERTY JamesMailbox.lastUid IF NOT EXISTS LONG");
            tx.command("CREATE PROPERTY JamesMailbox.highestModSeq IF NOT EXISTS LONG");
            tx.command("CREATE PROPERTY JamesMailbox.acl IF NOT EXISTS STRING");
            tx.command("CREATE INDEX JamesMailbox.mailboxId IF NOT EXISTS UNIQUE");
            tx.command("CREATE INDEX JamesMailbox.path IF NOT EXISTS ON JamesMailbox (namespace, user, name) UNIQUE");

            tx.command("CREATE CLASS JamesMailboxMessage IF NOT EXISTS EXTENDS V");
            tx.command("CREATE PROPERTY JamesMailboxMessage.mailboxId IF NOT EXISTS STRING");
            tx.command("CREATE PROPERTY JamesMailboxMessage.messageId IF NOT EXISTS STRING");
            tx.command("CREATE PROPERTY JamesMailboxMessage.uid IF NOT EXISTS LONG");
            tx.command("CREATE PROPERTY JamesMailboxMessage.modSeq IF NOT EXISTS LONG");
            tx.command("CREATE PROPERTY JamesMailboxMessage.internalDate IF NOT EXISTS LONG");
            tx.command("CREATE PROPERTY JamesMailboxMessage.size IF NOT EXISTS LONG");
            tx.command("CREATE PROPERTY JamesMailboxMessage.bodyStartOctet IF NOT EXISTS INTEGER");
            tx.command("CREATE PROPERTY JamesMailboxMessage.flags IF NOT EXISTS EMBEDDEDSET STRING");
            tx.command("CREATE PROPERTY JamesMailboxMessage.userFlags IF NOT EXISTS EMBEDDEDSET STRING");
            tx.command("CREATE PROPERTY JamesMailboxMessage.content IF NOT EXISTS BINARY");
            tx.command("CREATE INDEX JamesMailboxMessage.mailboxAndUid IF NOT EXISTS ON JamesMailboxMessage (mailboxId, uid) UNIQUE");
            tx.command("CREATE INDEX JamesMailboxMessage.mailboxId IF NOT EXISTS NOTUNIQUE");
        });

        mailboxMapper = new YouTrackDBMailboxMapper(g);
        uidProvider = new YouTrackDBUidProvider(g);
        modSeqProvider = new YouTrackDBModSeqProvider(g);

        MailboxSession session = new MailboxSession(
            MailboxSession.SessionId.of(1L),
            Username.of("alice"),
            Optional.of(Username.of("alice")),
            new ArrayList<>(),
            '.',
            MailboxSession.SessionType.User);
        messageMapper = new YouTrackDBMessageMapper(session, uidProvider, modSeqProvider, Clock.systemUTC(), g);

        mailbox = mailboxMapper.create(MailboxPath.forUser(Username.of("alice"), "INBOX"), UidValidity.of(12345L)).block();
    }

    @AfterEach
    void tearDown() {
        if (g != null) {
            try {
                g.close();
            } catch (Exception ignored) {
            }
        }
        if (ytdb != null) {
            try {
                ytdb.close();
            } catch (Exception ignored) {
            }
        }
    }

    private SimpleMailboxMessage createMessage(String contentStr, Flags flags) {
        return createMessage(mailbox, YouTrackDBMessageId.generate(), contentStr, flags);
    }

    private SimpleMailboxMessage createMessage(Mailbox targetMailbox, YouTrackDBMessageId msgId, String contentStr, Flags flags) {
        byte[] content = contentStr.getBytes(StandardCharsets.UTF_8);
        return SimpleMailboxMessage.builder()
            .mailboxId(targetMailbox.getMailboxId())
            .messageId(msgId)
            .threadId(ThreadId.fromBaseMessageId(msgId))
            .internalDate(new Date())
            .size(content.length)
            .bodyStartOctet(0)
            .content(new ByteContent(content))
            .flags(flags)
            .build();
    }

    @Test
    void addAndRetrieveMessageShouldWork() throws Exception {
        SimpleMailboxMessage msg = createMessage("Subject: Test\r\n\r\nBody", new Flags(Flag.RECENT));

        MessageMetaData meta = messageMapper.add(mailbox, msg);
        assertThat(meta.getUid()).isEqualTo(MessageUid.of(1L));
        assertThat(meta.getModSeq()).isEqualTo(ModSeq.of(1L));

        assertThat(messageMapper.countMessagesInMailbox(mailbox)).isEqualTo(1L);

        Iterator<MailboxMessage> it = messageMapper.findInMailbox(mailbox, MessageRange.one(meta.getUid()), FetchType.FULL, 10);
        assertThat(it.hasNext()).isTrue();
        MailboxMessage retrieved = it.next();
        assertThat(retrieved.getUid()).isEqualTo(MessageUid.of(1L));
        assertThat(retrieved.getMessageId()).isEqualTo(msg.getMessageId());
        assertThat(retrieved.isRecent()).isTrue();
        assertThat(retrieved.isSeen()).isFalse();
    }

    @Test
    void updateFlagsShouldModifyFlagsAndModSeq() throws Exception {
        SimpleMailboxMessage msg = createMessage("Hello", new Flags());
        MessageMetaData meta = messageMapper.add(mailbox, msg);

        FlagsUpdateCalculator calculator = new FlagsUpdateCalculator(new Flags(Flag.SEEN), FlagsUpdateMode.ADD);
        Iterator<UpdatedFlags> updatedIt = messageMapper.updateFlags(mailbox, calculator, MessageRange.one(meta.getUid()));
        assertThat(updatedIt.hasNext()).isTrue();

        UpdatedFlags updated = updatedIt.next();
        assertThat(updated.isChanged(Flag.SEEN)).isTrue();
        assertThat(updated.getNewFlags().contains(Flag.SEEN)).isTrue();
        assertThat(updated.getModSeq()).isEqualTo(ModSeq.of(2L));

        Iterator<MailboxMessage> it = messageMapper.findInMailbox(mailbox, MessageRange.one(meta.getUid()), FetchType.FULL, 1);
        MailboxMessage reloaded = it.next();
        assertThat(reloaded.isSeen()).isTrue();
        assertThat(reloaded.getModSeq()).isEqualTo(ModSeq.of(2L));
    }

    @Test
    void deleteMessagesShouldRemoveFromStore() throws Exception {
        SimpleMailboxMessage msg1 = createMessage("Mail 1", new Flags());
        SimpleMailboxMessage msg2 = createMessage("Mail 2", new Flags());

        MessageMetaData m1 = messageMapper.add(mailbox, msg1);
        MessageMetaData m2 = messageMapper.add(mailbox, msg2);

        assertThat(messageMapper.countMessagesInMailbox(mailbox)).isEqualTo(2L);

        Map<MessageUid, MessageMetaData> deleted = messageMapper.deleteMessages(mailbox, ImmutableList.of(m1.getUid()));
        assertThat(deleted).containsKey(m1.getUid());
        assertThat(messageMapper.countMessagesInMailbox(mailbox)).isEqualTo(1L);

        Iterator<MailboxMessage> it = messageMapper.findInMailbox(mailbox, MessageRange.all(), FetchType.METADATA, 10);
        MailboxMessage remaining = it.next();
        assertThat(remaining.getUid()).isEqualTo(m2.getUid());
        assertThat(it.hasNext()).isFalse();
    }

    @Test
    void messageRangeAndCopyShouldWork() throws Exception {
        Mailbox trash = mailboxMapper.create(MailboxPath.forUser(Username.of("alice"), "Trash"), UidValidity.of(67890L)).block();

        SimpleMailboxMessage msg1 = createMessage("Mail 1", new Flags());
        SimpleMailboxMessage msg2 = createMessage("Mail 2", new Flags());
        SimpleMailboxMessage msg3 = createMessage("Mail 3", new Flags());

        messageMapper.add(mailbox, msg1);
        messageMapper.add(mailbox, msg2);
        messageMapper.add(mailbox, msg3);

        List<MailboxMessage> rangeMessages = ImmutableList.copyOf(
            messageMapper.findInMailbox(mailbox, MessageRange.range(MessageUid.of(2L), MessageUid.of(3L)), FetchType.METADATA, 10));
        assertThat(rangeMessages).hasSize(2)
            .extracting(MailboxMessage::getUid)
            .containsExactly(MessageUid.of(2L), MessageUid.of(3L));

        // Copy msg1 to trash
        MessageMetaData copiedMeta = messageMapper.copy(trash, msg1);
        assertThat(copiedMeta.getUid()).isEqualTo(MessageUid.of(1L));
        assertThat(messageMapper.countMessagesInMailbox(trash)).isEqualTo(1L);
    }

    @Test
    void metadataAndFullFetchShouldReturnValidMessages() throws Exception {
        Mailbox benchmarkMailbox = mailboxMapper.create(MailboxPath.forUser(Username.of("benchmark"), "Benchmark"), UidValidity.of(99999L)).block();
        byte[] payload20k = new byte[20 * 1024]; // 20 KB
        java.util.Arrays.fill(payload20k, (byte) 'A');

        int messageCount = 100;
        for (int i = 0; i < messageCount; i++) {
            SimpleMailboxMessage msg = SimpleMailboxMessage.builder()
                .mailboxId(benchmarkMailbox.getMailboxId())
                .messageId(YouTrackDBMessageId.generate())
                .threadId(ThreadId.fromBaseMessageId(YouTrackDBMessageId.generate()))
                .internalDate(new Date())
                .size(payload20k.length)
                .bodyStartOctet(0)
                .content(new ByteContent(payload20k))
                .flags(new Flags())
                .build();
            messageMapper.add(benchmarkMailbox, msg);
        }

        List<MailboxMessage> fullList = ImmutableList.copyOf(
            messageMapper.findInMailbox(benchmarkMailbox, MessageRange.all(), FetchType.FULL, messageCount));

        List<MailboxMessage> metaList = ImmutableList.copyOf(
            messageMapper.findInMailbox(benchmarkMailbox, MessageRange.all(), FetchType.METADATA, messageCount));

        assertThat(fullList).hasSize(messageCount);
        assertThat(metaList).hasSize(messageCount);
        assertThat(metaList.get(0).getFullContentOctets()).isEqualTo(payload20k.length);
        assertThat(fullList.get(0).getFullContent().readAllBytes()).hasSize(payload20k.length);
    }

    @Test
    void unseenAndRecentQueriesShouldBeAccurate() throws Exception {
        SimpleMailboxMessage msg1 = createMessage("Msg 1", new Flags(Flag.RECENT));
        SimpleMailboxMessage msg2 = createMessage("Msg 2", new Flags(Flag.SEEN));
        SimpleMailboxMessage msg3 = createMessage("Msg 3", new Flags(Flag.RECENT));

        messageMapper.add(mailbox, msg1);
        messageMapper.add(mailbox, msg2);
        messageMapper.add(mailbox, msg3);

        assertThat(messageMapper.countMessagesInMailbox(mailbox)).isEqualTo(3L);
        assertThat(messageMapper.countUnseenMessagesInMailbox(mailbox)).isEqualTo(2L);
        assertThat(messageMapper.findFirstUnseenMessageUid(mailbox)).isEqualTo(MessageUid.of(1L));

        List<MessageUid> recent = messageMapper.findRecentMessageUidsInMailbox(mailbox);
        assertThat(recent).containsExactly(MessageUid.of(1L), MessageUid.of(3L));

        // Mark msg1 as SEEN
        FlagsUpdateCalculator seenCalculator = new FlagsUpdateCalculator(new Flags(Flag.SEEN), FlagsUpdateMode.ADD);
        messageMapper.updateFlags(mailbox, seenCalculator, MessageRange.one(MessageUid.of(1L)));

        assertThat(messageMapper.countUnseenMessagesInMailbox(mailbox)).isEqualTo(1L);
        assertThat(messageMapper.findFirstUnseenMessageUid(mailbox)).isEqualTo(MessageUid.of(3L));
    }

    @Test
    void retrieveMessagesMarkedForDeletionShouldFilterAccurately() throws Exception {
        SimpleMailboxMessage msg1 = createMessage("Msg 1", new Flags());
        SimpleMailboxMessage msg2 = createMessage("Msg 2", new Flags(Flag.DELETED));
        SimpleMailboxMessage msg3 = createMessage("Msg 3", new Flags());
        SimpleMailboxMessage msg4 = createMessage("Msg 4", new Flags(Flag.DELETED));

        messageMapper.add(mailbox, msg1);
        messageMapper.add(mailbox, msg2);
        messageMapper.add(mailbox, msg3);
        messageMapper.add(mailbox, msg4);

        List<MessageUid> allDeleted = messageMapper.retrieveMessagesMarkedForDeletion(mailbox, MessageRange.all());
        assertThat(allDeleted).containsExactly(MessageUid.of(2L), MessageUid.of(4L));

        List<MessageUid> rangeDeleted = messageMapper.retrieveMessagesMarkedForDeletion(
            mailbox, MessageRange.range(MessageUid.of(1L), MessageUid.of(3L)));
        assertThat(rangeDeleted).containsExactly(MessageUid.of(2L));

        List<MessageUid> oneDeleted = messageMapper.retrieveMessagesMarkedForDeletion(
            mailbox, MessageRange.one(MessageUid.of(4L)));
        assertThat(oneDeleted).containsExactly(MessageUid.of(4L));

        List<MessageUid> notDeleted = messageMapper.retrieveMessagesMarkedForDeletion(
            mailbox, MessageRange.one(MessageUid.of(1L)));
        assertThat(notDeleted).isEmpty();
    }

    @Test
    void moveShouldAtomicallyTransferMessageWithoutContentLoss() throws Exception {
        Mailbox archive = mailboxMapper.create(MailboxPath.forUser(Username.of("alice"), "Archive"), UidValidity.of(54321L)).block();

        SimpleMailboxMessage msg = createMessage("Important mail to archive", new Flags(Flag.FLAGGED));
        MessageMetaData added = messageMapper.add(mailbox, msg);
        assertThat(messageMapper.countMessagesInMailbox(mailbox)).isEqualTo(1L);
        assertThat(messageMapper.countMessagesInMailbox(archive)).isEqualTo(0L);

        MessageMetaData movedMeta = messageMapper.move(archive, msg);
        assertThat(movedMeta.getUid()).isEqualTo(MessageUid.of(1L));
        assertThat(messageMapper.countMessagesInMailbox(mailbox)).isEqualTo(0L);
        assertThat(messageMapper.countMessagesInMailbox(archive)).isEqualTo(1L);

        Iterator<MailboxMessage> archiveIt = messageMapper.findInMailbox(archive, MessageRange.one(movedMeta.getUid()), FetchType.FULL, 1);
        assertThat(archiveIt.hasNext()).isTrue();
        MailboxMessage movedMsg = archiveIt.next();
        assertThat(movedMsg.isFlagged()).isTrue();
        assertThat(new String(movedMsg.getFullContent().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8))
            .contains("Important mail to archive");
    }

    @Test
    void getApplicableFlagShouldReturnDefaultFlagsWhenNoUserFlags() throws Exception {
        SimpleMailboxMessage msg = createMessage("Normal message", new Flags(Flag.SEEN));
        messageMapper.add(mailbox, msg);

        Flags applicable = messageMapper.getApplicableFlag(mailbox);
        assertThat(applicable.contains(Flag.ANSWERED)).isTrue();
        assertThat(applicable.contains(Flag.DELETED)).isTrue();
        assertThat(applicable.contains(Flag.DRAFT)).isTrue();
        assertThat(applicable.contains(Flag.FLAGGED)).isTrue();
        assertThat(applicable.contains(Flag.SEEN)).isTrue();
        assertThat(applicable.getUserFlags()).isEmpty();
    }

    @Test
    void getApplicableFlagShouldIncludeCustomUserFlags() throws Exception {
        Flags customFlags1 = new Flags();
        customFlags1.add("MyCustomFlag1");
        customFlags1.add("SharedFlag");

        Flags customFlags2 = new Flags();
        customFlags2.add("MyCustomFlag2");
        customFlags2.add("SharedFlag");

        messageMapper.add(mailbox, createMessage("Msg 1", customFlags1));
        messageMapper.add(mailbox, createMessage("Msg 2", customFlags2));

        Flags applicable = messageMapper.getApplicableFlag(mailbox);
        assertThat(applicable.getUserFlags()).containsExactlyInAnyOrder("MyCustomFlag1", "MyCustomFlag2", "SharedFlag");
        assertThat(applicable.contains(Flag.SEEN)).isTrue();
    }

    @Test
    void listAllMessageUidsShouldReturnUidsOrdered() throws Exception {
        messageMapper.add(mailbox, createMessage("Msg 1", new Flags()));
        messageMapper.add(mailbox, createMessage("Msg 2", new Flags()));
        messageMapper.add(mailbox, createMessage("Msg 3", new Flags()));

        List<MessageUid> uids = messageMapper.listAllMessageUids(mailbox).collectList().block();
        assertThat(uids).containsExactly(MessageUid.of(1L), MessageUid.of(2L), MessageUid.of(3L));
    }

    @Test
    void findFirstUnseenMessageUidShouldReturnFirstUnseenUid() throws Exception {
        messageMapper.add(mailbox, createMessage("Msg 1", new Flags(Flag.SEEN)));
        messageMapper.add(mailbox, createMessage("Msg 2", new Flags())); // unseen
        messageMapper.add(mailbox, createMessage("Msg 3", new Flags())); // unseen

        MessageUid firstUnseen = messageMapper.findFirstUnseenMessageUid(mailbox);
        assertThat(firstUnseen).isEqualTo(MessageUid.of(2L));
    }

    @Test
    void findFirstUnseenMessageUidShouldReturnNullWhenAllSeen() throws Exception {
        messageMapper.add(mailbox, createMessage("Msg 1", new Flags(Flag.SEEN)));
        messageMapper.add(mailbox, createMessage("Msg 2", new Flags(Flag.SEEN)));

        MessageUid firstUnseen = messageMapper.findFirstUnseenMessageUid(mailbox);
        assertThat(firstUnseen).isNull();
    }

    @Test
    void messageIdMapperDeleteShouldBatchDeleteAcrossMailboxes() throws Exception {
        org.apache.james.youtrackdb.YouTrackDBMessageIdMapper msgIdMapper =
            new org.apache.james.youtrackdb.YouTrackDBMessageIdMapper(mailboxMapper, messageMapper, g);

        Mailbox otherMailbox = mailboxMapper.create(MailboxPath.forUser(Username.of("alice"), "Trash"), UidValidity.of(888L)).block();

        YouTrackDBMessageId sharedMsgId = YouTrackDBMessageId.generate();
        SimpleMailboxMessage msgInInbox = createMessage(mailbox, sharedMsgId, "Shared message", new Flags());
        messageMapper.add(mailbox, msgInInbox);

        SimpleMailboxMessage msgInTrash = createMessage(otherMailbox, sharedMsgId, "Shared message", new Flags());
        messageMapper.add(otherMailbox, msgInTrash);

        assertThat(messageMapper.countMessagesInMailbox(mailbox)).isEqualTo(1L);
        assertThat(messageMapper.countMessagesInMailbox(otherMailbox)).isEqualTo(1L);

        // Delete messageId globally across all mailboxes
        msgIdMapper.delete(sharedMsgId);

        assertThat(messageMapper.countMessagesInMailbox(mailbox)).isEqualTo(0L);
        assertThat(messageMapper.countMessagesInMailbox(otherMailbox)).isEqualTo(0L);
    }

    @Test
    void messageIdMapperDeleteWithMailboxIdsShouldDeleteOnlyTargetMailboxes() throws Exception {
        org.apache.james.youtrackdb.YouTrackDBMessageIdMapper msgIdMapper =
            new org.apache.james.youtrackdb.YouTrackDBMessageIdMapper(mailboxMapper, messageMapper, g);

        Mailbox otherMailbox = mailboxMapper.create(MailboxPath.forUser(Username.of("alice"), "Important"), UidValidity.of(999L)).block();

        YouTrackDBMessageId sharedMsgId = YouTrackDBMessageId.generate();
        SimpleMailboxMessage msgInInbox = createMessage(mailbox, sharedMsgId, "Shared message", new Flags());
        messageMapper.add(mailbox, msgInInbox);

        SimpleMailboxMessage msgInImportant = createMessage(otherMailbox, sharedMsgId, "Shared message", new Flags());
        messageMapper.add(otherMailbox, msgInImportant);

        // Delete only from mailbox (Inbox), keep otherMailbox (Important)
        msgIdMapper.delete(sharedMsgId, ImmutableList.of(mailbox.getMailboxId()));

        assertThat(messageMapper.countMessagesInMailbox(mailbox)).isEqualTo(0L);
        assertThat(messageMapper.countMessagesInMailbox(otherMailbox)).isEqualTo(1L);
    }

    @Test
    void messageIdMapperSetFlagsShouldBatchUpdateFlagsAcrossTargetMailboxes() throws Exception {
        org.apache.james.youtrackdb.YouTrackDBMessageIdMapper msgIdMapper =
            new org.apache.james.youtrackdb.YouTrackDBMessageIdMapper(mailboxMapper, messageMapper, g);

        Mailbox otherMailbox = mailboxMapper.create(MailboxPath.forUser(Username.of("alice"), "Work"), UidValidity.of(777L)).block();

        YouTrackDBMessageId sharedMsgId = YouTrackDBMessageId.generate();
        SimpleMailboxMessage msgInInbox = createMessage(mailbox, sharedMsgId, "Shared message", new Flags());
        messageMapper.add(mailbox, msgInInbox);

        SimpleMailboxMessage msgInWork = createMessage(otherMailbox, sharedMsgId, "Shared message", new Flags());
        messageMapper.add(otherMailbox, msgInWork);

        // Update flags in both mailboxes via setFlags
        Flags seenFlag = new Flags(Flag.SEEN);
        com.google.common.collect.Multimap<MailboxId, UpdatedFlags> updated = msgIdMapper.setFlags(
            sharedMsgId,
            ImmutableList.of(mailbox.getMailboxId(), otherMailbox.getMailboxId()),
            seenFlag,
            org.apache.james.mailbox.MessageManager.FlagsUpdateMode.ADD
        ).block();

        assertThat(updated.keySet()).containsExactlyInAnyOrder(mailbox.getMailboxId(), otherMailbox.getMailboxId());

        // Verify messages in both mailboxes now have SEEN flag
        Iterator<MailboxMessage> inboxIt = messageMapper.findInMailbox(mailbox, MessageRange.all(), FetchType.METADATA, 1);
        assertThat(inboxIt.next().isSeen()).isTrue();

        Iterator<MailboxMessage> workIt = messageMapper.findInMailbox(otherMailbox, MessageRange.all(), FetchType.METADATA, 1);
        assertThat(workIt.next().isSeen()).isTrue();
    }
}
