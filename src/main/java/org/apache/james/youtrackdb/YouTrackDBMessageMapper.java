package org.apache.james.youtrackdb;

import java.io.InputStream;
import java.time.Clock;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import jakarta.mail.Flags;
import jakarta.mail.Flags.Flag;

import org.apache.commons.io.IOUtils;
import org.apache.james.mailbox.MailboxSession;
import org.apache.james.mailbox.MessageUid;
import org.apache.james.mailbox.ModSeq;
import org.apache.james.mailbox.exception.MailboxException;
import org.apache.james.mailbox.model.ByteContent;
import org.apache.james.mailbox.model.Mailbox;
import org.apache.james.mailbox.model.MailboxId;
import org.apache.james.mailbox.model.MessageMetaData;
import org.apache.james.mailbox.model.MessageRange;
import org.apache.james.mailbox.model.ThreadId;
import org.apache.james.mailbox.model.UpdatedFlags;
import org.apache.james.mailbox.store.FlagsUpdateCalculator;
import org.apache.james.mailbox.store.mail.AbstractMessageMapper;
import org.apache.james.mailbox.store.mail.ModSeqProvider;
import org.apache.james.mailbox.store.mail.UidProvider;
import org.apache.james.mailbox.store.mail.model.MailboxMessage;
import org.apache.james.mailbox.store.mail.model.impl.SimpleMailboxMessage;
import org.apache.james.mailbox.store.mail.utils.ApplicableFlagCalculator;

import com.google.common.collect.ImmutableList;
import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;

public class YouTrackDBMessageMapper extends AbstractMessageMapper {

    private static final String CLASS_NAME = "JamesMailboxMessage";
    private static final String PROP_MAILBOX_ID = "mailboxId";
    private static final String PROP_MESSAGE_ID = "messageId";
    private static final String PROP_THREAD_ID = "threadId";
    private static final String PROP_UID = "uid";
    private static final String PROP_MODSEQ = "modSeq";
    private static final String PROP_INTERNAL_DATE = "internalDate";
    private static final String PROP_SAVE_DATE = "saveDate";
    private static final String PROP_SIZE = "size";
    private static final String PROP_BODY_START = "bodyStartOctet";
    private static final String PROP_FLAGS = "flags";
    private static final String PROP_USER_FLAGS = "userFlags";
    private static final String PROP_CONTENT = "content";

    private final YTDBGraphTraversalSource g;
    private final UidProvider uidProvider;
    private final ModSeqProvider modSeqProvider;

    public YouTrackDBMessageMapper(MailboxSession mailboxSession,
                                   UidProvider uidProvider,
                                   ModSeqProvider modSeqProvider,
                                   Clock clock,
                                   YTDBGraphTraversalSource g) {
        super(mailboxSession, uidProvider, modSeqProvider, clock);
        this.uidProvider = uidProvider;
        this.modSeqProvider = modSeqProvider;
        this.g = Objects.requireNonNull(g, "g must not be null");
    }

    @Override
    public Iterator<MailboxMessage> findInMailbox(Mailbox mailbox, MessageRange range, FetchType type, int limit) throws MailboxException {
        try {
            String mailboxId = mailbox.getMailboxId().serialize();
            String selectClause = (type == FetchType.METADATA)
                ? "SELECT mailboxId, messageId, threadId, uid, modSeq, internalDate, saveDate, size, bodyStartOctet, flags, userFlags FROM JamesMailboxMessage"
                : "SELECT FROM JamesMailboxMessage";
            String query;
            List<Object> params = new ArrayList<>();
            params.add("mbx");
            params.add(mailboxId);

            switch (range.getType()) {
                case ONE:
                    query = selectClause + " WHERE mailboxId = :mbx AND uid = :uid";
                    params.add("uid");
                    params.add(range.getUidFrom().asLong());
                    break;
                case FROM:
                    query = selectClause + " WHERE mailboxId = :mbx AND uid >= :from ORDER BY uid ASC";
                    params.add("from");
                    params.add(range.getUidFrom().asLong());
                    break;
                case RANGE:
                    query = selectClause + " WHERE mailboxId = :mbx AND uid >= :from AND uid <= :to ORDER BY uid ASC";
                    params.add("from");
                    params.add(range.getUidFrom().asLong());
                    params.add("to");
                    params.add(range.getUidTo().asLong());
                    break;
                case ALL:
                default:
                    query = selectClause + " WHERE mailboxId = :mbx ORDER BY uid ASC";
                    break;
            }

            if (limit > 0) {
                query += " LIMIT " + limit;
            }

            List<Map<String, Object>> rows = YouTrackDBTransactions.queryRows(g, query, params.toArray());
            List<MailboxMessage> messages = new ArrayList<>(rows.size());
            for (Map<String, Object> row : rows) {
                messages.add(readMessage(row, mailbox.getMailboxId(), type));
            }
            return messages.iterator();
        } catch (Exception e) {
            throw new MailboxException("Failed to find messages in mailbox " + mailbox.getMailboxId().serialize(), e);
        }
    }

    @Override
    public List<MessageUid> retrieveMessagesMarkedForDeletion(Mailbox mailbox, MessageRange messageRange) throws MailboxException {
        Iterator<MailboxMessage> it = findInMailbox(mailbox, messageRange, FetchType.METADATA, UNLIMITED);
        List<MessageUid> result = new ArrayList<>();
        while (it.hasNext()) {
            MailboxMessage msg = it.next();
            if (msg.isDeleted()) {
                result.add(msg.getUid());
            }
        }
        return result;
    }

    @Override
    public long countMessagesInMailbox(Mailbox mailbox) throws MailboxException {
        try {
            List<Map<String, Object>> rows = YouTrackDBTransactions.queryRows(g,
                "SELECT count(*) AS cnt FROM JamesMailboxMessage WHERE mailboxId = :mbx",
                "mbx", mailbox.getMailboxId().serialize());
            if (!rows.isEmpty()) {
                Object cnt = rows.get(0).get("cnt");
                return cnt instanceof Number ? ((Number) cnt).longValue() : 0L;
            }
            return 0L;
        } catch (Exception e) {
            throw new MailboxException("Failed to count messages in mailbox " + mailbox.getMailboxId().serialize(), e);
        }
    }

    @Override
    protected long countUnseenMessagesInMailbox(Mailbox mailbox) throws MailboxException {
        Iterator<MailboxMessage> it = findInMailbox(mailbox, MessageRange.all(), FetchType.METADATA, UNLIMITED);
        long unseen = 0;
        while (it.hasNext()) {
            if (!it.next().isSeen()) {
                unseen++;
            }
        }
        return unseen;
    }

    @Override
    public void delete(Mailbox mailbox, MailboxMessage message) throws MailboxException {
        deleteMessages(mailbox, ImmutableList.of(message.getUid()));
    }

    @Override
    public Map<MessageUid, MessageMetaData> deleteMessages(Mailbox mailbox, List<MessageUid> uids) throws MailboxException {
        try {
            Map<MessageUid, MessageMetaData> result = new HashMap<>();
            for (MessageUid uid : uids) {
                Iterator<MailboxMessage> it = findInMailbox(mailbox, MessageRange.one(uid), FetchType.METADATA, 1);
                if (it.hasNext()) {
                    MailboxMessage msg = it.next();
                    result.put(uid, msg.metaData());
                }
            }

            YouTrackDBTransactions.executeStrictTx(g, tx -> {
                for (MessageUid uid : uids) {
                    tx.command("DELETE VERTEX JamesMailboxMessage WHERE mailboxId = :mbx AND uid = :uid",
                        "mbx", mailbox.getMailboxId().serialize(),
                        "uid", uid.asLong());
                }
            });

            return result;
        } catch (Exception e) {
            throw new MailboxException("Failed to delete messages in mailbox " + mailbox.getMailboxId().serialize(), e);
        }
    }

    @Override
    public MessageUid findFirstUnseenMessageUid(Mailbox mailbox) throws MailboxException {
        Iterator<MailboxMessage> it = findInMailbox(mailbox, MessageRange.all(), FetchType.METADATA, UNLIMITED);
        while (it.hasNext()) {
            MailboxMessage msg = it.next();
            if (!msg.isSeen()) {
                return msg.getUid();
            }
        }
        return null;
    }

    @Override
    public List<MessageUid> findRecentMessageUidsInMailbox(Mailbox mailbox) throws MailboxException {
        Iterator<MailboxMessage> it = findInMailbox(mailbox, MessageRange.all(), FetchType.METADATA, UNLIMITED);
        List<MessageUid> recent = new ArrayList<>();
        while (it.hasNext()) {
            MailboxMessage msg = it.next();
            if (msg.isRecent()) {
                recent.add(msg.getUid());
            }
        }
        return recent;
    }

    @Override
    public Flags getApplicableFlag(Mailbox mailbox) throws MailboxException {
        List<MailboxMessage> messages = ImmutableList.copyOf(
            findInMailbox(mailbox, MessageRange.all(), FetchType.METADATA, UNLIMITED));
        return new ApplicableFlagCalculator(messages).computeApplicableFlags();
    }

    @Override
    public Iterator<UpdatedFlags> updateFlags(Mailbox mailbox, FlagsUpdateCalculator flagsUpdateCalculator, MessageRange set) throws MailboxException {
        List<UpdatedFlags> updatedFlags = new ArrayList<>();
        Iterator<MailboxMessage> messages = findInMailbox(mailbox, set, FetchType.METADATA, UNLIMITED);

        if (!messages.hasNext()) {
            return Collections.emptyIterator();
        }
        ModSeq modSeq = modSeqProvider.nextModSeq(mailbox);
        while (messages.hasNext()) {
            MailboxMessage member = messages.next();
            Flags originalFlags = member.createFlags();
            member.setFlags(flagsUpdateCalculator.buildNewFlags(originalFlags));
            Flags newFlags = member.createFlags();
            if (UpdatedFlags.flagsChanged(originalFlags, newFlags)) {
                member.setModSeq(modSeq);
                Set<String> systemFlags = extractSystemFlags(newFlags);
                Set<String> userFlags = extractUserFlags(newFlags);
                try {
                    YouTrackDBTransactions.executeStrictTx(g, tx -> {
                        long currentModSeq = member.getModSeq().asLong();
                        tx.command("UPDATE JamesMailboxMessage SET flags = :flags, userFlags = :userFlags, modSeq = :newModSeq WHERE mailboxId = :mbx AND uid = :uid AND (modSeq = :currModSeq OR modSeq < :newModSeq)",
                            "flags", systemFlags,
                            "userFlags", userFlags,
                            "newModSeq", modSeq.asLong(),
                            "mbx", mailbox.getMailboxId().serialize(),
                            "uid", member.getUid().asLong(),
                            "currModSeq", currentModSeq);
                    });
                } catch (Exception e) {
                    throw new MailboxException("Failed to update flags for message " + member.getUid(), e);
                }
            }

            updatedFlags.add(UpdatedFlags.builder()
                .uid(member.getUid())
                .messageId(member.getMessageId())
                .internalDate(member.getInternalDate())
                .modSeq(member.getModSeq())
                .newFlags(newFlags)
                .oldFlags(originalFlags)
                .build());
        }

        return updatedFlags.iterator();
    }

    @Override
    protected MessageMetaData save(Mailbox mailbox, MailboxMessage message) throws MailboxException {
        try {
            byte[] fullBytes;
            try (InputStream is = message.getFullContent()) {
                fullBytes = IOUtils.toByteArray(is);
            }

            Set<String> systemFlags = extractSystemFlags(message.createFlags());
            Set<String> userFlags = extractUserFlags(message.createFlags());

            int bodyStart = (int) message.getHeaderOctets();
            long saveDateMs = message.getSaveDate().map(Date::getTime).orElseGet(() -> clock.instant().toEpochMilli());
            String threadId = message.getThreadId() != null && message.getThreadId().getBaseMessageId() != null
                ? message.getThreadId().getBaseMessageId().serialize()
                : message.getMessageId().serialize();

            YouTrackDBTransactions.executeStrictTx(g, tx -> {
                tx.addV(CLASS_NAME)
                    .property(PROP_MAILBOX_ID, mailbox.getMailboxId().serialize())
                    .property(PROP_MESSAGE_ID, message.getMessageId().serialize())
                    .property(PROP_THREAD_ID, threadId)
                    .property(PROP_UID, message.getUid().asLong())
                    .property(PROP_MODSEQ, message.getModSeq().asLong())
                    .property(PROP_INTERNAL_DATE, message.getInternalDate().getTime())
                    .property(PROP_SAVE_DATE, saveDateMs)
                    .property(PROP_SIZE, message.getFullContentOctets())
                    .property(PROP_BODY_START, bodyStart)
                    .property(PROP_FLAGS, systemFlags)
                    .property(PROP_USER_FLAGS, userFlags)
                    .property(PROP_CONTENT, fullBytes)
                    .iterate();
            });

            return message.metaData();
        } catch (Exception e) {
            throw new MailboxException("Failed to save message in mailbox " + mailbox.getMailboxId().serialize(), e);
        }
    }

    @Override
    protected MessageMetaData copy(Mailbox mailbox, MessageUid uid, ModSeq modSeq, MailboxMessage original) throws MailboxException {
        SimpleMailboxMessage copy = SimpleMailboxMessage.copy(mailbox.getMailboxId(), original);
        copy.setUid(uid);
        copy.setModSeq(modSeq);
        return save(mailbox, copy);
    }

    @Override
    public MessageMetaData move(Mailbox mailbox, MailboxMessage original) throws MailboxException {
        try {
            MessageUid newUid = uidProvider.nextUid(mailbox);
            ModSeq newModSeq = modSeqProvider.nextModSeq(mailbox);
            SimpleMailboxMessage copy = SimpleMailboxMessage.copy(mailbox.getMailboxId(), original);
            copy.setUid(newUid);
            copy.setModSeq(newModSeq);

            byte[] fullBytes;
            try (InputStream is = copy.getFullContent()) {
                fullBytes = IOUtils.toByteArray(is);
            }

            Set<String> systemFlags = extractSystemFlags(copy.createFlags());
            Set<String> userFlags = extractUserFlags(copy.createFlags());
            int bodyStart = (int) copy.getHeaderOctets();
            long saveDateMs = clock.instant().toEpochMilli();
            String threadId = original.getThreadId() != null && original.getThreadId().getBaseMessageId() != null
                ? original.getThreadId().getBaseMessageId().serialize()
                : original.getMessageId().serialize();

            YouTrackDBTransactions.executeStrictTx(g, tx -> {
                tx.addV(CLASS_NAME)
                    .property(PROP_MAILBOX_ID, mailbox.getMailboxId().serialize())
                    .property(PROP_MESSAGE_ID, copy.getMessageId().serialize())
                    .property(PROP_THREAD_ID, threadId)
                    .property(PROP_UID, copy.getUid().asLong())
                    .property(PROP_MODSEQ, copy.getModSeq().asLong())
                    .property(PROP_INTERNAL_DATE, copy.getInternalDate().getTime())
                    .property(PROP_SAVE_DATE, saveDateMs)
                    .property(PROP_SIZE, copy.getFullContentOctets())
                    .property(PROP_BODY_START, bodyStart)
                    .property(PROP_FLAGS, systemFlags)
                    .property(PROP_USER_FLAGS, userFlags)
                    .property(PROP_CONTENT, fullBytes)
                    .iterate();

                tx.command("DELETE VERTEX JamesMailboxMessage WHERE mailboxId = :mbx AND uid = :uid",
                    "mbx", original.getMailboxId().serialize(),
                    "uid", original.getUid().asLong());
            });

            return copy.metaData();
        } catch (Exception e) {
            throw new MailboxException("Failed to move message " + original.getUid() + " to mailbox " + mailbox.getMailboxId().serialize(), e);
        }
    }

    private void delete(MailboxId mailboxId, MailboxMessage message) throws MailboxException {
        try {
            YouTrackDBTransactions.executeStrictTx(g, tx ->
                tx.command("DELETE VERTEX JamesMailboxMessage WHERE mailboxId = :mbx AND uid = :uid",
                    "mbx", mailboxId.serialize(),
                    "uid", message.getUid().asLong()));
        } catch (Exception e) {
            throw new MailboxException("Failed to delete message " + message.getUid(), e);
        }
    }

    @Override
    protected void begin() {
    }

    @Override
    protected void commit() {
    }

    @Override
    protected void rollback() {
    }

    static MailboxMessage readMessage(Map<String, Object> row, MailboxId mailboxId, FetchType type) {
        String messageIdStr = Objects.toString(row.get(PROP_MESSAGE_ID), null);
        String threadIdStr = Objects.toString(row.get(PROP_THREAD_ID), messageIdStr);
        long uid = ((Number) row.get(PROP_UID)).longValue();
        long modSeq = ((Number) row.get(PROP_MODSEQ)).longValue();
        long internalDateMs = ((Number) row.get(PROP_INTERNAL_DATE)).longValue();
        Object saveDateObj = row.get(PROP_SAVE_DATE);
        Optional<Date> saveDate = saveDateObj instanceof Number num ? Optional.of(new Date(num.longValue())) : Optional.empty();
        long size = ((Number) row.get(PROP_SIZE)).longValue();
        int bodyStart = ((Number) row.get(PROP_BODY_START)).intValue();
        byte[] content = (type == FetchType.METADATA) ? new byte[0] : (byte[]) row.get(PROP_CONTENT);

        Flags flags = readFlags(row);

        SimpleMailboxMessage.Builder builder = SimpleMailboxMessage.builder()
            .mailboxId(mailboxId)
            .messageId(YouTrackDBMessageId.of(messageIdStr))
            .threadId(ThreadId.fromBaseMessageId(YouTrackDBMessageId.of(threadIdStr)))
            .uid(MessageUid.of(uid))
            .modseq(ModSeq.of(modSeq))
            .internalDate(new Date(internalDateMs))
            .size(size)
            .bodyStartOctet(bodyStart)
            .content(new ByteContent(content != null ? content : new byte[0]))
            .flags(flags);

        saveDate.ifPresent(builder::saveDate);

        return builder.build();
    }

    private static Set<String> extractSystemFlags(Flags flags) {
        Set<String> set = new HashSet<>();
        if (flags.contains(Flag.ANSWERED)) {
            set.add("ANSWERED");
        }
        if (flags.contains(Flag.DELETED)) {
            set.add("DELETED");
        }
        if (flags.contains(Flag.DRAFT)) {
            set.add("DRAFT");
        }
        if (flags.contains(Flag.FLAGGED)) {
            set.add("FLAGGED");
        }
        if (flags.contains(Flag.RECENT)) {
            set.add("RECENT");
        }
        if (flags.contains(Flag.SEEN)) {
            set.add("SEEN");
        }
        return set;
    }

    private static Set<String> extractUserFlags(Flags flags) {
        Set<String> set = new HashSet<>();
        for (String userFlag : flags.getUserFlags()) {
            set.add(userFlag);
        }
        return set;
    }

    private static Flags readFlags(Map<String, Object> row) {
        Flags flags = new Flags();
        Object sysObj = row.get(PROP_FLAGS);
        if (sysObj instanceof Iterable<?> it) {
            for (Object f : it) {
                if ("ANSWERED".equals(f)) {
                    flags.add(Flag.ANSWERED);
                } else if ("DELETED".equals(f)) {
                    flags.add(Flag.DELETED);
                } else if ("DRAFT".equals(f)) {
                    flags.add(Flag.DRAFT);
                } else if ("FLAGGED".equals(f)) {
                    flags.add(Flag.FLAGGED);
                } else if ("RECENT".equals(f)) {
                    flags.add(Flag.RECENT);
                } else if ("SEEN".equals(f)) {
                    flags.add(Flag.SEEN);
                }
            }
        }
        Object userObj = row.get(PROP_USER_FLAGS);
        if (userObj instanceof Iterable<?> it) {
            for (Object f : it) {
                flags.add(Objects.toString(f));
            }
        }
        return flags;
    }
}
