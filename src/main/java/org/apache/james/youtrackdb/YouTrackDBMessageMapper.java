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
            String metadataCols = String.join(", ",
                PROP_MAILBOX_ID, PROP_MESSAGE_ID, PROP_THREAD_ID, PROP_UID, PROP_MODSEQ,
                PROP_INTERNAL_DATE, PROP_SAVE_DATE, PROP_SIZE, PROP_BODY_START, PROP_FLAGS, PROP_USER_FLAGS);
            String selectClause = (type == FetchType.METADATA)
                ? "SELECT " + metadataCols + " FROM " + CLASS_NAME
                : "SELECT FROM " + CLASS_NAME;
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
                query += " LIMIT :limit";
                params.add("limit");
                params.add(limit);
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
        try {
            String mailboxId = mailbox.getMailboxId().serialize();
            String query;
            List<Object> params = new ArrayList<>();
            params.add("mbx");
            params.add(mailboxId);

            switch (messageRange.getType()) {
                case ONE:
                    query = "SELECT uid FROM " + CLASS_NAME + " WHERE mailboxId = :mbx AND uid = :uid AND flags CONTAINS 'DELETED'";
                    params.add("uid");
                    params.add(messageRange.getUidFrom().asLong());
                    break;
                case FROM:
                    query = "SELECT uid FROM " + CLASS_NAME + " WHERE mailboxId = :mbx AND uid >= :from AND flags CONTAINS 'DELETED' ORDER BY uid ASC";
                    params.add("from");
                    params.add(messageRange.getUidFrom().asLong());
                    break;
                case RANGE:
                    query = "SELECT uid FROM " + CLASS_NAME + " WHERE mailboxId = :mbx AND uid >= :from AND uid <= :to AND flags CONTAINS 'DELETED' ORDER BY uid ASC";
                    params.add("from");
                    params.add(messageRange.getUidFrom().asLong());
                    params.add("to");
                    params.add(messageRange.getUidTo().asLong());
                    break;
                case ALL:
                default:
                    query = "SELECT uid FROM " + CLASS_NAME + " WHERE mailboxId = :mbx AND flags CONTAINS 'DELETED' ORDER BY uid ASC";
                    break;
            }

            List<Map<String, Object>> rows = YouTrackDBTransactions.queryRows(g, query, params.toArray());
            List<MessageUid> result = new ArrayList<>(rows.size());
            for (Map<String, Object> row : rows) {
                if (row.get(PROP_UID) instanceof Number n) {
                    result.add(MessageUid.of(n.longValue()));
                }
            }
            return result;
        } catch (Exception e) {
            throw new MailboxException("Failed to retrieve messages marked for deletion in mailbox " + mailbox.getMailboxId().serialize(), e);
        }
    }

    @Override
    public long countMessagesInMailbox(Mailbox mailbox) throws MailboxException {
        try {
            List<Map<String, Object>> rows = YouTrackDBTransactions.queryRows(g,
                "SELECT count(*) AS cnt FROM JamesMailboxMessage WHERE mailboxId = :mbx",
                "mbx", mailbox.getMailboxId().serialize());
            if (!rows.isEmpty()) {
                Object cnt = rows.getFirst().get("cnt");
                return cnt instanceof Number ? ((Number) cnt).longValue() : 0L;
            }
            return 0L;
        } catch (Exception e) {
            throw new MailboxException("Failed to count messages in mailbox " + mailbox.getMailboxId().serialize(), e);
        }
    }

    @Override
    public long countUnseenMessagesInMailbox(Mailbox mailbox) throws MailboxException {
        try {
            List<Map<String, Object>> rows = YouTrackDBTransactions.queryRows(g,
                "SELECT count(*) AS cnt FROM JamesMailboxMessage WHERE mailboxId = :mbx AND NOT (flags CONTAINS 'SEEN')",
                "mbx", mailbox.getMailboxId().serialize());
            if (!rows.isEmpty() && rows.getFirst().get("cnt") instanceof Number n) {
                return n.longValue();
            }
            return 0L;
        } catch (Exception e) {
            throw new MailboxException("Failed to count unseen messages in mailbox " + mailbox.getMailboxId().serialize(), e);
        }
    }

    @Override
    public void delete(Mailbox mailbox, MailboxMessage message) throws MailboxException {
        deleteMessages(mailbox, ImmutableList.of(message.getUid()));
    }

    @Override
    public Map<MessageUid, MessageMetaData> deleteMessages(Mailbox mailbox, List<MessageUid> uids) throws MailboxException {
        if (uids == null || uids.isEmpty()) {
            return Collections.emptyMap();
        }
        try {
            String mailboxId = mailbox.getMailboxId().serialize();
            List<Long> uidLongs = uids.stream().map(MessageUid::asLong).toList();

            String metadataCols = String.join(", ",
                PROP_MAILBOX_ID, PROP_MESSAGE_ID, PROP_THREAD_ID, PROP_UID, PROP_MODSEQ,
                PROP_INTERNAL_DATE, PROP_SAVE_DATE, PROP_SIZE, PROP_BODY_START, PROP_FLAGS, PROP_USER_FLAGS);

            List<Map<String, Object>> rows = YouTrackDBTransactions.queryRows(g,
                "SELECT " + metadataCols + " FROM " + CLASS_NAME + " WHERE mailboxId = :mbx AND uid IN :uids",
                "mbx", mailboxId,
                "uids", uidLongs);

            Map<MessageUid, MessageMetaData> result = new HashMap<>(rows.size());
            for (Map<String, Object> row : rows) {
                MailboxMessage msg = readMessage(row, mailbox.getMailboxId(), FetchType.METADATA);
                result.put(msg.getUid(), msg.metaData());
            }

            YouTrackDBTransactions.executeStrictTx(g, tx ->
                tx.command("DELETE VERTEX JamesMailboxMessage WHERE mailboxId = :mbx AND uid IN :uids",
                    "mbx", mailboxId,
                    "uids", uidLongs));

            return result;
        } catch (Exception e) {
            throw new MailboxException("Failed to delete messages in mailbox " + mailbox.getMailboxId().serialize(), e);
        }
    }

    @Override
    public MessageUid findFirstUnseenMessageUid(Mailbox mailbox) throws MailboxException {
        try {
            List<Map<String, Object>> rows = YouTrackDBTransactions.queryRows(g,
                "SELECT min(uid) AS firstUnseen FROM JamesMailboxMessage WHERE mailboxId = :mbx AND NOT (flags CONTAINS 'SEEN')",
                "mbx", mailbox.getMailboxId().serialize());
            if (!rows.isEmpty() && rows.getFirst().get("firstUnseen") instanceof Number n) {
                return MessageUid.of(n.longValue());
            }
            return null;
        } catch (Exception e) {
            throw new MailboxException("Failed to find first unseen message in mailbox " + mailbox.getMailboxId().serialize(), e);
        }
    }

    @Override
    public List<MessageUid> findRecentMessageUidsInMailbox(Mailbox mailbox) throws MailboxException {
        try {
            List<Map<String, Object>> rows = YouTrackDBTransactions.queryRows(g,
                "SELECT uid FROM JamesMailboxMessage WHERE mailboxId = :mbx AND flags CONTAINS 'RECENT' ORDER BY uid ASC",
                "mbx", mailbox.getMailboxId().serialize());
            List<MessageUid> recent = new ArrayList<>(rows.size());
            for (Map<String, Object> row : rows) {
                if (row.get(PROP_UID) instanceof Number n) {
                    recent.add(MessageUid.of(n.longValue()));
                }
            }
            return recent;
        } catch (Exception e) {
            throw new MailboxException("Failed to find recent messages in mailbox " + mailbox.getMailboxId().serialize(), e);
        }
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
        try {
            YouTrackDBTransactions.executeStrictTx(g, tx -> {
                while (messages.hasNext()) {
                    MailboxMessage member = messages.next();
                    Flags originalFlags = member.createFlags();
                    member.setFlags(flagsUpdateCalculator.buildNewFlags(originalFlags));
                    Flags newFlags = member.createFlags();
                    if (UpdatedFlags.flagsChanged(originalFlags, newFlags)) {
                        long currentModSeq = member.getModSeq().asLong();
                        member.setModSeq(modSeq);
                        Set<String> systemFlags = extractSystemFlags(newFlags);
                        Set<String> userFlags = extractUserFlags(newFlags);
                        tx.command("UPDATE JamesMailboxMessage SET flags = :flags, userFlags = :userFlags, modSeq = :newModSeq WHERE mailboxId = :mbx AND uid = :uid AND (modSeq = :currModSeq OR modSeq < :newModSeq)",
                            "flags", systemFlags,
                            "userFlags", userFlags,
                            "newModSeq", modSeq.asLong(),
                            "mbx", mailbox.getMailboxId().serialize(),
                            "uid", member.getUid().asLong(),
                            "currModSeq", currentModSeq);
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
            });
        } catch (Exception e) {
            throw new MailboxException("Failed to update flags in mailbox " + mailbox.getMailboxId().serialize(), e);
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

    private static final Map<Flag, String> SYSTEM_FLAG_TO_NAME = Map.of(
        Flag.ANSWERED, "ANSWERED",
        Flag.DELETED, "DELETED",
        Flag.DRAFT, "DRAFT",
        Flag.FLAGGED, "FLAGGED",
        Flag.RECENT, "RECENT",
        Flag.SEEN, "SEEN"
    );

    private static final Map<String, Flag> NAME_TO_SYSTEM_FLAG = Map.of(
        "ANSWERED", Flag.ANSWERED,
        "DELETED", Flag.DELETED,
        "DRAFT", Flag.DRAFT,
        "FLAGGED", Flag.FLAGGED,
        "RECENT", Flag.RECENT,
        "SEEN", Flag.SEEN
    );

    private static Set<String> extractSystemFlags(Flags flags) {
        Set<String> set = new HashSet<>();
        for (Map.Entry<Flag, String> entry : SYSTEM_FLAG_TO_NAME.entrySet()) {
            if (flags.contains(entry.getKey())) {
                set.add(entry.getValue());
            }
        }
        return set;
    }

    private static Set<String> extractUserFlags(Flags flags) {
        return Set.of(flags.getUserFlags());
    }

    private static Flags readFlags(Map<String, Object> row) {
        Flags flags = new Flags();
        Object sysObj = row.get(PROP_FLAGS);
        if (sysObj instanceof Iterable<?> it) {
            for (Object f : it) {
                Flag systemFlag = NAME_TO_SYSTEM_FLAG.get(Objects.toString(f));
                if (systemFlag != null) {
                    flags.add(systemFlag);
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
