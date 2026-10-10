package org.apache.james.youtrackdb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import jakarta.mail.Flags;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.james.mailbox.MessageManager;
import org.apache.james.mailbox.MessageUid;
import org.apache.james.mailbox.exception.MailboxException;
import org.apache.james.mailbox.exception.MailboxNotFoundException;
import org.apache.james.mailbox.model.ComposedMessageIdWithMetaData;
import org.apache.james.mailbox.model.Mailbox;
import org.apache.james.mailbox.model.MailboxId;
import org.apache.james.mailbox.model.MessageId;
import org.apache.james.mailbox.model.MessageRange;
import org.apache.james.mailbox.model.UpdatedFlags;
import org.apache.james.mailbox.store.FlagsUpdateCalculator;
import org.apache.james.mailbox.store.MailboxReactorUtils;
import org.apache.james.mailbox.store.mail.MailboxMapper;
import org.apache.james.mailbox.store.mail.MessageIdMapper;
import org.apache.james.mailbox.store.mail.MessageMapper;
import org.apache.james.mailbox.store.mail.model.MailboxMessage;
import org.reactivestreams.Publisher;

import com.google.common.collect.ImmutableListMultimap;
import com.google.common.collect.Multimap;
import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class YouTrackDBMessageIdMapper implements MessageIdMapper {

    private final MailboxMapper mailboxMapper;
    private final MessageMapper messageMapper;
    private final YTDBGraphTraversalSource g;

    public YouTrackDBMessageIdMapper(MailboxMapper mailboxMapper,
                                     MessageMapper messageMapper,
                                     YTDBGraphTraversalSource g) {
        this.mailboxMapper = Objects.requireNonNull(mailboxMapper, "mailboxMapper must not be null");
        this.messageMapper = Objects.requireNonNull(messageMapper, "messageMapper must not be null");
        this.g = Objects.requireNonNull(g, "g must not be null");
    }

    @Override
    public List<MailboxMessage> find(Collection<MessageId> messageIds, MessageMapper.FetchType fetchType) {
        return findReactive(messageIds, fetchType).collectList().block();
    }

    @Override
    public Publisher<ComposedMessageIdWithMetaData> findMetadata(MessageId messageId) {
        return findReactive(List.of(messageId), MessageMapper.FetchType.METADATA)
            .map(MailboxMessage::getComposedMessageIdWithMetaData);
    }

    @Override
    public Flux<MailboxMessage> findReactive(Collection<MessageId> messageIds, MessageMapper.FetchType fetchType) {
        if (messageIds.isEmpty()) {
            return Flux.empty();
        }
        return Flux.defer(() -> {
            try {
                List<String> serializedIds = messageIds.stream()
                    .map(MessageId::serialize)
                    .toList();

                String selectClause = (fetchType == MessageMapper.FetchType.METADATA)
                    ? "SELECT mailboxId, messageId, threadId, uid, modSeq, internalDate, saveDate, size, bodyStartOctet, flags, userFlags FROM JamesMailboxMessage"
                    : "SELECT mailboxId, messageId, threadId, uid, modSeq, internalDate, saveDate, size, bodyStartOctet, flags, userFlags, content FROM JamesMailboxMessage";

                List<Map<String, Object>> rows = YouTrackDBTransactions.queryRows(g,
                    selectClause + " WHERE messageId IN :mids",
                    "mids", serializedIds);

                List<MailboxMessage> messages = rows.stream()
                    .map(row -> {
                        String mbxIdStr = Objects.toString(row.get("mailboxId"), null);
                        MailboxId mbxId = YouTrackDBMailboxId.of(mbxIdStr);
                        return YouTrackDBMessageMapper.readMessage(row, mbxId, fetchType);
                    })
                    .toList();
                return Flux.fromIterable(messages);
            } catch (Exception e) {
                return Flux.error(new RuntimeException("Error finding messages by messageIds", e));
            }
        }).subscribeOn(YouTrackDBTransactions.virtualThreadScheduler());
    }

    @Override
    public List<MailboxId> findMailboxes(MessageId messageId) {
        return findMailboxesReactive(messageId).collectList().block();
    }

    @Override
    public Flux<MailboxId> findMailboxesReactive(MessageId messageId) {
        return Flux.defer(() -> {
            try {
                List<Map<String, Object>> rows = YouTrackDBTransactions.queryRows(g,
                    "SELECT mailboxId FROM JamesMailboxMessage WHERE messageId = :mid",
                    "mid", messageId.serialize());
                List<MailboxId> mailboxIds = rows.stream()
                    .<MailboxId>map(r -> YouTrackDBMailboxId.of(Objects.toString(r.get("mailboxId"), null)))
                    .distinct()
                    .toList();
                return Flux.fromIterable(mailboxIds);
            } catch (Exception e) {
                return Flux.error(e);
            }
        }).subscribeOn(YouTrackDBTransactions.virtualThreadScheduler());
    }

    @Override
    public void save(MailboxMessage mailboxMessage) throws MailboxNotFoundException, MailboxException {
        Mailbox mailbox = MailboxReactorUtils.block(mailboxMapper.findMailboxById(mailboxMessage.getMailboxId()));
        messageMapper.add(mailbox, mailboxMessage);
    }

    @Override
    public void copyInMailbox(MailboxMessage mailboxMessage, Mailbox mailbox) throws MailboxException {
        List<MailboxId> mailboxes = findMailboxes(mailboxMessage.getMessageId());
        if (!mailboxes.contains(mailbox.getMailboxId())) {
            save(mailboxMessage);
        }
    }

    @Override
    public void delete(MessageId messageId) {
        List<MailboxMessage> messages = find(List.of(messageId), MessageMapper.FetchType.METADATA);
        deleteGroupedByMailbox(messageId, messages);
    }

    @Override
    public void delete(MessageId messageId, Collection<MailboxId> mailboxIds) {
        List<MailboxMessage> messages = find(List.of(messageId), MessageMapper.FetchType.METADATA)
            .stream()
            .filter(message -> mailboxIds.contains(message.getMailboxId()))
            .toList();
        deleteGroupedByMailbox(messageId, messages);
    }

    private void deleteGroupedByMailbox(MessageId messageId, List<MailboxMessage> messages) {
        if (messages.isEmpty()) {
            return;
        }
        Map<MailboxId, List<MessageUid>> uidsByMailbox = messages.stream()
            .collect(java.util.stream.Collectors.groupingBy(
                MailboxMessage::getMailboxId,
                java.util.stream.Collectors.mapping(MailboxMessage::getUid, java.util.stream.Collectors.toList())));

        for (Map.Entry<MailboxId, List<MessageUid>> entry : uidsByMailbox.entrySet()) {
            try {
                Mailbox mailbox = MailboxReactorUtils.block(mailboxMapper.findMailboxById(entry.getKey()));
                messageMapper.deleteMessages(mailbox, entry.getValue());
            } catch (MailboxException e) {
                throw new RuntimeException("Failed to delete message " + messageId.serialize() + " in mailbox " + entry.getKey().serialize(), e);
            }
        }
    }

    @Override
    public Mono<Multimap<MailboxId, UpdatedFlags>> setFlags(MessageId messageId,
                                                            List<MailboxId> mailboxIds,
                                                            Flags newState,
                                                            MessageManager.FlagsUpdateMode updateMode) {
        return findReactive(List.of(messageId), MessageMapper.FetchType.METADATA)
            .filter(message -> mailboxIds.contains(message.getMailboxId()))
            .collect(java.util.stream.Collectors.groupingBy(
                MailboxMessage::getMailboxId,
                java.util.stream.Collectors.mapping(MailboxMessage::getUid, java.util.stream.Collectors.toList())))
            .flatMapMany(uidsByMailbox -> Flux.fromIterable(uidsByMailbox.entrySet()))
            .concatMap(entry -> updateMailboxFlags(entry.getKey(), entry.getValue(), newState, updateMode))
            .distinct()
            .collect(ImmutableListMultimap.toImmutableListMultimap(Pair::getKey, Pair::getValue));
    }

    private Flux<Pair<MailboxId, UpdatedFlags>> updateMailboxFlags(MailboxId mailboxId,
                                                                   List<MessageUid> uids,
                                                                   Flags newState,
                                                                   MessageManager.FlagsUpdateMode updateMode) {
        if (uids.isEmpty()) {
            return Flux.empty();
        }
        FlagsUpdateCalculator flagsUpdateCalculator = new FlagsUpdateCalculator(newState, updateMode);
        return mailboxMapper.findMailboxById(mailboxId)
            .flatMapMany(mailbox -> {
                try {
                    List<UpdatedFlags> result = new ArrayList<>();
                    for (MessageRange range : MessageRange.toRanges(uids)) {
                        Iterator<UpdatedFlags> it = messageMapper.updateFlags(mailbox, flagsUpdateCalculator, range);
                        while (it.hasNext()) {
                            result.add(it.next());
                        }
                    }
                    return Flux.fromIterable(result);
                } catch (MailboxException e) {
                    return Flux.error(e);
                }
            })
            .map(updatedFlags -> Pair.of(mailboxId, updatedFlags));
    }
}
