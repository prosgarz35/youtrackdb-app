package org.apache.james.youtrackdb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import org.apache.james.core.Username;
import org.apache.james.mailbox.acl.ACLDiff;
import org.apache.james.mailbox.exception.MailboxExistsException;
import org.apache.james.mailbox.exception.MailboxNotFoundException;
import org.apache.james.mailbox.exception.UnsupportedRightException;
import org.apache.james.mailbox.model.Mailbox;
import org.apache.james.mailbox.model.MailboxACL;
import org.apache.james.mailbox.model.MailboxACL.NameType;
import org.apache.james.mailbox.model.MailboxACL.Right;
import org.apache.james.mailbox.model.MailboxId;
import org.apache.james.mailbox.model.MailboxPath;
import org.apache.james.mailbox.model.UidValidity;
import org.apache.james.mailbox.model.search.MailboxQuery;
import org.apache.james.mailbox.store.mail.MailboxMapper;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Preconditions;
import com.jetbrains.youtrackdb.api.exception.RecordDuplicatedException;
import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class YouTrackDBMailboxMapper implements MailboxMapper {

    private static final String CLASS_NAME = "JamesMailbox";
    private static final String PROP_MAILBOX_ID = "mailboxId";
    private static final String PROP_NAMESPACE = "namespace";
    private static final String PROP_USER = "user";
    private static final String PROP_NAME = "name";
    private static final String PROP_UID_VALIDITY = "uidValidity";
    private static final String PROP_ACL = "acl";

    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private final YTDBGraphTraversalSource g;

    public YouTrackDBMailboxMapper(YTDBGraphTraversalSource g) {
        this.g = Objects.requireNonNull(g, "g must not be null");
    }

    @Override
    public Mono<Mailbox> create(MailboxPath mailboxPath, UidValidity uidValidity) {
        return Mono.fromCallable(() -> {
            YouTrackDBMailboxId id = YouTrackDBMailboxId.generate();
            Mailbox mailbox = new Mailbox(mailboxPath, uidValidity, id);

            try {
                YouTrackDBTransactions.executeStrictTx(g, tx -> {
                    List<Map<String, Object>> existing = YouTrackDBTransactions.queryRows(tx,
                        "SELECT FROM JamesMailbox WHERE namespace = :ns AND user = :user AND name = :name",
                        "ns", mailboxPath.getNamespace(),
                        "user", mailboxPath.getUser().asString(),
                        "name", mailboxPath.getName());
                    if (!existing.isEmpty()) {
                        throw new MailboxExistsException(mailboxPath.getName());
                    }

                    tx.addV(CLASS_NAME)
                        .property(PROP_MAILBOX_ID, id.serialize())
                        .property(PROP_NAMESPACE, mailboxPath.getNamespace())
                        .property(PROP_USER, mailboxPath.getUser().asString())
                        .property(PROP_NAME, mailboxPath.getName())
                        .property(PROP_UID_VALIDITY, uidValidity.asLong())
                        .property("lastUid", 0L)
                        .property("highestModSeq", 0L)
                        .property(PROP_ACL, serializeACL(mailbox.getACL()))
                        .iterate();
                });
            } catch (Exception e) {
                if (e instanceof MailboxExistsException || YouTrackDBTransactions.hasCause(e, RecordDuplicatedException.class)) {
                    throw new MailboxExistsException(mailboxPath.getName());
                }
                throw e;
            }

            return mailbox;
        }).subscribeOn(Schedulers.boundedElastic());
    }

    @Override
    public Mono<MailboxId> rename(Mailbox mailbox) {
        Preconditions.checkNotNull(mailbox.getMailboxId(), "A mailbox we want to rename should have a defined mailboxId");

        return Mono.fromCallable(() -> {
            try {
                YouTrackDBTransactions.executeStrictTx(g, tx -> {
                    List<Map<String, Object>> existing = YouTrackDBTransactions.queryRows(tx,
                        "SELECT FROM JamesMailbox WHERE namespace = :ns AND user = :user AND name = :name AND mailboxId <> :id",
                        "ns", mailbox.getNamespace(),
                        "user", mailbox.getUser().asString(),
                        "name", mailbox.getName(),
                        "id", mailbox.getMailboxId().serialize());
                    if (!existing.isEmpty()) {
                        throw new MailboxExistsException(mailbox.generateAssociatedPath().getName());
                    }

                    List<Map<String, Object>> target = YouTrackDBTransactions.queryRows(tx,
                        "SELECT FROM JamesMailbox WHERE mailboxId = :id",
                        "id", mailbox.getMailboxId().serialize());
                    if (target.isEmpty()) {
                        throw new MailboxNotFoundException(mailbox.getMailboxId());
                    }

                    tx.command("UPDATE JamesMailbox SET namespace = :ns, user = :user, name = :name WHERE mailboxId = :id",
                        "ns", mailbox.getNamespace(),
                        "user", mailbox.getUser().asString(),
                        "name", mailbox.getName(),
                        "id", mailbox.getMailboxId().serialize());
                });
            } catch (Exception e) {
                if (e instanceof MailboxExistsException || YouTrackDBTransactions.hasCause(e, RecordDuplicatedException.class)) {
                    throw new MailboxExistsException(mailbox.generateAssociatedPath().getName());
                }
                throw e;
            }

            return mailbox.getMailboxId();
        }).subscribeOn(Schedulers.boundedElastic());
    }

    @Override
    public Mono<Void> delete(Mailbox mailbox) {
        return Mono.fromRunnable(() -> {
            YouTrackDBTransactions.executeStrictTx(g, tx ->
                tx.command("DELETE VERTEX JamesMailbox WHERE mailboxId = :id",
                    "id", mailbox.getMailboxId().serialize()));
        }).subscribeOn(Schedulers.boundedElastic()).then();
    }

    @Override
    public Mono<Mailbox> findMailboxByPath(MailboxPath mailboxPath) {
        return Mono.fromCallable(() -> {
            List<Map<String, Object>> rows = YouTrackDBTransactions.queryRows(g,
                "SELECT FROM JamesMailbox WHERE namespace = :ns AND user = :user AND name = :name",
                "ns", mailboxPath.getNamespace(),
                "user", mailboxPath.getUser().asString(),
                "name", mailboxPath.getName());
            if (rows.isEmpty()) {
                return Optional.<Mailbox>empty();
            }
            return Optional.of(readMailbox(rows.get(0)));
        }).subscribeOn(Schedulers.boundedElastic())
          .flatMap(opt -> opt.map(Mono::just).orElseGet(Mono::empty));
    }

    @Override
    public Mono<Mailbox> findMailboxById(MailboxId mailboxId) {
        return Mono.fromCallable(() -> {
            List<Map<String, Object>> rows = YouTrackDBTransactions.queryRows(g,
                "SELECT FROM JamesMailbox WHERE mailboxId = :id",
                "id", mailboxId.serialize());
            if (rows.isEmpty()) {
                return Optional.<Mailbox>empty();
            }
            return Optional.of(readMailbox(rows.get(0)));
        }).subscribeOn(Schedulers.boundedElastic())
          .flatMap(opt -> opt.map(Mono::just).orElseGet(Mono::empty))
          .switchIfEmpty(Mono.error(new MailboxNotFoundException(mailboxId)));
    }

    @Override
    public Flux<Mailbox> findMailboxWithPathLike(MailboxQuery.UserBound query) {
        return list()
            .filter(query::matches);
    }

    @Override
    public Mono<Boolean> hasChildren(Mailbox mailbox, char delimiter) {
        String childPrefix = mailbox.getName() + delimiter;
        return list()
            .filter(box -> belongsToSameUser(mailbox, box) && box.getName().startsWith(childPrefix))
            .hasElements();
    }

    private boolean belongsToSameUser(Mailbox mailbox, Mailbox otherMailbox) {
        return Objects.equals(mailbox.getNamespace(), otherMailbox.getNamespace())
            && Objects.equals(mailbox.getUser(), otherMailbox.getUser());
    }

    @Override
    public Mono<ACLDiff> updateACL(Mailbox mailbox, MailboxACL.ACLCommand mailboxACLCommand) {
        return Mono.fromCallable(() -> {
            return YouTrackDBTransactions.computeStrictTx(g, tx -> {
                Mailbox found = findMailboxByIdSync(tx, mailbox.getMailboxId());
                if (found == null) {
                    throw new MailboxNotFoundException(mailbox.getMailboxId());
                }
                MailboxACL oldAcl = found.getACL();
                MailboxACL newAcl;
                try {
                    newAcl = oldAcl.apply(mailboxACLCommand);
                } catch (UnsupportedRightException e) {
                    throw new RuntimeException("ACL update failed", e);
                }

                tx.command("UPDATE JamesMailbox SET acl = :acl WHERE mailboxId = :id",
                    "acl", serializeACL(newAcl),
                    "id", mailbox.getMailboxId().serialize());

                mailbox.setACL(newAcl);
                return ACLDiff.computeDiff(oldAcl, newAcl);
            });
        }).subscribeOn(Schedulers.boundedElastic());
    }

    @Override
    public Mono<ACLDiff> setACL(Mailbox mailbox, MailboxACL mailboxACL) {
        return Mono.fromCallable(() -> {
            return YouTrackDBTransactions.computeStrictTx(g, tx -> {
                Mailbox found = findMailboxByIdSync(tx, mailbox.getMailboxId());
                if (found == null) {
                    throw new MailboxNotFoundException(mailbox.getMailboxId());
                }
                MailboxACL oldAcl = found.getACL();

                tx.command("UPDATE JamesMailbox SET acl = :acl WHERE mailboxId = :id",
                    "acl", serializeACL(mailboxACL),
                    "id", mailbox.getMailboxId().serialize());

                mailbox.setACL(mailboxACL);
                return ACLDiff.computeDiff(oldAcl, mailboxACL);
            });
        }).subscribeOn(Schedulers.boundedElastic());
    }

    @Override
    public Flux<Mailbox> findNonPersonalMailboxes(Username userName, Right right) {
        return list()
            .filter(mailbox -> hasRightOn(mailbox, userName, right));
    }

    private Boolean hasRightOn(Mailbox mailbox, Username userName, Right right) {
        return Optional.ofNullable(
            mailbox.getACL()
                .ofPositiveNameType(NameType.user)
                .get(MailboxACL.EntryKey.createUserEntryKey(userName)))
            .map(rights -> rights.contains(right))
            .orElse(false);
    }

    @Override
    public Flux<Mailbox> list() {
        return Mono.fromCallable(() -> {
            List<Map<String, Object>> rows = YouTrackDBTransactions.queryRows(g, "SELECT FROM JamesMailbox");
            List<Mailbox> result = new ArrayList<>(rows.size());
            for (Map<String, Object> row : rows) {
                result.add(readMailbox(row));
            }
            return result;
        }).subscribeOn(Schedulers.boundedElastic())
          .flatMapIterable(list -> list);
    }

    @Override
    public void endRequest() {
    }

    private Mailbox findMailboxByIdSync(YTDBGraphTraversalSource traversal, MailboxId mailboxId) {
        List<Map<String, Object>> rows = YouTrackDBTransactions.queryRows(traversal,
            "SELECT FROM JamesMailbox WHERE mailboxId = :id",
            "id", mailboxId.serialize());
        if (rows.isEmpty()) {
            return null;
        }
        return readMailbox(rows.get(0));
    }

    private Mailbox readMailbox(Map<String, Object> row) {
        String mailboxIdStr = Objects.toString(row.get(PROP_MAILBOX_ID), null);
        String namespace = Objects.toString(row.get(PROP_NAMESPACE), null);
        String user = Objects.toString(row.get(PROP_USER), null);
        String name = Objects.toString(row.get(PROP_NAME), null);
        Object uidValidityObj = row.get(PROP_UID_VALIDITY);
        long uidValidity = uidValidityObj instanceof Number ? ((Number) uidValidityObj).longValue() : 1L;
        String aclStr = Objects.toString(row.get(PROP_ACL), null);

        MailboxPath path = new MailboxPath(namespace, Username.of(user), name);
        Mailbox mailbox = new Mailbox(path, UidValidity.of(uidValidity), YouTrackDBMailboxId.of(mailboxIdStr));
        mailbox.setACL(deserializeACL(aclStr));
        return mailbox;
    }

    private String serializeACL(MailboxACL acl) {
        if (acl == null || acl.getEntries().isEmpty()) {
            return "{}";
        }
        try {
            Map<String, String> map = new HashMap<>();
            acl.getEntries().forEach((key, rights) -> map.put(key.serialize(), rights.serialize()));
            return OBJECT_MAPPER.writeValueAsString(map);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize ACL", e);
        }
    }

    private MailboxACL deserializeACL(String json) {
        if (json == null || json.trim().isEmpty() || json.trim().equals("{}")) {
            return MailboxACL.EMPTY;
        }
        try {
            Map<String, String> map = OBJECT_MAPPER.readValue(json, new TypeReference<Map<String, String>>() {});
            Map<MailboxACL.EntryKey, MailboxACL.Rfc4314Rights> entries = new HashMap<>();
            for (Map.Entry<String, String> entry : map.entrySet()) {
                entries.put(MailboxACL.EntryKey.deserialize(entry.getKey()), MailboxACL.Rfc4314Rights.fromSerializedRfc4314Rights(entry.getValue()));
            }
            return new MailboxACL(entries);
        } catch (Exception e) {
            return MailboxACL.EMPTY;
        }
    }
}
