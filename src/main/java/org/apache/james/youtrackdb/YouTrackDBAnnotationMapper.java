package org.apache.james.youtrackdb;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

import org.apache.james.mailbox.model.MailboxAnnotation;
import org.apache.james.mailbox.model.MailboxAnnotationKey;
import org.apache.james.mailbox.model.MailboxId;
import org.apache.james.mailbox.store.mail.AnnotationMapper;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;

public class YouTrackDBAnnotationMapper implements AnnotationMapper {

    public static final String CLASS = "JamesMailboxAnnotation";
    public static final String PROP_MAILBOX_ID = "mailboxId";
    public static final String PROP_KEY = "key";
    public static final String PROP_VALUE = "value";

    private final YTDBGraphTraversalSource g;

    public YouTrackDBAnnotationMapper(YTDBGraphTraversalSource g) {
        this.g = Objects.requireNonNull(g, "g must not be null");
    }

    private String getMailboxIdString(MailboxId mailboxId) {
        return mailboxId.serialize();
    }

    @Override
    public List<MailboxAnnotation> getAllAnnotations(MailboxId mailboxId) {
        List<Map<String, Object>> rows = YouTrackDBTransactions.queryRows(g,
            "SELECT key, value FROM " + CLASS + " WHERE mailboxId = :mbx",
            "mbx", mailboxId.serialize());
        ImmutableList.Builder<MailboxAnnotation> builder = ImmutableList.builder();
        for (Map<String, Object> row : rows) {
            Object keyObj = row.get(PROP_KEY);
            if (keyObj != null) {
                String key = keyObj.toString();
                Object valObj = row.get(PROP_VALUE);
                String val = valObj != null ? valObj.toString() : "";
                builder.add(MailboxAnnotation.newInstance(new MailboxAnnotationKey(key), val));
            }
        }
        return builder.build();
    }

    @Override
    public List<MailboxAnnotation> getAnnotationsByKeys(MailboxId mailboxId, Set<MailboxAnnotationKey> keys) {
        if (keys == null || keys.isEmpty()) {
            return Collections.emptyList();
        }
        List<String> keyStrings = keys.stream().map(MailboxAnnotationKey::asString).toList();
        List<Map<String, Object>> rows = YouTrackDBTransactions.queryRows(g,
            "SELECT key, value FROM " + CLASS + " WHERE mailboxId = :mbx AND key IN :keys",
            "mbx", mailboxId.serialize(),
            "keys", keyStrings);
        ImmutableList.Builder<MailboxAnnotation> builder = ImmutableList.builder();
        for (Map<String, Object> row : rows) {
            Object keyObj = row.get(PROP_KEY);
            if (keyObj != null) {
                String key = keyObj.toString();
                Object valObj = row.get(PROP_VALUE);
                String val = valObj != null ? valObj.toString() : "";
                builder.add(MailboxAnnotation.newInstance(new MailboxAnnotationKey(key), val));
            }
        }
        return builder.build();
    }

    @Override
    public List<MailboxAnnotation> getAnnotationsByKeysWithAllDepth(MailboxId mailboxId, Set<MailboxAnnotationKey> keys) {
        return getAllAnnotations(mailboxId)
            .stream()
            .filter(getPredicateFilterByAll(keys))
            .collect(ImmutableList.toImmutableList());
    }

    @Override
    public List<MailboxAnnotation> getAnnotationsByKeysWithOneDepth(MailboxId mailboxId, Set<MailboxAnnotationKey> keys) {
        return getAnnotationsByKeysWithAllDepth(mailboxId, keys)
            .stream()
            .filter(getPredicateFilterByOne(keys))
            .collect(ImmutableList.toImmutableList());
    }

    private Predicate<MailboxAnnotation> getPredicateFilterByAll(final Set<MailboxAnnotationKey> keys) {
        return input -> keys.stream().anyMatch(filterAnnotationsByPrefix(input));
    }

    private Predicate<MailboxAnnotation> getPredicateFilterByOne(final Set<MailboxAnnotationKey> keys) {
        return input -> keys.stream().anyMatch(filterAnnotationsByParentKey(input.getKey()));
    }

    private Predicate<MailboxAnnotationKey> filterAnnotationsByParentKey(final MailboxAnnotationKey input) {
        return key -> input.countComponents() <= (key.countComponents() + 1);
    }

    private Predicate<MailboxAnnotationKey> filterAnnotationsByPrefix(final MailboxAnnotation input) {
        return key -> key.equals(input.getKey()) || input.getKey().asString().startsWith(key.asString() + "/");
    }

    @Override
    public void insertAnnotation(MailboxId mailboxId, MailboxAnnotation mailboxAnnotation) {
        Preconditions.checkArgument(!mailboxAnnotation.isNil());
        String mId = getMailboxIdString(mailboxId);
        String key = mailboxAnnotation.getKey().asString();
        String val = mailboxAnnotation.getValue().orElse("");

        try {
            YouTrackDBTransactions.retryOnConflict(10, () -> {
                try {
                    YouTrackDBTransactions.executeStrictTx(g, tx ->
                        tx.addV(CLASS)
                            .property(PROP_MAILBOX_ID, mId)
                            .property(PROP_KEY, key)
                            .property(PROP_VALUE, val)
                            .iterate());
                } catch (Exception e) {
                    if (YouTrackDBTransactions.hasCause(e, com.jetbrains.youtrackdb.api.exception.RecordDuplicatedException.class)) {
                        YouTrackDBTransactions.executeStrictTx(g, tx ->
                            tx.command("UPDATE " + CLASS + " SET value = :val WHERE mailboxId = :mbx AND key = :key",
                                "val", val,
                                "mbx", mId,
                                "key", key));
                    } else {
                        throw e;
                    }
                }
                return null;
            });
        } catch (Exception e) {
            throw new RuntimeException("Failed to insert/update annotation " + key + " for mailbox " + mId, e);
        }
    }

    @Override
    public void deleteAnnotation(MailboxId mailboxId, MailboxAnnotationKey key) {
        String mId = getMailboxIdString(mailboxId);
        String keyStr = key.asString();

        try {
            YouTrackDBTransactions.retryOnConflict(() -> {
                YouTrackDBTransactions.executeStrictTx(g, tx ->
                    tx.command("DELETE VERTEX " + CLASS + " WHERE mailboxId = :mbx AND key = :key",
                        "mbx", mId, "key", keyStr));
                return null;
            });
        } catch (Exception e) {
            throw new RuntimeException("Failed to delete annotation " + key + " for mailbox " + mId, e);
        }
    }

    @Override
    public boolean exist(MailboxId mailboxId, MailboxAnnotation mailboxAnnotation) {
        String mId = getMailboxIdString(mailboxId);
        String keyStr = mailboxAnnotation.getKey().asString();
        List<Map<String, Object>> rows = YouTrackDBTransactions.queryRows(g,
            "SELECT 1 FROM " + CLASS + " WHERE mailboxId = :mbx AND key = :key LIMIT 1",
            "mbx", mId, "key", keyStr);
        return !rows.isEmpty();
    }

    @Override
    public int countAnnotations(MailboxId mailboxId) {
        String mId = getMailboxIdString(mailboxId);
        List<Map<String, Object>> rows = YouTrackDBTransactions.queryRows(g,
            "SELECT count(*) AS cnt FROM " + CLASS + " WHERE mailboxId = :mbx",
            "mbx", mId);
        if (!rows.isEmpty() && rows.getFirst().get("cnt") instanceof Number n) {
            return n.intValue();
        }
        return 0;
    }
}
