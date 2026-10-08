package org.apache.james.youtrackdb;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;

import org.apache.commons.lang3.StringUtils;
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
        return g.computeInTx(tx -> {
            List<Vertex> vertices = tx.V().hasLabel(CLASS).has(PROP_MAILBOX_ID, mailboxId.serialize()).toList();
            ImmutableList.Builder<MailboxAnnotation> builder = ImmutableList.builder();
            for (Vertex v : vertices) {
                var keyProp = v.property(PROP_KEY);
                var valProp = v.property(PROP_VALUE);
                if (keyProp.isPresent()) {
                    String key = keyProp.value().toString();
                    String val = valProp.isPresent() ? valProp.value().toString() : "";
                    builder.add(MailboxAnnotation.newInstance(new MailboxAnnotationKey(key), val));
                }
            }
            return builder.build();
        });
    }

    @Override
    public List<MailboxAnnotation> getAnnotationsByKeys(MailboxId mailboxId, Set<MailboxAnnotationKey> keys) {
        return getAllAnnotations(mailboxId)
            .stream()
            .filter(a -> keys.contains(a.getKey()))
            .collect(ImmutableList.toImmutableList());
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
        return key -> key.equals(input.getKey()) || StringUtils.startsWith(input.getKey().asString(), key.asString() + "/");
    }

    @Override
    public void insertAnnotation(MailboxId mailboxId, MailboxAnnotation mailboxAnnotation) {
        Preconditions.checkArgument(!mailboxAnnotation.isNil());
        String mId = getMailboxIdString(mailboxId);
        String key = mailboxAnnotation.getKey().asString();
        String val = mailboxAnnotation.getValue().orElse("");

        try {
            YouTrackDBTransactions.retryOnConflict(10, () -> {
                YouTrackDBTransactions.executeStrictTx(g, tx -> {
                    var existing = tx.V().hasLabel(CLASS)
                        .has(PROP_MAILBOX_ID, mId)
                        .has(PROP_KEY, key)
                        .tryNext();
                    if (existing.isPresent()) {
                        existing.get().property(PROP_VALUE, val);
                    } else {
                        tx.addV(CLASS)
                            .property(PROP_MAILBOX_ID, mId)
                            .property(PROP_KEY, key)
                            .property(PROP_VALUE, val)
                            .iterate();
                    }
                });
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
                YouTrackDBTransactions.executeStrictTx(g, tx -> {
                    tx.V().hasLabel(CLASS)
                        .has(PROP_MAILBOX_ID, mId)
                        .has(PROP_KEY, keyStr)
                        .drop()
                        .iterate();
                });
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
        return g.computeInTx(tx -> tx.V().hasLabel(CLASS)
            .has(PROP_MAILBOX_ID, mId)
            .has(PROP_KEY, keyStr)
            .hasNext());
    }

    @Override
    public int countAnnotations(MailboxId mailboxId) {
        String mId = getMailboxIdString(mailboxId);
        long count = g.computeInTx(tx -> tx.V().hasLabel(CLASS)
            .has(PROP_MAILBOX_ID, mId)
            .count()
            .next());
        return Math.toIntExact(count);
    }
}
