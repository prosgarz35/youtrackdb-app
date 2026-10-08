package org.apache.james.youtrackdb;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.james.core.Username;
import org.apache.james.mailbox.exception.SubscriptionException;
import org.apache.james.mailbox.store.user.SubscriptionMapper;
import org.apache.james.mailbox.store.user.model.Subscription;

import com.jetbrains.youtrackdb.api.exception.RecordDuplicatedException;
import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;

public class YouTrackDBSubscriptionMapper implements SubscriptionMapper {

    private static final String CLASS_NAME = "JamesSubscription";
    private static final String PROP_USER = "user";
    private static final String PROP_MAILBOX = "mailbox";

    private final YTDBGraphTraversalSource g;

    public YouTrackDBSubscriptionMapper(YTDBGraphTraversalSource g) {
        this.g = Objects.requireNonNull(g, "g must not be null");
    }

    @Override
    public void save(Subscription subscription) throws SubscriptionException {
        try {
            YouTrackDBTransactions.executeStrictTx(g, tx -> {
                List<Map<String, Object>> existing = YouTrackDBTransactions.queryRows(tx,
                    "SELECT FROM JamesSubscription WHERE user = :user AND mailbox = :mailbox",
                    "user", subscription.getUser().asString(),
                    "mailbox", subscription.getMailbox());
                if (existing.isEmpty()) {
                    tx.addV(CLASS_NAME)
                        .property(PROP_USER, subscription.getUser().asString())
                        .property(PROP_MAILBOX, subscription.getMailbox())
                        .iterate();
                }
            });
        } catch (Exception e) {
            if (YouTrackDBTransactions.hasCause(e, RecordDuplicatedException.class)) {
                return; // idempotent
            }
            throw new SubscriptionException(e);
        }
    }

    @Override
    public List<Subscription> findSubscriptionsForUser(Username user) throws SubscriptionException {
        try {
            List<Map<String, Object>> rows = YouTrackDBTransactions.queryRows(g,
                "SELECT FROM JamesSubscription WHERE user = :user",
                "user", user.asString());
            List<Subscription> result = new ArrayList<>(rows.size());
            for (Map<String, Object> row : rows) {
                Object mailbox = row.get(PROP_MAILBOX);
                if (mailbox != null) {
                    result.add(new Subscription(user, mailbox.toString()));
                }
            }
            return result;
        } catch (Exception e) {
            throw new SubscriptionException(e);
        }
    }

    @Override
    public void delete(Subscription subscription) throws SubscriptionException {
        try {
            YouTrackDBTransactions.executeStrictTx(g, tx ->
                tx.command("DELETE VERTEX JamesSubscription WHERE user = :user AND mailbox = :mailbox",
                    "user", subscription.getUser().asString(),
                    "mailbox", subscription.getMailbox()));
        } catch (Exception e) {
            throw new SubscriptionException(e);
        }
    }

    @Override
    public void endRequest() {
    }
}
