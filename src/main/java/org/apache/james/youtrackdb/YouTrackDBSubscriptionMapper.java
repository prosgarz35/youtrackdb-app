package org.apache.james.youtrackdb;

import java.util.List;
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
                tx.addV(CLASS_NAME)
                    .property(PROP_USER, subscription.getUser().asString())
                    .property(PROP_MAILBOX, subscription.getMailbox())
                    .iterate();
            });
        } catch (Exception e) {
            if (YouTrackDBTransactions.hasCause(e, RecordDuplicatedException.class)) {
                return; // idempotent due to UNIQUE B-Tree index on (user, mailbox)
            }
            throw new SubscriptionException(e);
        }
    }

    @Override
    public List<Subscription> findSubscriptionsForUser(Username user) throws SubscriptionException {
        try {
            return YouTrackDBTransactions.queryRows(g,
                "SELECT mailbox FROM JamesSubscription WHERE user = :user",
                "user", user.asString())
                .stream()
                .map(row -> row.get(PROP_MAILBOX))
                .filter(Objects::nonNull)
                .map(mailbox -> new Subscription(user, mailbox.toString()))
                .toList();
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
