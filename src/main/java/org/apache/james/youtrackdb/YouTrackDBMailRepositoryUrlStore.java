package org.apache.james.youtrackdb;

import java.util.Objects;
import java.util.stream.Stream;

import jakarta.inject.Inject;
import jakarta.inject.Singleton;

import org.apache.james.mailrepository.api.MailRepositoryUrl;
import org.apache.james.mailrepository.api.MailRepositoryUrlStore;

import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;

@Singleton
public class YouTrackDBMailRepositoryUrlStore implements MailRepositoryUrlStore {

    public static final String CLASS = "JamesMailRepositoryUrl";
    public static final String PROP_URL = "url";

    private final YTDBGraphTraversalSource g;

    @Inject
    public YouTrackDBMailRepositoryUrlStore(YTDBGraphTraversalSource g) {
        this.g = Objects.requireNonNull(g, "g must not be null");
    }

    @Override
    public void add(MailRepositoryUrl url) {
        Objects.requireNonNull(url, "url must not be null");
        String urlStr = url.asString();
        try {
            YouTrackDBTransactions.executeStrictTx(g, tx -> {
                boolean exists = tx.V().hasLabel(CLASS).has(PROP_URL, urlStr).hasNext();
                if (!exists) {
                    tx.addV(CLASS).property(PROP_URL, urlStr).iterate();
                }
            });
        } catch (Exception e) {
            if (YouTrackDBTransactions.hasCause(e, com.jetbrains.youtrackdb.api.exception.RecordDuplicatedException.class)) {
                // Concurrent insert already succeeded - idempotent
                return;
            }
            throw e;
        }
    }

    @Override
    public Stream<MailRepositoryUrl> listDistinct() {
        return g.computeInTx(tx -> tx.V().hasLabel(CLASS).values(PROP_URL).toList())
            .stream()
            .map(Object::toString)
            .distinct()
            .map(MailRepositoryUrl::from);
    }

    @Override
    public boolean contains(MailRepositoryUrl url) {
        Objects.requireNonNull(url, "url must not be null");
        String urlStr = url.asString();
        return g.computeInTx(tx -> tx.V().hasLabel(CLASS).has(PROP_URL, urlStr).hasNext());
    }
}
