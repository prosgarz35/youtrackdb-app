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
            YouTrackDBTransactions.executeStrictTx(g, tx ->
                tx.addV(CLASS).property(PROP_URL, urlStr).iterate());
        } catch (Exception e) {
            if (YouTrackDBTransactions.hasCause(e, com.jetbrains.youtrackdb.api.exception.RecordDuplicatedException.class)) {
                // Concurrent insert already succeeded - idempotent due to UNIQUE B-Tree index on (url)
                return;
            }
            throw e;
        }
    }

    @Override
    public Stream<MailRepositoryUrl> listDistinct() {
        return YouTrackDBTransactions.queryRows(g, "SELECT DISTINCT(url) AS url FROM " + CLASS)
            .stream()
            .map(row -> row.get(PROP_URL))
            .filter(Objects::nonNull)
            .map(Object::toString)
            .map(MailRepositoryUrl::from);
    }

    @Override
    public boolean contains(MailRepositoryUrl url) {
        Objects.requireNonNull(url, "url must not be null");
        String urlStr = url.asString();
        return !YouTrackDBTransactions.queryRows(g,
            "SELECT 1 FROM " + CLASS + " WHERE url = :url LIMIT 1", "url", urlStr).isEmpty();
    }
}
