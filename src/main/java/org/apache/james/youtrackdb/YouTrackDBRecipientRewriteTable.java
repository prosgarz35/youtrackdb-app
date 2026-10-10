package org.apache.james.youtrackdb;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import jakarta.inject.Inject;

import org.apache.james.rrt.api.RecipientRewriteTableException;
import org.apache.james.rrt.lib.AbstractRecipientRewriteTable;
import org.apache.james.rrt.lib.Mapping;
import org.apache.james.rrt.lib.MappingSource;
import org.apache.james.rrt.lib.Mappings;
import org.apache.james.rrt.lib.MappingsImpl;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.jetbrains.youtrackdb.api.exception.RecordDuplicatedException;
import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;

public class YouTrackDBRecipientRewriteTable extends AbstractRecipientRewriteTable {
    private static final String CLASS_NAME = "JamesRRTMapping";
    private static final String PROP_SOURCE = "source";
    private static final String PROP_MAPPING = "mapping";

    private final YTDBGraphTraversalSource g;
    private final Cache<MappingSource, Mappings> mappingsCache = Caffeine.newBuilder()
        .maximumSize(50_000)
        .build();

    @Inject
    public YouTrackDBRecipientRewriteTable(YTDBGraphTraversalSource g) {
        this.g = g;
    }

    @Override
    public void addMapping(MappingSource source, Mapping mapping) throws RecipientRewriteTableException {
        try {
            YouTrackDBTransactions.executeStrictTx(g, tx -> tx.addV(CLASS_NAME)
                .property(PROP_SOURCE, source.asString())
                .property(PROP_MAPPING, mapping.asString())
                .iterate());
            mappingsCache.invalidate(source);
        } catch (Exception e) {
            if (YouTrackDBTransactions.hasCause(e, RecordDuplicatedException.class)) {
                return; // already exists (idempotent add)
            }
            throw new RecipientRewriteTableException("Failed to add mapping: " + source.asString() + " -> " + mapping.asString(), e);
        }
    }

    @Override
    public void removeMapping(MappingSource source, Mapping mapping) throws RecipientRewriteTableException {
        try {
            YouTrackDBTransactions.executeStrictTx(g, tx ->
                tx.command("DELETE VERTEX JamesRRTMapping WHERE source = :source AND mapping = :mapping",
                    "source", source.asString(), "mapping", mapping.asString()));
            mappingsCache.invalidate(source);
        } catch (Exception e) {
            throw new RecipientRewriteTableException("Failed to remove mapping: " + source.asString() + " -> " + mapping.asString(), e);
        }
    }

    @Override
    public Mappings getStoredMappings(MappingSource source) throws RecipientRewriteTableException {
        Mappings cached = mappingsCache.getIfPresent(source);
        if (cached != null) {
            return cached;
        }

        try {
            List<Mapping> mappings = YouTrackDBTransactions.queryRows(g,
                    "SELECT mapping FROM JamesRRTMapping WHERE source = :src", "src", source.asString())
                .stream()
                .map(row -> row.get(PROP_MAPPING))
                .filter(Objects::nonNull)
                .map(Object::toString)
                .map(Mapping::of)
                .toList();
            Mappings result = MappingsImpl.fromMappings(mappings.stream());
            mappingsCache.put(source, result);
            return result;
        } catch (Exception e) {
            throw new RecipientRewriteTableException("Failed to get stored mappings for " + source.asString(), e);
        }
    }

    @Override
    public Map<MappingSource, Mappings> getAllMappings() throws RecipientRewriteTableException {
        try {
            return YouTrackDBTransactions.queryRows(g, "SELECT source, mapping FROM JamesRRTMapping")
                .stream()
                .filter(row -> row.get(PROP_SOURCE) != null && row.get(PROP_MAPPING) != null)
                .collect(java.util.stream.Collectors.groupingBy(
                    row -> MappingSource.parse(row.get(PROP_SOURCE).toString()),
                    java.util.stream.Collector.of(
                        MappingsImpl::builder,
                        (builder, row) -> builder.add(Mapping.of(row.get(PROP_MAPPING).toString())),
                        (b1, b2) -> { b1.addAll(b2.build()); return b1; },
                        MappingsImpl.Builder::build)));
        } catch (Exception e) {
            throw new RecipientRewriteTableException("Failed to get all mappings", e);
        }
    }
}
