package org.apache.james.youtrackdb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import jakarta.inject.Inject;

import org.apache.james.rrt.lib.AbstractRecipientRewriteTable;
import org.apache.james.rrt.lib.Mapping;
import org.apache.james.rrt.lib.MappingSource;
import org.apache.james.rrt.lib.Mappings;
import org.apache.james.rrt.lib.MappingsImpl;

import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class YouTrackDBRecipientRewriteTable extends AbstractRecipientRewriteTable {
    private static final String CLASS_NAME = "JamesRRTMapping";
    private static final String PROP_SOURCE = "source";
    private static final String PROP_MAPPING = "mapping";

    private final YTDBGraphTraversalSource g;

    @Inject
    public YouTrackDBRecipientRewriteTable(YTDBGraphTraversalSource g) {
        this.g = g;
    }

    @Override
    public void addMapping(MappingSource source, Mapping mapping) {
        try {
            g.executeInTx(tx -> {
                var traversal = tx.V().hasLabel(CLASS_NAME)
                    .has(PROP_SOURCE, source.asString())
                    .has(PROP_MAPPING, mapping.asString());
                if (traversal.hasNext()) {
                    return; // already exists
                }
                tx.addV(CLASS_NAME)
                    .property(PROP_SOURCE, source.asString())
                    .property(PROP_MAPPING, mapping.asString())
                    .iterate();
            });
        } catch (Exception e) {
            throw new RuntimeException("Failed to add mapping: " + source.asString() + " -> " + mapping.asString(), e);
        }
    }

    @Override
    public void removeMapping(MappingSource source, Mapping mapping) {
        try {
            g.executeInTx(tx -> {
                var traversal = tx.V().hasLabel(CLASS_NAME)
                    .has(PROP_SOURCE, source.asString())
                    .has(PROP_MAPPING, mapping.asString());
                while (traversal.hasNext()) {
                    traversal.next().remove();
                }
            });
        } catch (Exception e) {
            throw new RuntimeException("Failed to remove mapping: " + source.asString() + " -> " + mapping.asString(), e);
        }
    }

    @Override
    public Mappings getStoredMappings(MappingSource source) {
        try {
            return g.computeInTx(tx -> {
                List<Mapping> list = new ArrayList<>();
                var traversal = tx.V().hasLabel(CLASS_NAME)
                    .has(PROP_SOURCE, source.asString())
                    .<String>values(PROP_MAPPING);
                while (traversal.hasNext()) {
                    list.add(Mapping.of(traversal.next()));
                }
                return MappingsImpl.fromMappings(list.stream());
            });
        } catch (Exception e) {
            throw new RuntimeException("Failed to get stored mappings for " + source.asString(), e);
        }
    }

    @Override
    public Map<MappingSource, Mappings> getAllMappings() {
        try {
            return g.computeInTx(tx -> {
                Map<MappingSource, List<Mapping>> map = new HashMap<>();
                var traversal = tx.V().hasLabel(CLASS_NAME);
                while (traversal.hasNext()) {
                    Vertex v = traversal.next();
                    String src = v.value(PROP_SOURCE);
                    String mappingStr = v.value(PROP_MAPPING);
                    if (src != null && mappingStr != null) {
                        MappingSource source = MappingSource.parse(src);
                        map.computeIfAbsent(source, s -> new ArrayList<>()).add(Mapping.of(mappingStr));
                    }
                }
                Map<MappingSource, Mappings> result = new HashMap<>();
                map.forEach((k, v) -> result.put(k, MappingsImpl.fromMappings(v.stream())));
                return result;
            });
        } catch (Exception e) {
            throw new RuntimeException("Failed to get all mappings", e);
        }
    }
}
