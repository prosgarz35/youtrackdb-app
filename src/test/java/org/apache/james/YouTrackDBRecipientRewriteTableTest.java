package org.apache.james;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.nio.file.Path;

import org.apache.james.core.Domain;
import org.apache.james.rrt.lib.Mapping;
import org.apache.james.rrt.lib.MappingSource;
import org.apache.james.rrt.lib.Mappings;
import org.apache.james.youtrackdb.YouTrackDBRecipientRewriteTable;
import org.apache.james.youtrackdb.YouTrackDBTransactions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.jetbrains.youtrackdb.api.DatabaseType;
import com.jetbrains.youtrackdb.api.YouTrackDB;
import com.jetbrains.youtrackdb.api.YourTracks;
import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;

public class YouTrackDBRecipientRewriteTableTest {

    @TempDir
    Path tempDir;

    private YouTrackDB ytdb;
    private YTDBGraphTraversalSource g;
    private YouTrackDBRecipientRewriteTable rrt;

    @BeforeEach
    void setUp() {
        File dbDir = tempDir.resolve("var").resolve("youtrackdb").toFile();
        dbDir.mkdirs();
        ytdb = YourTracks.instance(dbDir.getAbsolutePath());
        ytdb.createIfNotExists("james", DatabaseType.DISK, "admin", "admin", "admin");
        g = ytdb.openTraversal("james", "admin", "admin");

        YouTrackDBTransactions.executeStrictTx(g, tx -> {
            tx.command("CREATE CLASS JamesRRTMapping IF NOT EXISTS EXTENDS V");
            tx.command("CREATE PROPERTY JamesRRTMapping.source IF NOT EXISTS STRING");
            tx.command("CREATE PROPERTY JamesRRTMapping.mapping IF NOT EXISTS STRING");
            tx.command("CREATE INDEX JamesRRTMapping.sourceAndMapping IF NOT EXISTS ON JamesRRTMapping (source, mapping) UNIQUE");
        });

        rrt = new YouTrackDBRecipientRewriteTable(g);
    }

    @AfterEach
    void tearDown() {
        if (g != null) {
            g.close();
        }
        if (ytdb != null) {
            ytdb.close();
        }
    }

    @Test
    void addAndGetStoredMappingsShouldBeCachedAndCorrect() throws Exception {
        MappingSource source = MappingSource.fromUser("bob", Domain.of("example.com"));
        Mapping mapping1 = Mapping.alias("bob-alias@example.com");
        Mapping mapping2 = Mapping.forward("bob-forward@example.com");

        rrt.addMapping(source, mapping1);
        rrt.addMapping(source, mapping2);

        Mappings mappings = rrt.getStoredMappings(source);
        assertThat(mappings.asStrings()).containsExactlyInAnyOrder(mapping1.asString(), mapping2.asString());

        // Fast cached lookup
        Mappings cachedMappings = rrt.getStoredMappings(source);
        assertThat(cachedMappings.asStrings()).containsExactlyInAnyOrder(mapping1.asString(), mapping2.asString());

        // Remove mapping1 -> cache invalidation
        rrt.removeMapping(source, mapping1);

        Mappings afterRemove = rrt.getStoredMappings(source);
        assertThat(afterRemove.asStrings()).containsExactly(mapping2.asString());
    }

    @Test
    void idempotentAddMappingShouldNotFail() throws Exception {
        MappingSource source = MappingSource.fromUser("alice", Domain.of("example.com"));
        Mapping mapping = Mapping.alias("alice-alias@example.com");

        rrt.addMapping(source, mapping);
        rrt.addMapping(source, mapping); // Duplicate add should be ignored

        Mappings mappings = rrt.getStoredMappings(source);
        assertThat(mappings.asStrings()).containsExactly(mapping.asString());
    }
}
