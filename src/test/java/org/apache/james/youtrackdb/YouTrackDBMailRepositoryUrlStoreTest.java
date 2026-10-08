package org.apache.james.youtrackdb;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;

import org.apache.james.mailrepository.MailRepositoryUrlStoreContract;
import org.apache.james.mailrepository.api.MailRepositoryUrlStore;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.BeforeEachCallback;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.ExtensionContext;
import org.junit.jupiter.api.extension.ParameterContext;
import org.junit.jupiter.api.extension.ParameterResolutionException;
import org.junit.jupiter.api.extension.ParameterResolver;

import com.jetbrains.youtrackdb.api.DatabaseType;
import com.jetbrains.youtrackdb.api.YouTrackDB;
import com.jetbrains.youtrackdb.api.YourTracks;
import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;

@ExtendWith(YouTrackDBMailRepositoryUrlStoreTest.YouTrackDBMailRepositoryUrlStoreExtension.class)
class YouTrackDBMailRepositoryUrlStoreTest implements MailRepositoryUrlStoreContract {

    static class YouTrackDBMailRepositoryUrlStoreExtension implements ParameterResolver, BeforeEachCallback, AfterEachCallback {
        private Path tempDir;
        private YouTrackDB youTrackDB;
        private YTDBGraphTraversalSource g;
        private YouTrackDBMailRepositoryUrlStore store;

        @Override
        public void beforeEach(ExtensionContext context) throws Exception {
            tempDir = Files.createTempDirectory("ytdb-urlstore-test");
            File dbDir = tempDir.resolve("ytdb").toFile();
            dbDir.mkdirs();
            youTrackDB = YourTracks.instance(dbDir.getAbsolutePath());
            youTrackDB.createIfNotExists("test", DatabaseType.DISK, "admin", "admin", "admin");
            g = youTrackDB.openTraversal("test", "admin", "admin");

            g.executeInTx(tx -> {
                tx.command("CREATE CLASS " + YouTrackDBMailRepositoryUrlStore.CLASS + " IF NOT EXISTS EXTENDS V");
                tx.command("CREATE PROPERTY " + YouTrackDBMailRepositoryUrlStore.CLASS + "." + YouTrackDBMailRepositoryUrlStore.PROP_URL + " IF NOT EXISTS STRING");
                tx.command("CREATE INDEX " + YouTrackDBMailRepositoryUrlStore.CLASS + ".url IF NOT EXISTS ON " + YouTrackDBMailRepositoryUrlStore.CLASS + " (url) UNIQUE");
            });

            store = new YouTrackDBMailRepositoryUrlStore(g);
        }

        @Override
        public void afterEach(ExtensionContext context) throws Exception {
            if (g != null) {
                g.close();
            }
            if (youTrackDB != null) {
                youTrackDB.close();
            }
            if (tempDir != null && Files.exists(tempDir)) {
                try (var s = Files.walk(tempDir)) {
                    s.sorted(Comparator.reverseOrder())
                        .map(Path::toFile)
                        .forEach(File::delete);
                } catch (IOException ignored) {
                }
            }
        }

        @Override
        public boolean supportsParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
            return parameterContext.getParameter().getType() == MailRepositoryUrlStore.class;
        }

        @Override
        public Object resolveParameter(ParameterContext parameterContext, ExtensionContext extensionContext) throws ParameterResolutionException {
            return store;
        }
    }
}
