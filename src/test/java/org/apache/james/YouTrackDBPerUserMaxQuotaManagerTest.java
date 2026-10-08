package org.apache.james;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.nio.file.Path;
import java.util.Optional;

import org.apache.james.core.Domain;
import org.apache.james.core.quota.QuotaCountLimit;
import org.apache.james.core.quota.QuotaSizeLimit;
import org.apache.james.mailbox.model.Quota;
import org.apache.james.mailbox.model.QuotaRoot;
import org.apache.james.mailbox.quota.MaxQuotaManager;
import org.apache.james.youtrackdb.YouTrackDBPerUserMaxQuotaManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.jetbrains.youtrackdb.api.DatabaseType;
import com.jetbrains.youtrackdb.api.YouTrackDB;
import com.jetbrains.youtrackdb.api.YourTracks;
import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;

public class YouTrackDBPerUserMaxQuotaManagerTest {

    private static final Domain DOMAIN = Domain.of("domain");
    private static final Domain DOMAIN_CASE_VARIATION = Domain.of("doMain");
    private static final QuotaRoot QUOTA_ROOT = QuotaRoot.quotaRoot("benwa@domain", Optional.of(DOMAIN));

    @TempDir
    Path tempDir;

    private YouTrackDB ytdb;
    private YTDBGraphTraversalSource g;
    private MaxQuotaManager maxQuotaManager;

    @BeforeEach
    void setUp() {
        File dbDir = tempDir.resolve("var").resolve("youtrackdb").toFile();
        dbDir.mkdirs();
        ytdb = YourTracks.instance(dbDir.getAbsolutePath());
        ytdb.createIfNotExists("james", DatabaseType.DISK, "admin", "admin", "admin");
        g = ytdb.openTraversal("james", "admin", "admin");

        g.executeInTx(tx -> {
            tx.command("CREATE CLASS JamesQuotaLimit IF NOT EXISTS EXTENDS V");
            tx.command("CREATE PROPERTY JamesQuotaLimit.scope IF NOT EXISTS STRING");
            tx.command("CREATE PROPERTY JamesQuotaLimit.quotaKey IF NOT EXISTS STRING");
            tx.command("CREATE PROPERTY JamesQuotaLimit.maxStorage IF NOT EXISTS LONG");
            tx.command("CREATE PROPERTY JamesQuotaLimit.maxMessage IF NOT EXISTS LONG");
            tx.command("CREATE INDEX JamesQuotaLimit.scopeAndKey IF NOT EXISTS ON JamesQuotaLimit (scope, quotaKey) UNIQUE");
            tx.tx().commit();
        });

        maxQuotaManager = new YouTrackDBPerUserMaxQuotaManager(g);
    }

    @AfterEach
    void tearDown() {
        if (g != null) {
            try {
                g.close();
            } catch (Exception ignored) {
            }
        }
        if (ytdb != null) {
            try {
                ytdb.close();
            } catch (Exception ignored) {
            }
        }
    }

    @Test
    void getMaxMessageShouldReturnEmptyWhenNoGlobalValue() throws Exception {
        assertThat(maxQuotaManager.getMaxMessage(QUOTA_ROOT)).isEmpty();
    }

    @Test
    void getMaxStorageShouldReturnEmptyWhenNoGlobalValue() throws Exception {
        assertThat(maxQuotaManager.getMaxStorage(QUOTA_ROOT)).isEmpty();
    }

    @Test
    void getMaxMessageShouldReturnDomainWhenNoUserValue() throws Exception {
        maxQuotaManager.setGlobalMaxMessage(QuotaCountLimit.count(36));
        maxQuotaManager.setDomainMaxMessage(DOMAIN, QuotaCountLimit.count(23));
        assertThat(maxQuotaManager.getMaxMessage(QUOTA_ROOT)).contains(QuotaCountLimit.count(23));
    }

    @Test
    void getMaxMessageShouldReturnGlobalWhenNoDomainOrUserValue() throws Exception {
        maxQuotaManager.setGlobalMaxMessage(QuotaCountLimit.count(36));
        assertThat(maxQuotaManager.getMaxMessage(QUOTA_ROOT)).contains(QuotaCountLimit.count(36));
    }

    @Test
    void getMaxStorageShouldReturnGlobalWhenNoDomainOrUserValue() throws Exception {
        maxQuotaManager.setGlobalMaxStorage(QuotaSizeLimit.size(36));
        assertThat(maxQuotaManager.getMaxStorage(QUOTA_ROOT)).contains(QuotaSizeLimit.size(36));
    }

    @Test
    void getMaxStorageShouldReturnDomainWhenNoUserValue() throws Exception {
        maxQuotaManager.setGlobalMaxStorage(QuotaSizeLimit.size(234));
        maxQuotaManager.setDomainMaxStorage(DOMAIN, QuotaSizeLimit.size(111));
        assertThat(maxQuotaManager.getMaxStorage(QUOTA_ROOT)).contains(QuotaSizeLimit.size(111));
    }

    @Test
    void getMaxMessageShouldReturnProvidedUserValuePriority() throws Exception {
        maxQuotaManager.setGlobalMaxMessage(QuotaCountLimit.count(500));
        maxQuotaManager.setDomainMaxMessage(DOMAIN, QuotaCountLimit.count(250));
        maxQuotaManager.setMaxMessage(QUOTA_ROOT, QuotaCountLimit.count(100));

        // Priority User > Domain > Global
        assertThat(maxQuotaManager.getMaxMessage(QUOTA_ROOT)).contains(QuotaCountLimit.count(100));

        // Remove user limit -> falls back to domain limit (250)
        maxQuotaManager.removeMaxMessage(QUOTA_ROOT);
        assertThat(maxQuotaManager.getMaxMessage(QUOTA_ROOT)).contains(QuotaCountLimit.count(250));

        // Remove domain limit -> falls back to global limit (500)
        maxQuotaManager.removeDomainMaxMessage(DOMAIN);
        assertThat(maxQuotaManager.getMaxMessage(QUOTA_ROOT)).contains(QuotaCountLimit.count(500));

        // Remove global limit -> empty
        maxQuotaManager.removeGlobalMaxMessage();
        assertThat(maxQuotaManager.getMaxMessage(QUOTA_ROOT)).isEmpty();
    }

    @Test
    void getMaxStorageShouldReturnProvidedUserValuePriority() throws Exception {
        maxQuotaManager.setGlobalMaxStorage(QuotaSizeLimit.size(5000000));
        maxQuotaManager.setDomainMaxStorage(DOMAIN, QuotaSizeLimit.size(2500000));
        maxQuotaManager.setMaxStorage(QUOTA_ROOT, QuotaSizeLimit.size(1024000));

        // Priority User > Domain > Global
        assertThat(maxQuotaManager.getMaxStorage(QUOTA_ROOT)).contains(QuotaSizeLimit.size(1024000));

        // Remove user limit -> falls back to domain limit (2500000)
        maxQuotaManager.removeMaxStorage(QUOTA_ROOT);
        assertThat(maxQuotaManager.getMaxStorage(QUOTA_ROOT)).contains(QuotaSizeLimit.size(2500000));

        // Remove domain limit -> falls back to global limit (5000000)
        maxQuotaManager.removeDomainMaxStorage(DOMAIN);
        assertThat(maxQuotaManager.getMaxStorage(QUOTA_ROOT)).contains(QuotaSizeLimit.size(5000000));

        // Remove global limit -> empty
        maxQuotaManager.removeGlobalMaxStorage();
        assertThat(maxQuotaManager.getMaxStorage(QUOTA_ROOT)).isEmpty();
    }

    @Test
    void listMaxMessagesDetailsShouldReturnAllValuesWhenDefined() throws Exception {
        maxQuotaManager.setGlobalMaxMessage(QuotaCountLimit.count(1234));
        maxQuotaManager.setDomainMaxMessage(DOMAIN, QuotaCountLimit.count(333));
        maxQuotaManager.setMaxMessage(QUOTA_ROOT, QuotaCountLimit.count(123));
        assertThat(maxQuotaManager.listMaxMessagesDetails(QUOTA_ROOT))
            .hasSize(3)
            .containsEntry(Quota.Scope.Global, QuotaCountLimit.count(1234))
            .containsEntry(Quota.Scope.Domain, QuotaCountLimit.count(333))
            .containsEntry(Quota.Scope.User, QuotaCountLimit.count(123));
    }

    @Test
    void listMaxStorageDetailsShouldReturnAllValuesWhenDefined() throws Exception {
        maxQuotaManager.setGlobalMaxStorage(QuotaSizeLimit.size(3333));
        maxQuotaManager.setDomainMaxStorage(DOMAIN, QuotaSizeLimit.size(2222));
        maxQuotaManager.setMaxStorage(QUOTA_ROOT, QuotaSizeLimit.size(4444));
        assertThat(maxQuotaManager.listMaxStorageDetails(QUOTA_ROOT))
            .hasSize(3)
            .containsEntry(Quota.Scope.Global, QuotaSizeLimit.size(3333))
            .containsEntry(Quota.Scope.Domain, QuotaSizeLimit.size(2222))
            .containsEntry(Quota.Scope.User, QuotaSizeLimit.size(4444));
    }

    @Test
    void domainMethodsShouldNotBeCaseSensitive() throws Exception {
        maxQuotaManager.setDomainMaxMessage(DOMAIN_CASE_VARIATION, QuotaCountLimit.count(36));
        assertThat(maxQuotaManager.getDomainMaxMessage(DOMAIN)).contains(QuotaCountLimit.count(36));

        maxQuotaManager.setDomainMaxStorage(DOMAIN_CASE_VARIATION, QuotaSizeLimit.size(3600));
        assertThat(maxQuotaManager.getDomainMaxStorage(DOMAIN)).contains(QuotaSizeLimit.size(3600));

        maxQuotaManager.removeDomainMaxMessage(DOMAIN_CASE_VARIATION);
        assertThat(maxQuotaManager.getDomainMaxMessage(DOMAIN)).isEmpty();

        maxQuotaManager.removeDomainMaxStorage(DOMAIN_CASE_VARIATION);
        assertThat(maxQuotaManager.getDomainMaxStorage(DOMAIN)).isEmpty();
    }
}
