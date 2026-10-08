package org.apache.james;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.nio.file.Path;
import java.util.Optional;

import org.apache.james.core.quota.QuotaCountUsage;
import org.apache.james.core.quota.QuotaSizeUsage;
import org.apache.james.mailbox.model.CurrentQuotas;
import org.apache.james.mailbox.model.QuotaOperation;
import org.apache.james.mailbox.model.QuotaRoot;
import org.apache.james.youtrackdb.YouTrackDBCurrentQuotaManager;
import org.assertj.core.api.SoftAssertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.github.fge.lambdas.Throwing;
import com.jetbrains.youtrackdb.api.DatabaseType;
import com.jetbrains.youtrackdb.api.YouTrackDB;
import com.jetbrains.youtrackdb.api.YourTracks;
import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;

import reactor.core.publisher.Mono;

public class YouTrackDBCurrentQuotaManagerTest {

    private static final QuotaRoot QUOTA_ROOT = QuotaRoot.quotaRoot("#private&benwa", Optional.empty());
    private static final CurrentQuotas CURRENT_QUOTAS = new CurrentQuotas(QuotaCountUsage.count(10), QuotaSizeUsage.size(100));
    private static final QuotaOperation RESET_QUOTA_OPERATION = new QuotaOperation(QUOTA_ROOT, QuotaCountUsage.count(10), QuotaSizeUsage.size(100));

    @TempDir
    Path tempDir;

    private YouTrackDB ytdb;
    private YTDBGraphTraversalSource g;
    private YouTrackDBCurrentQuotaManager testee;

    @BeforeEach
    void setUp() {
        File dbDir = tempDir.resolve("var").resolve("youtrackdb").toFile();
        dbDir.mkdirs();
        ytdb = YourTracks.instance(dbDir.getAbsolutePath());
        ytdb.createIfNotExists("james", DatabaseType.DISK, "admin", "admin", "admin");
        g = ytdb.openTraversal("james", "admin", "admin");

        g.executeInTx(tx -> {
            tx.command("CREATE CLASS JamesQuotaUsage IF NOT EXISTS EXTENDS V");
            tx.command("CREATE PROPERTY JamesQuotaUsage.quotaRoot IF NOT EXISTS STRING");
            tx.command("CREATE PROPERTY JamesQuotaUsage.messageCount IF NOT EXISTS LONG");
            tx.command("CREATE PROPERTY JamesQuotaUsage.size IF NOT EXISTS LONG");
            tx.command("CREATE INDEX JamesQuotaUsage.quotaRoot IF NOT EXISTS ON JamesQuotaUsage (quotaRoot) UNIQUE");
            tx.tx().commit();
        });

        testee = new YouTrackDBCurrentQuotaManager(g);
    }

    @AfterEach
    void tearDown() {
        if (g != null) {
            g.close();
        }
        if (ytdb != null && ytdb.isOpen()) {
            ytdb.close();
        }
    }

    @Test
    void getCurrentStorageShouldReturnZeroByDefault() {
        assertThat(Mono.from(testee.getCurrentStorage(QUOTA_ROOT)).block()).isEqualTo(QuotaSizeUsage.size(0));
    }

    @Test
    void getCurrentMessageCountShouldReturnZeroByDefault() {
        assertThat(Mono.from(testee.getCurrentMessageCount(QUOTA_ROOT)).block()).isEqualTo(QuotaCountUsage.count(0));
    }

    @Test
    void getCurrentQuotasShouldReturnZeroByDefault() {
        assertThat(Mono.from(testee.getCurrentQuotas(QUOTA_ROOT)).block()).isEqualTo(CurrentQuotas.emptyQuotas());
    }

    @Test
    void increaseShouldWork() {
        Mono.from(testee.increase(new QuotaOperation(QUOTA_ROOT, QuotaCountUsage.count(10), QuotaSizeUsage.size(100)))).block();

        SoftAssertions.assertSoftly(Throwing.consumer(softly -> {
            softly.assertThat(Mono.from(testee.getCurrentQuotas(QUOTA_ROOT)).block()).isEqualTo(CURRENT_QUOTAS);
            softly.assertThat(Mono.from(testee.getCurrentMessageCount(QUOTA_ROOT)).block()).isEqualTo(QuotaCountUsage.count(10));
            softly.assertThat(Mono.from(testee.getCurrentStorage(QUOTA_ROOT)).block()).isEqualTo(QuotaSizeUsage.size(100));
        }));
    }

    @Test
    void decreaseShouldWork() {
        Mono.from(testee.increase(new QuotaOperation(QUOTA_ROOT, QuotaCountUsage.count(20), QuotaSizeUsage.size(200)))).block();

        Mono.from(testee.decrease(new QuotaOperation(QUOTA_ROOT, QuotaCountUsage.count(10), QuotaSizeUsage.size(100)))).block();

        SoftAssertions.assertSoftly(Throwing.consumer(softly -> {
            softly.assertThat(Mono.from(testee.getCurrentQuotas(QUOTA_ROOT)).block()).isEqualTo(CURRENT_QUOTAS);
            softly.assertThat(Mono.from(testee.getCurrentMessageCount(QUOTA_ROOT)).block()).isEqualTo(QuotaCountUsage.count(10));
            softly.assertThat(Mono.from(testee.getCurrentStorage(QUOTA_ROOT)).block()).isEqualTo(QuotaSizeUsage.size(100));
        }));
    }

    @Test
    void decreaseShouldNotFailWhenItLeadsToNegativeValues() {
        Mono.from(testee.decrease(new QuotaOperation(QUOTA_ROOT, QuotaCountUsage.count(10), QuotaSizeUsage.size(100)))).block();

        SoftAssertions.assertSoftly(Throwing.consumer(softly -> {
            softly.assertThat(Mono.from(testee.getCurrentQuotas(QUOTA_ROOT)).block())
                .isEqualTo(new CurrentQuotas(QuotaCountUsage.count(-10), QuotaSizeUsage.size(-100)));
            softly.assertThat(Mono.from(testee.getCurrentMessageCount(QUOTA_ROOT)).block()).isEqualTo(QuotaCountUsage.count(-10));
            softly.assertThat(Mono.from(testee.getCurrentStorage(QUOTA_ROOT)).block()).isEqualTo(QuotaSizeUsage.size(-100));
        }));
    }

    @Test
    void setCurrentQuotasShouldNoopWhenZeroAndNoData() {
        QuotaOperation quotaOperation = new QuotaOperation(QUOTA_ROOT, QuotaCountUsage.count(0), QuotaSizeUsage.size(0));

        Mono.from(testee.setCurrentQuotas(quotaOperation)).block();

        assertThat(Mono.from(testee.getCurrentQuotas(QUOTA_ROOT)).block())
            .isEqualTo(CurrentQuotas.emptyQuotas());
    }

    @Test
    void setCurrentQuotasShouldReInitQuotasWhenNothing() {
        Mono.from(testee.setCurrentQuotas(RESET_QUOTA_OPERATION)).block();

        assertThat(Mono.from(testee.getCurrentQuotas(QUOTA_ROOT)).block())
            .isEqualTo(CURRENT_QUOTAS);
    }

    @Test
    void setCurrentQuotasShouldReInitQuotasWhenData() {
        Mono.from(testee.increase(new QuotaOperation(QUOTA_ROOT, QuotaCountUsage.count(20), QuotaSizeUsage.size(200)))).block();

        Mono.from(testee.setCurrentQuotas(RESET_QUOTA_OPERATION)).block();

        assertThat(Mono.from(testee.getCurrentQuotas(QUOTA_ROOT)).block())
            .isEqualTo(CURRENT_QUOTAS);
    }

    @Test
    void setCurrentQuotasShouldBeIdempotent() {
        Mono.from(testee.increase(new QuotaOperation(QUOTA_ROOT, QuotaCountUsage.count(20), QuotaSizeUsage.size(200)))).block();

        Mono.from(testee.setCurrentQuotas(RESET_QUOTA_OPERATION)).block();
        Mono.from(testee.setCurrentQuotas(RESET_QUOTA_OPERATION)).block();

        assertThat(Mono.from(testee.getCurrentQuotas(QUOTA_ROOT)).block())
            .isEqualTo(CURRENT_QUOTAS);
    }

    @Test
    void setCurrentQuotasShouldTolerateNegativeValues() {
        Mono.from(testee.decrease(new QuotaOperation(QUOTA_ROOT, QuotaCountUsage.count(20), QuotaSizeUsage.size(200)))).block();

        Mono.from(testee.setCurrentQuotas(RESET_QUOTA_OPERATION)).block();

        assertThat(Mono.from(testee.getCurrentQuotas(QUOTA_ROOT)).block())
            .isEqualTo(CURRENT_QUOTAS);
    }
}
