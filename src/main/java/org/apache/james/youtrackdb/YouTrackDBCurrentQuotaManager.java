package org.apache.james.youtrackdb;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import jakarta.inject.Inject;

import org.apache.james.core.quota.QuotaCountUsage;
import org.apache.james.core.quota.QuotaSizeUsage;
import org.apache.james.mailbox.model.CurrentQuotas;
import org.apache.james.mailbox.model.QuotaOperation;
import org.apache.james.mailbox.model.QuotaRoot;
import org.apache.james.mailbox.quota.CurrentQuotaManager;

import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class YouTrackDBCurrentQuotaManager implements CurrentQuotaManager {

    private static final String CLASS_NAME = "JamesQuotaUsage";
    private static final long NO_MESSAGES = 0L;
    private static final long NO_STORED_BYTES = 0L;

    private final YTDBGraphTraversalSource g;

    @Inject
    public YouTrackDBCurrentQuotaManager(YTDBGraphTraversalSource g) {
        this.g = Objects.requireNonNull(g, "g must not be null");
    }

    @Override
    public Mono<QuotaCountUsage> getCurrentMessageCount(QuotaRoot quotaRoot) {
        return Mono.fromCallable(() -> {
            CurrentQuotas quotas = retrieveQuotas(quotaRoot);
            return quotas.count();
        }).subscribeOn(Schedulers.boundedElastic());
    }

    @Override
    public Mono<QuotaSizeUsage> getCurrentStorage(QuotaRoot quotaRoot) {
        return Mono.fromCallable(() -> {
            CurrentQuotas quotas = retrieveQuotas(quotaRoot);
            return quotas.size();
        }).subscribeOn(Schedulers.boundedElastic());
    }

    @Override
    public Mono<CurrentQuotas> getCurrentQuotas(QuotaRoot quotaRoot) {
        return Mono.fromCallable(() -> retrieveQuotas(quotaRoot))
            .subscribeOn(Schedulers.boundedElastic());
    }

    @Override
    public Mono<Void> increase(QuotaOperation quotaOperation) {
        return Mono.fromRunnable(() -> applyOperation(quotaOperation, 1L))
            .subscribeOn(Schedulers.boundedElastic())
            .then();
    }

    @Override
    public Mono<Void> decrease(QuotaOperation quotaOperation) {
        return Mono.fromRunnable(() -> applyOperation(quotaOperation, -1L))
            .subscribeOn(Schedulers.boundedElastic())
            .then();
    }

    @Override
    public Mono<Void> setCurrentQuotas(QuotaOperation quotaOperation) {
        return Mono.fromRunnable(() -> {
            String rootVal = quotaOperation.quotaRoot().getValue();
            long count = quotaOperation.count().asLong();
            long size = quotaOperation.size().asLong();

            try {
                YouTrackDBTransactions.retryOnConflict(10, () -> {
                    YouTrackDBTransactions.executeStrictTx(g, tx -> {
                        List<Map<String, Object>> rows = YouTrackDBTransactions.queryRowsInTx(tx,
                            "SELECT FROM JamesQuotaUsage WHERE quotaRoot = :qr", "qr", rootVal);
                        if (rows.isEmpty()) {
                            tx.addV(CLASS_NAME)
                                .property("quotaRoot", rootVal)
                                .property("messageCount", count)
                                .property("size", size)
                                .iterate();
                        } else {
                            tx.command("UPDATE JamesQuotaUsage SET messageCount = :mc, size = :sz WHERE quotaRoot = :qr",
                                "mc", count, "sz", size, "qr", rootVal);
                        }
                    });
                    return null;
                });
            } catch (Exception e) {
                throw new RuntimeException("Failed to set current quotas for " + rootVal, e);
            }
        }).subscribeOn(Schedulers.boundedElastic()).then();
    }

    private CurrentQuotas retrieveQuotas(QuotaRoot quotaRoot) {
        List<Map<String, Object>> rows = YouTrackDBTransactions.queryRows(g,
            "SELECT messageCount, size FROM JamesQuotaUsage WHERE quotaRoot = :qr",
            "qr", quotaRoot.getValue());
        if (rows.isEmpty()) {
            return CurrentQuotas.emptyQuotas();
        }
        Map<String, Object> row = rows.getFirst();
        Object cntObj = row.get("messageCount");
        Object szObj = row.get("size");
        long count = cntObj instanceof Number ? ((Number) cntObj).longValue() : NO_MESSAGES;
        long size = szObj instanceof Number ? ((Number) szObj).longValue() : NO_STORED_BYTES;
        return new CurrentQuotas(QuotaCountUsage.count(count), QuotaSizeUsage.size(size));
    }

    private void applyOperation(QuotaOperation quotaOperation, long sign) {
        String rootVal = quotaOperation.quotaRoot().getValue();
        long diffCount = quotaOperation.count().asLong() * sign;
        long diffSize = quotaOperation.size().asLong() * sign;

        try {
            YouTrackDBTransactions.retryOnConflict(10, () -> {
                YouTrackDBTransactions.executeStrictTx(g, tx -> {
                    List<Map<String, Object>> rows = YouTrackDBTransactions.queryRowsInTx(tx,
                        "SELECT 1 FROM JamesQuotaUsage WHERE quotaRoot = :qr LIMIT 1", "qr", rootVal);
                    if (rows.isEmpty()) {
                        tx.addV(CLASS_NAME)
                            .property("quotaRoot", rootVal)
                            .property("messageCount", diffCount)
                            .property("size", diffSize)
                            .iterate();
                    } else {
                        tx.command("UPDATE JamesQuotaUsage SET messageCount = messageCount + :dc, size = size + :ds WHERE quotaRoot = :qr",
                            "dc", diffCount, "ds", diffSize, "qr", rootVal);
                    }
                });
                return null;
            });
        } catch (Exception e) {
            throw new RuntimeException("Failed to update quota usage for " + rootVal, e);
        }
    }
}
