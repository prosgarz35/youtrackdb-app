package org.apache.james.youtrackdb;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

import jakarta.inject.Inject;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.james.core.Domain;
import org.apache.james.core.quota.QuotaCountLimit;
import org.apache.james.core.quota.QuotaLimitValue;
import org.apache.james.core.quota.QuotaSizeLimit;
import org.apache.james.mailbox.model.Quota;
import org.apache.james.mailbox.model.QuotaRoot;
import org.apache.james.mailbox.quota.MaxQuotaManager;
import org.reactivestreams.Publisher;

import com.google.common.collect.ImmutableMap;
import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;

import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class YouTrackDBPerUserMaxQuotaManager implements MaxQuotaManager {

    private static final String CLASS_NAME = "JamesQuotaLimit";
    private static final String SCOPE_GLOBAL = "GLOBAL";
    private static final String SCOPE_DOMAIN = "DOMAIN";
    private static final String SCOPE_USER = "USER";
    private static final String GLOBAL_KEY = "DEFAULT";
    private static final long INFINITE = -1L;

    private final YTDBGraphTraversalSource g;

    @Inject
    public YouTrackDBPerUserMaxQuotaManager(YTDBGraphTraversalSource g) {
        this.g = Objects.requireNonNull(g, "g must not be null");
    }

    private Mono<Void> blockingRunnable(Runnable action) {
        return Mono.fromRunnable(action).subscribeOn(Schedulers.boundedElastic()).then();
    }

    private <T> Mono<T> blockingSupplier(java.util.function.Supplier<Optional<T>> supplier) {
        return Mono.fromSupplier(supplier).flatMap(Mono::justOrEmpty).subscribeOn(Schedulers.boundedElastic());
    }

    @Override
    public void setMaxStorage(QuotaRoot quotaRoot, QuotaSizeLimit maxStorageQuota) {
        setUserLimit(quotaRoot.getValue(), "maxStorage", quotaValueToLong(Optional.of(maxStorageQuota)));
    }

    @Override
    public Publisher<Void> setMaxStorageReactive(QuotaRoot quotaRoot, QuotaSizeLimit maxStorageQuota) {
        return blockingRunnable(() -> setMaxStorage(quotaRoot, maxStorageQuota));
    }

    @Override
    public void setMaxMessage(QuotaRoot quotaRoot, QuotaCountLimit maxMessageCount) {
        setUserLimit(quotaRoot.getValue(), "maxMessage", quotaValueToLong(Optional.of(maxMessageCount)));
    }

    @Override
    public Publisher<Void> setMaxMessageReactive(QuotaRoot quotaRoot, QuotaCountLimit maxMessageCount) {
        return blockingRunnable(() -> setMaxMessage(quotaRoot, maxMessageCount));
    }

    @Override
    public void setDomainMaxMessage(Domain domain, QuotaCountLimit count) {
        setDomainLimit(domain.asString().toLowerCase(Locale.US), "maxMessage", quotaValueToLong(Optional.of(count)));
    }

    @Override
    public Publisher<Void> setDomainMaxMessageReactive(Domain domain, QuotaCountLimit count) {
        return blockingRunnable(() -> setDomainMaxMessage(domain, count));
    }

    @Override
    public void setDomainMaxStorage(Domain domain, QuotaSizeLimit size) {
        setDomainLimit(domain.asString().toLowerCase(Locale.US), "maxStorage", quotaValueToLong(Optional.of(size)));
    }

    @Override
    public Publisher<Void> setDomainMaxStorageReactive(Domain domain, QuotaSizeLimit size) {
        return blockingRunnable(() -> setDomainMaxStorage(domain, size));
    }

    @Override
    public void removeDomainMaxMessage(Domain domain) {
        setDomainLimit(domain.asString().toLowerCase(Locale.US), "maxMessage", null);
    }

    @Override
    public Publisher<Void> removeDomainMaxMessageReactive(Domain domain) {
        return blockingRunnable(() -> removeDomainMaxMessage(domain));
    }

    @Override
    public void removeDomainMaxStorage(Domain domain) {
        setDomainLimit(domain.asString().toLowerCase(Locale.US), "maxStorage", null);
    }

    @Override
    public Publisher<Void> removeDomainMaxStorageReactive(Domain domain) {
        return blockingRunnable(() -> removeDomainMaxStorage(domain));
    }

    @Override
    public Optional<QuotaCountLimit> getDomainMaxMessage(Domain domain) {
        Long val = getLimit(SCOPE_DOMAIN, domain.asString().toLowerCase(Locale.US), "maxMessage");
        return longToQuotaCount(val);
    }

    @Override
    public Publisher<QuotaCountLimit> getDomainMaxMessageReactive(Domain domain) {
        return blockingSupplier(() -> getDomainMaxMessage(domain));
    }

    @Override
    public Optional<QuotaSizeLimit> getDomainMaxStorage(Domain domain) {
        Long val = getLimit(SCOPE_DOMAIN, domain.asString().toLowerCase(Locale.US), "maxStorage");
        return longToQuotaSize(val);
    }

    @Override
    public Publisher<QuotaSizeLimit> getDomainMaxStorageReactive(Domain domain) {
        return blockingSupplier(() -> getDomainMaxStorage(domain));
    }

    @Override
    public void removeMaxMessage(QuotaRoot quotaRoot) {
        setUserLimit(quotaRoot.getValue(), "maxMessage", null);
    }

    @Override
    public Publisher<Void> removeMaxMessageReactive(QuotaRoot quotaRoot) {
        return blockingRunnable(() -> removeMaxMessage(quotaRoot));
    }

    @Override
    public void setGlobalMaxStorage(QuotaSizeLimit globalMaxStorage) {
        setGlobalLimit("maxStorage", quotaValueToLong(Optional.of(globalMaxStorage)));
    }

    @Override
    public Publisher<Void> setGlobalMaxStorageReactive(QuotaSizeLimit globalMaxStorage) {
        return blockingRunnable(() -> setGlobalMaxStorage(globalMaxStorage));
    }

    @Override
    public void removeGlobalMaxMessage() {
        setGlobalLimit("maxMessage", null);
    }

    @Override
    public Publisher<Void> removeGlobalMaxMessageReactive() {
        return blockingRunnable(this::removeGlobalMaxMessage);
    }

    @Override
    public void setGlobalMaxMessage(QuotaCountLimit globalMaxMessageCount) {
        setGlobalLimit("maxMessage", quotaValueToLong(Optional.of(globalMaxMessageCount)));
    }

    @Override
    public Publisher<Void> setGlobalMaxMessageReactive(QuotaCountLimit globalMaxMessageCount) {
        return blockingRunnable(() -> setGlobalMaxMessage(globalMaxMessageCount));
    }

    @Override
    public Optional<QuotaSizeLimit> getGlobalMaxStorage() {
        Long val = getLimit(SCOPE_GLOBAL, GLOBAL_KEY, "maxStorage");
        return longToQuotaSize(val);
    }

    @Override
    public Publisher<QuotaSizeLimit> getGlobalMaxStorageReactive() {
        return blockingSupplier(this::getGlobalMaxStorage);
    }

    @Override
    public Optional<QuotaCountLimit> getGlobalMaxMessage() {
        Long val = getLimit(SCOPE_GLOBAL, GLOBAL_KEY, "maxMessage");
        return longToQuotaCount(val);
    }

    @Override
    public Publisher<QuotaCountLimit> getGlobalMaxMessageReactive() {
        return blockingSupplier(this::getGlobalMaxMessage);
    }

    @Override
    public Publisher<QuotaDetails> quotaDetailsReactive(QuotaRoot quotaRoot) {
        return Mono.zip(
                Mono.fromCallable(() -> listMaxMessagesDetails(quotaRoot)),
                Mono.fromCallable(() -> listMaxStorageDetails(quotaRoot)))
            .map(tuple -> new QuotaDetails(tuple.getT1(), tuple.getT2()))
            .subscribeOn(Schedulers.boundedElastic());
    }

    @Override
    public Map<Quota.Scope, QuotaCountLimit> listMaxMessagesDetails(QuotaRoot quotaRoot) {
        return Stream.of(
                Pair.of(Quota.Scope.User, longToQuotaCount(getLimit(SCOPE_USER, quotaRoot.getValue(), "maxMessage"))),
                Pair.of(Quota.Scope.Domain, quotaRoot.getDomain().flatMap(this::getDomainMaxMessage)),
                Pair.of(Quota.Scope.Global, getGlobalMaxMessage()))
            .filter(pair -> pair.getValue().isPresent())
            .collect(ImmutableMap.toImmutableMap(Pair::getKey, value -> value.getValue().get()));
    }

    @Override
    public Map<Quota.Scope, QuotaSizeLimit> listMaxStorageDetails(QuotaRoot quotaRoot) {
        return Stream.of(
                Pair.of(Quota.Scope.User, longToQuotaSize(getLimit(SCOPE_USER, quotaRoot.getValue(), "maxStorage"))),
                Pair.of(Quota.Scope.Domain, quotaRoot.getDomain().flatMap(this::getDomainMaxStorage)),
                Pair.of(Quota.Scope.Global, getGlobalMaxStorage()))
            .filter(pair -> pair.getValue().isPresent())
            .collect(ImmutableMap.toImmutableMap(Pair::getKey, value -> value.getValue().get()));
    }

    @Override
    public void removeMaxStorage(QuotaRoot quotaRoot) {
        setUserLimit(quotaRoot.getValue(), "maxStorage", null);
    }

    @Override
    public Publisher<Void> removeMaxStorageReactive(QuotaRoot quotaRoot) {
        return blockingRunnable(() -> removeMaxStorage(quotaRoot));
    }

    @Override
    public void removeGlobalMaxStorage() {
        setGlobalLimit("maxStorage", null);
    }

    @Override
    public Publisher<Void> removeGlobalMaxStorageReactive() {
        return blockingRunnable(this::removeGlobalMaxStorage);
    }

    private void setUserLimit(String key, String field, Long value) {
        setLimit(SCOPE_USER, key, field, value);
    }

    private void setDomainLimit(String key, String field, Long value) {
        setLimit(SCOPE_DOMAIN, key, field, value);
    }

    private void setGlobalLimit(String field, Long value) {
        setLimit(SCOPE_GLOBAL, GLOBAL_KEY, field, value);
    }

    private void setLimit(String scope, String key, String field, Long value) {
        try {
            YouTrackDBTransactions.retryOnConflict(10, () -> {
                YouTrackDBTransactions.executeStrictTx(g, tx -> {
                    List<Map<String, Object>> rows = YouTrackDBTransactions.queryRowsInTx(tx,
                        "SELECT FROM JamesQuotaLimit WHERE scope = :scope AND quotaKey = :key",
                        "scope", scope, "key", key);
                    if (rows.isEmpty()) {
                        if (value != null) {
                            tx.addV(CLASS_NAME)
                                .property("scope", scope)
                                .property("quotaKey", key)
                                .property(field, value)
                                .iterate();
                        }
                    } else {
                        tx.command("UPDATE JamesQuotaLimit SET " + field + " = :val WHERE scope = :scope AND quotaKey = :key",
                            "val", value, "scope", scope, "key", key);
                    }
                });
                return null;
            });
        } catch (Exception e) {
            throw new RuntimeException("Failed to update quota limit " + scope + "/" + key, e);
        }
    }

    private Long getLimit(String scope, String key, String field) {
        List<Map<String, Object>> rows = YouTrackDBTransactions.queryRows(g,
            "SELECT " + field + " AS val FROM JamesQuotaLimit WHERE scope = :scope AND quotaKey = :key",
            "scope", scope, "key", key);
        if (rows.isEmpty()) {
            return null;
        }
        Object obj = rows.get(0).get("val");
        return obj instanceof Number ? ((Number) obj).longValue() : null;
    }

    private Long quotaValueToLong(Optional<? extends QuotaLimitValue<?>> quota) {
        return quota.map(value -> {
            if (value.isUnlimited()) {
                return INFINITE;
            }
            return value.asLong();
        }).orElse(null);
    }

    private Optional<QuotaSizeLimit> longToQuotaSize(Long value) {
        return longToQuotaValue(value, QuotaSizeLimit.unlimited(), QuotaSizeLimit::size);
    }

    private Optional<QuotaCountLimit> longToQuotaCount(Long value) {
        return longToQuotaValue(value, QuotaCountLimit.unlimited(), QuotaCountLimit::count);
    }

    private <T extends QuotaLimitValue<T>> Optional<T> longToQuotaValue(Long value, T infiniteValue, Function<Long, T> quotaFactory) {
        if (value == null) {
            return Optional.empty();
        }
        if (value == INFINITE) {
            return Optional.of(infiniteValue);
        }
        return Optional.of(quotaFactory.apply(value));
    }
}
