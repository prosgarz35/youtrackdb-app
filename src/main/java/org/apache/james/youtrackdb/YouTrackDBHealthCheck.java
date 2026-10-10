package org.apache.james.youtrackdb;

import java.time.Duration;

import jakarta.inject.Inject;

import org.apache.james.core.healthcheck.ComponentName;
import org.apache.james.core.healthcheck.HealthCheck;
import org.apache.james.core.healthcheck.Result;
import org.reactivestreams.Publisher;

import com.jetbrains.youtrackdb.api.YouTrackDB;
import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;

import reactor.core.publisher.Mono;

public class YouTrackDBHealthCheck implements HealthCheck {
    public static final ComponentName COMPONENT_NAME = new ComponentName("YouTrackDB");

    private final YouTrackDB youTrackDB;
    private final YTDBGraphTraversalSource traversalSource;

    @Inject
    public YouTrackDBHealthCheck(YouTrackDB youTrackDB, YTDBGraphTraversalSource traversalSource) {
        this.youTrackDB = youTrackDB;
        this.traversalSource = traversalSource;
    }

    @Override
    public ComponentName componentName() {
        return COMPONENT_NAME;
    }

    @Override
    public Publisher<Result> check() {
        return Mono.fromCallable(() -> {
                if (!youTrackDB.isOpen()) {
                    return Result.unhealthy(COMPONENT_NAME, "YouTrackDB instance is not open");
                }
                traversalSource.computeInTx(tx -> tx.yql("SELECT 1").toList());
                return Result.healthy(COMPONENT_NAME);
            })
            .subscribeOn(YouTrackDBTransactions.virtualThreadScheduler())
            .timeout(Duration.ofSeconds(5))
            .onErrorResume(e -> Mono.just(Result.unhealthy(COMPONENT_NAME, "Failed to query YouTrackDB", e)));
    }
}
