package org.apache.james.youtrackdb;

import jakarta.inject.Inject;

import org.apache.james.mailbox.quota.MaxQuotaManager;
import org.apache.james.utils.GuiceProbe;

import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;

public class YouTrackDBProbe implements GuiceProbe {

    private final YTDBGraphTraversalSource traversalSource;
    private final MaxQuotaManager maxQuotaManager;

    @Inject
    public YouTrackDBProbe(YTDBGraphTraversalSource traversalSource, MaxQuotaManager maxQuotaManager) {
        this.traversalSource = traversalSource;
        this.maxQuotaManager = maxQuotaManager;
    }

    public YTDBGraphTraversalSource getTraversalSource() {
        return traversalSource;
    }

    public MaxQuotaManager getMaxQuotaManager() {
        return maxQuotaManager;
    }
}
