package org.apache.james.youtrackdb;

import jakarta.inject.Inject;

import org.apache.james.utils.GuiceProbe;

import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;

public class YouTrackDBProbe implements GuiceProbe {

    private final YTDBGraphTraversalSource traversalSource;

    @Inject
    public YouTrackDBProbe(YTDBGraphTraversalSource traversalSource) {
        this.traversalSource = traversalSource;
    }

    public YTDBGraphTraversalSource getTraversalSource() {
        return traversalSource;
    }
}
