package org.apache.james.youtrackdb;

import java.util.function.Consumer;
import java.util.function.Function;

import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;

/**
 * Utility for executing transactions with strict commit semantics on YouTrackDB.
 * Gremlin's default YTDBTransaction.finishTx swallows exceptions during tx.commit()
 * and merely logs them. Calling tx.tx().commit() explicitly inside the transaction scope
 * ensures any commit failure (constraint violations, WAL/IO errors) propagates immediately.
 */
public final class YouTrackDBTransactions {

    private YouTrackDBTransactions() {
    }

    public static void executeStrictTx(YTDBGraphTraversalSource g, Consumer<YTDBGraphTraversalSource> action) {
        g.executeInTx(tx -> {
            action.accept(tx);
            tx.tx().commit();
        });
    }

    public static <R> R computeStrictTx(YTDBGraphTraversalSource g, Function<YTDBGraphTraversalSource, R> action) {
        return g.computeInTx(tx -> {
            R result = action.apply(tx);
            tx.tx().commit();
            return result;
        });
    }
}
