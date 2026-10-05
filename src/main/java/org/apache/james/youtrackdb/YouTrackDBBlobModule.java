package org.apache.james.youtrackdb;

import org.apache.james.blob.api.BlobId;
import org.apache.james.blob.api.BlobStore;
import org.apache.james.blob.api.BlobStoreDAO;
import org.apache.james.blob.api.BucketName;
import org.apache.james.blob.api.PlainBlobId;
import org.apache.james.blob.zstd.CompressionConfiguration;
import org.apache.james.blob.zstd.ZstdBlobStoreDAO;
import org.apache.james.metrics.api.MetricFactory;
import org.apache.james.server.blob.deduplication.DeDuplicationBlobStore;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.google.inject.name.Names;

public class YouTrackDBBlobModule extends AbstractModule {
    private static final String YOUTRACKDB_RAW = "youtrackdbRaw";

    @Override
    protected void configure() {
        bind(PlainBlobId.Factory.class).in(Scopes.SINGLETON);
        bind(BlobId.Factory.class).to(PlainBlobId.Factory.class);

        bind(DeDuplicationBlobStore.class).in(Scopes.SINGLETON);
        bind(BlobStore.class).to(DeDuplicationBlobStore.class);

        bind(YouTrackDBBlobStoreDAO.class).in(Scopes.SINGLETON);
        bind(BlobStoreDAO.class).to(YouTrackDBBlobStoreDAO.class);

        bind(BucketName.class)
            .annotatedWith(Names.named(BlobStore.DEFAULT_BUCKET_NAME_QUALIFIER))
            .toInstance(BucketName.DEFAULT);
    }
}
