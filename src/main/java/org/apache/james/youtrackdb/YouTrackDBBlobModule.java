package org.apache.james.youtrackdb;

import org.apache.james.blob.api.BlobId;
import org.apache.james.blob.api.BlobStore;
import org.apache.james.blob.api.BlobStoreDAO;
import org.apache.james.blob.api.BucketName;
import org.apache.james.blob.api.PlainBlobId;
import org.apache.james.server.blob.deduplication.DeDuplicationBlobStore;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.google.inject.name.Names;

public class YouTrackDBBlobModule extends AbstractModule {
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
