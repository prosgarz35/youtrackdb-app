package org.apache.james.youtrackdb;

import jakarta.inject.Inject;

import org.apache.james.mailrepository.api.MailRepository;
import org.apache.james.mailrepository.api.MailRepositoryLoader;
import org.apache.james.mailrepository.api.MailRepositoryStore.MailRepositoryStoreException;
import org.apache.james.mailrepository.api.MailRepositoryUrl;
import org.apache.james.modules.mailrepository.guice.GuiceMailRepositoryLoader;

/**
 * The blob mail repository has no injectable constructor: it is built by a factory. Everything else (the memory
 * repository) is still instantiated by Guice, as before.
 */
public class YouTrackDBMailRepositoryLoader implements MailRepositoryLoader {
    private final GuiceMailRepositoryLoader guiceLoader;
    private final YouTrackDBBlobMailRepositoryFactory blobFactory;

    @Inject
    public YouTrackDBMailRepositoryLoader(GuiceMailRepositoryLoader guiceLoader, YouTrackDBBlobMailRepositoryFactory blobFactory) {
        this.guiceLoader = guiceLoader;
        this.blobFactory = blobFactory;
    }

    @Override
    public MailRepository load(String fullyQualifiedClassName, MailRepositoryUrl url) throws MailRepositoryStoreException {
        if (blobFactory.mailRepositoryClass().getName().equals(fullyQualifiedClassName)) {
            return blobFactory.create(url);
        }
        return guiceLoader.load(fullyQualifiedClassName, url);
    }
}
