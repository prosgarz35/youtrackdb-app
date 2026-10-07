package org.apache.james.youtrackdb;

import org.apache.james.mailrepository.api.MailRepositoryLoader;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;

/**
 * Installed with {@code Modules.override(new MailetProcessingModule()).with(...)}: it replaces only the
 * MailRepositoryLoader binding of MailStoreRepositoryModule.
 */
public class YouTrackDBMailRepositoryModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(YouTrackDBBlobMailRepositoryFactory.class).in(Scopes.SINGLETON);
        bind(YouTrackDBMailRepositoryLoader.class).in(Scopes.SINGLETON);
        bind(MailRepositoryLoader.class).to(YouTrackDBMailRepositoryLoader.class);
    }
}
