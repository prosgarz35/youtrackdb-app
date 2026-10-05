package org.apache.james.youtrackdb;

import org.apache.james.queue.api.MailQueue;
import org.apache.james.queue.api.MailQueueFactory;
import org.apache.james.queue.api.ManageableMailQueue;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Scopes;
import com.google.inject.Singleton;

public class YouTrackDBMailQueueModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(YouTrackDBMailQueueFactory.class).in(Scopes.SINGLETON);
    }

    @Provides
    @Singleton
    public MailQueueFactory<? extends ManageableMailQueue> provideManageableMailQueueFactory(YouTrackDBMailQueueFactory mailQueueFactory) {
        return mailQueueFactory;
    }

    @Provides
    @Singleton
    public MailQueueFactory<?> provideMailQueueFactory(YouTrackDBMailQueueFactory mailQueueFactory) {
        return mailQueueFactory;
    }

    @Provides
    @Singleton
    public MailQueueFactory<? extends MailQueue> provideMailQueueFactoryGenerics(YouTrackDBMailQueueFactory mailQueueFactory) {
        return mailQueueFactory;
    }
}
