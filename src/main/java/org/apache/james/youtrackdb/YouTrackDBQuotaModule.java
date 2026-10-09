package org.apache.james.youtrackdb;

import org.apache.james.events.EventListener;
import org.apache.james.mailbox.quota.CurrentQuotaManager;
import org.apache.james.mailbox.quota.MaxQuotaManager;
import org.apache.james.mailbox.quota.QuotaManager;
import org.apache.james.mailbox.quota.QuotaRootDeserializer;
import org.apache.james.mailbox.quota.QuotaRootResolver;
import org.apache.james.mailbox.quota.UserQuotaRootResolver;
import org.apache.james.mailbox.store.quota.DefaultUserQuotaRootResolver;
import org.apache.james.mailbox.store.quota.ListeningCurrentQuotaUpdater;
import org.apache.james.mailbox.store.quota.QuotaUpdater;
import org.apache.james.mailbox.store.quota.StoreQuotaManager;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;

public class YouTrackDBQuotaModule extends AbstractModule {

    @Override
    protected void configure() {
        bind(DefaultUserQuotaRootResolver.class).in(Scopes.SINGLETON);
        bind(YouTrackDBPerUserMaxQuotaManager.class).in(Scopes.SINGLETON);
        bind(StoreQuotaManager.class).in(Scopes.SINGLETON);
        bind(YouTrackDBCurrentQuotaManager.class).in(Scopes.SINGLETON);

        bind(UserQuotaRootResolver.class).to(DefaultUserQuotaRootResolver.class);
        bind(QuotaRootResolver.class).to(DefaultUserQuotaRootResolver.class);
        bind(QuotaRootDeserializer.class).to(DefaultUserQuotaRootResolver.class);
        bind(MaxQuotaManager.class).to(YouTrackDBPerUserMaxQuotaManager.class);
        bind(QuotaManager.class).to(StoreQuotaManager.class);
        bind(CurrentQuotaManager.class).to(YouTrackDBCurrentQuotaManager.class);

        bind(ListeningCurrentQuotaUpdater.class).in(Scopes.SINGLETON);
        bind(QuotaUpdater.class).to(ListeningCurrentQuotaUpdater.class);
        Multibinder.newSetBinder(binder(), EventListener.ReactiveGroupEventListener.class)
            .addBinding()
            .to(ListeningCurrentQuotaUpdater.class);

        bind(org.apache.james.mailbox.store.quota.CurrentQuotaCalculator.class).in(Scopes.SINGLETON);
        Multibinder.newSetBinder(binder(), org.apache.james.mailbox.quota.task.RecomputeSingleComponentCurrentQuotasService.class)
            .addBinding()
            .to(org.apache.james.mailbox.quota.task.RecomputeMailboxCurrentQuotasService.class);
    }
}
