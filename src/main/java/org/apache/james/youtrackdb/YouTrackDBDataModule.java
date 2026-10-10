package org.apache.james.youtrackdb;

import org.apache.commons.configuration2.BaseHierarchicalConfiguration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.james.CoreDataModule;
import org.apache.james.domainlist.api.DomainList;
import org.apache.james.mailrepository.api.MailRepositoryFactory;
import org.apache.james.mailrepository.api.MailRepositoryUrlStore;
import org.apache.james.mailrepository.api.Protocol;
import org.apache.james.mailrepository.memory.MailRepositoryStoreConfiguration;
import org.apache.james.mailrepository.memory.MemoryMailRepository;
import org.apache.james.mailrepository.memory.MemoryMailRepositoryFactory;
import org.apache.james.modules.data.SieveFileRepositoryModule;
import org.apache.james.rrt.api.AliasReverseResolver;
import org.apache.james.rrt.api.CanSendFrom;
import org.apache.james.rrt.api.RecipientRewriteTable;
import org.apache.james.rrt.lib.AliasReverseResolverImpl;
import org.apache.james.rrt.lib.CanSendFromImpl;
import org.apache.james.server.core.configuration.ConfigurationProvider;
import org.apache.james.user.api.UsersRepository;
import org.apache.james.user.lib.UsersDAO;
import org.apache.james.utils.InitializationOperation;
import org.apache.james.utils.InitilizationOperationBuilder;

import com.google.inject.AbstractModule;
import com.google.inject.Scopes;
import com.google.inject.multibindings.Multibinder;
import com.google.inject.multibindings.ProvidesIntoSet;

public class YouTrackDBDataModule extends AbstractModule {

    @Override
    protected void configure() {
        install(new YouTrackDBCommonModule());
        install(new SieveFileRepositoryModule());
        install(new CoreDataModule());

        bind(YouTrackDBDomainList.class).in(Scopes.SINGLETON);
        bind(DomainList.class).to(YouTrackDBDomainList.class);

        bind(YouTrackDBRecipientRewriteTable.class).in(Scopes.SINGLETON);
        bind(RecipientRewriteTable.class).to(YouTrackDBRecipientRewriteTable.class);

        bind(AliasReverseResolverImpl.class).in(Scopes.SINGLETON);
        bind(AliasReverseResolver.class).to(AliasReverseResolverImpl.class);

        bind(CanSendFromImpl.class).in(Scopes.SINGLETON);
        bind(CanSendFrom.class).to(CanSendFromImpl.class);

        bind(YouTrackDBMailRepositoryUrlStore.class).in(Scopes.SINGLETON);
        bind(MailRepositoryUrlStore.class).to(YouTrackDBMailRepositoryUrlStore.class);

        bind(MailRepositoryStoreConfiguration.Item.class)
            .toProvider(() -> new MailRepositoryStoreConfiguration.Item(
                java.util.List.of(new Protocol("memory")),
                MemoryMailRepository.class.getName(),
                new BaseHierarchicalConfiguration()));

        Multibinder.newSetBinder(binder(), MailRepositoryFactory.class)
            .addBinding().to(MemoryMailRepositoryFactory.class);

        bind(YouTrackDBUsersRepository.class).in(Scopes.SINGLETON);
        bind(UsersRepository.class).to(YouTrackDBUsersRepository.class);

        bind(YouTrackDBUsersDAO.class).in(Scopes.SINGLETON);
        bind(UsersDAO.class).to(YouTrackDBUsersDAO.class);
    }

    @ProvidesIntoSet
    InitializationOperation configureUsersRepository(ConfigurationProvider configurationProvider, YouTrackDBUsersRepository usersRepository) {
        return InitilizationOperationBuilder
            .forClass(YouTrackDBUsersRepository.class)
            .init(() -> {
                try {
                    usersRepository.configure(configurationProvider.getConfiguration("usersrepository"));
                } catch (ConfigurationException e) {
                    throw new RuntimeException("Error configuring usersrepository", e);
                }
            });
    }

    @ProvidesIntoSet
    InitializationOperation configureDomainList(ConfigurationProvider configurationProvider, YouTrackDBDomainList domainList) {
        return InitilizationOperationBuilder
            .forClass(YouTrackDBDomainList.class)
            .init(() -> {
                try {
                    domainList.configure(configurationProvider.getConfiguration("domainlist"));
                } catch (ConfigurationException e) {
                    throw new RuntimeException("Error configuring domainlist", e);
                }
            });
    }

    @ProvidesIntoSet
    InitializationOperation configureRecipientRewriteTable(ConfigurationProvider configurationProvider, YouTrackDBRecipientRewriteTable rrt) {
        return InitilizationOperationBuilder
            .forClass(YouTrackDBRecipientRewriteTable.class)
            .init(() -> {
                try {
                    rrt.configure(configurationProvider.getConfiguration("recipientrewritetable"));
                } catch (ConfigurationException e) {
                    throw new RuntimeException("Error configuring recipientrewritetable", e);
                }
            });
    }
}
