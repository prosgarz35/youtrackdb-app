package org.apache.james.youtrackdb;

import jakarta.inject.Inject;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.james.domainlist.api.DomainList;
import org.apache.james.user.lib.UsersRepositoryImpl;

public class YouTrackDBUsersRepository extends UsersRepositoryImpl<YouTrackDBUsersDAO> {
    private final YouTrackDBUsersDAO usersDAO;

    @Inject
    public YouTrackDBUsersRepository(DomainList domainList, YouTrackDBUsersDAO usersDAO) {
        super(domainList, usersDAO);
        this.usersDAO = usersDAO;
    }

    @Override
    public void configure(HierarchicalConfiguration<ImmutableNode> config) throws org.apache.commons.configuration2.ex.ConfigurationException {
        usersDAO.configure(config);
        super.configure(config);
    }
}

