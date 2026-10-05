package org.apache.james.youtrackdb;

import jakarta.inject.Inject;

import org.apache.james.domainlist.api.DomainList;
import org.apache.james.user.lib.UsersRepositoryImpl;

public class YouTrackDBUsersRepository extends UsersRepositoryImpl<YouTrackDBUsersDAO> {
    @Inject
    public YouTrackDBUsersRepository(DomainList domainList, YouTrackDBUsersDAO usersDAO) {
        super(domainList, usersDAO);
    }
}
