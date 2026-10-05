package org.apache.james.youtrackdb;

import static org.apache.james.user.lib.model.Algorithm.HashingMode.PLAIN;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import jakarta.inject.Inject;

import org.apache.commons.configuration2.HierarchicalConfiguration;
import org.apache.commons.configuration2.tree.ImmutableNode;
import org.apache.james.core.Username;
import org.apache.james.lifecycle.api.Configurable;
import org.apache.james.user.api.AlreadyExistInUsersRepositoryException;
import org.apache.james.user.api.UsersRepositoryException;
import org.apache.james.user.api.model.User;
import org.apache.james.user.lib.UsersDAO;
import org.apache.james.user.lib.model.Algorithm;
import org.apache.james.user.lib.model.DefaultUser;

import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.Vertex;

public class YouTrackDBUsersDAO implements UsersDAO, Configurable {
    private static final String CLASS_NAME = "JamesUser";
    private static final String PROP_USERNAME = "username";
    private static final String PROP_PASSWORD = "password";
    private static final String PROP_ALGO = "algorithm";

    private final YTDBGraphTraversalSource g;
    private Algorithm algo;

    @Inject
    public YouTrackDBUsersDAO(YTDBGraphTraversalSource g) {
        this.g = g;
        this.algo = Algorithm.of("PBKDF2");
    }

    @Override
    public void configure(HierarchicalConfiguration<ImmutableNode> config) {
        algo = Algorithm.of(config.getString("algorithm", "PBKDF2"), config.getString("hashingMode", PLAIN.name()));
    }

    @Override
    public void addUser(Username username, String password) throws UsersRepositoryException {
        DefaultUser user = new DefaultUser(username, algo, algo);
        user.setPassword(password);

        try {
            g.executeInTx(tx -> {
                boolean exists = tx.V().hasLabel(CLASS_NAME).has(PROP_USERNAME, username.asString()).hasNext();
                if (exists) {
                    throw new RuntimeException(new AlreadyExistInUsersRepositoryException("User " + username.asString() + " already exists"));
                }
                tx.addV(CLASS_NAME)
                    .property(PROP_USERNAME, username.asString())
                    .property(PROP_PASSWORD, user.getHashedPassword())
                    .property(PROP_ALGO, user.getHashAlgorithm().asString())
                    .iterate();
            });
        } catch (Exception e) {
            if (e.getCause() instanceof UsersRepositoryException) {
                throw (UsersRepositoryException) e.getCause();
            }
            throw new UsersRepositoryException("Failed to add user " + username.asString(), e);
        }
    }

    @Override
    public Optional<User> getUserByName(Username name) throws UsersRepositoryException {
        try {
            return g.computeInTx(tx -> {
                var traversal = tx.V().hasLabel(CLASS_NAME).has(PROP_USERNAME, name.asString());
                if (!traversal.hasNext()) {
                    return Optional.empty();
                }
                Vertex v = traversal.next();
                String pwd = v.value(PROP_PASSWORD);
                String algoStr = v.property(PROP_ALGO).isPresent() ? v.value(PROP_ALGO) : null;
                Algorithm userAlgo = (algoStr != null) ? Algorithm.of(algoStr) : algo;
                return Optional.of((User) new DefaultUser(name, pwd, userAlgo, algo));
            });
        } catch (Exception e) {
            throw new UsersRepositoryException("Failed to get user " + name.asString(), e);
        }
    }

    @Override
    public void updateUser(User user) throws UsersRepositoryException {
        if (!(user instanceof DefaultUser)) {
            throw new UsersRepositoryException("Unsupported user type: " + user.getClass());
        }
        DefaultUser defaultUser = (DefaultUser) user;
        Username username = user.getUserName();

        try {
            g.executeInTx(tx -> {
                var traversal = tx.V().hasLabel(CLASS_NAME).has(PROP_USERNAME, username.asString());
                if (!traversal.hasNext()) {
                    throw new RuntimeException(new UsersRepositoryException("User " + username.asString() + " not found to update"));
                }
                Vertex v = traversal.next();
                v.property(PROP_PASSWORD, defaultUser.getHashedPassword());
                v.property(PROP_ALGO, defaultUser.getHashAlgorithm().asString());
            });
        } catch (Exception e) {
            if (e.getCause() instanceof UsersRepositoryException) {
                throw (UsersRepositoryException) e.getCause();
            }
            throw new UsersRepositoryException("Failed to update user " + username.asString(), e);
        }
    }

    @Override
    public void removeUser(Username name) throws UsersRepositoryException {
        try {
            boolean removed = g.computeInTx(tx -> {
                var traversal = tx.V().hasLabel(CLASS_NAME).has(PROP_USERNAME, name.asString());
                if (traversal.hasNext()) {
                    traversal.next().remove();
                    return true;
                }
                return false;
            });
            if (!removed) {
                throw new UsersRepositoryException("Unable to remove unknown user " + name.asString());
            }
        } catch (Exception e) {
            if (e instanceof UsersRepositoryException) {
                throw (UsersRepositoryException) e;
            }
            throw new UsersRepositoryException("Failed to remove user " + name.asString(), e);
        }
    }

    @Override
    public boolean contains(Username name) throws UsersRepositoryException {
        try {
            return g.computeInTx(tx -> tx.V().hasLabel(CLASS_NAME).has(PROP_USERNAME, name.asString()).hasNext());
        } catch (Exception e) {
            throw new UsersRepositoryException("Failed to check if user exists: " + name.asString(), e);
        }
    }

    @Override
    public int countUsers() throws UsersRepositoryException {
        try {
            Long count = g.computeInTx(tx -> tx.V().hasLabel(CLASS_NAME).count().next());
            return count != null ? count.intValue() : 0;
        } catch (Exception e) {
            throw new UsersRepositoryException("Failed to count users", e);
        }
    }

    @Override
    public Iterator<Username> list() throws UsersRepositoryException {
        try {
            List<Username> result = g.computeInTx(tx -> {
                List<Username> list = new ArrayList<>();
                var traversal = tx.V().hasLabel(CLASS_NAME).<String>values(PROP_USERNAME);
                while (traversal.hasNext()) {
                    list.add(Username.of(traversal.next()));
                }
                return list;
            });
            return result.iterator();
        } catch (Exception e) {
            throw new UsersRepositoryException("Failed to list users", e);
        }
    }
}
