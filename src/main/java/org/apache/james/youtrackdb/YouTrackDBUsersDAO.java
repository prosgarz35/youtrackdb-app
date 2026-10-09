package org.apache.james.youtrackdb;

import static org.apache.james.user.lib.model.Algorithm.HashingMode.PLAIN;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
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

import java.time.Duration;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.jetbrains.youtrackdb.api.exception.RecordDuplicatedException;
import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;

public class YouTrackDBUsersDAO implements UsersDAO, Configurable {
    private static final String CLASS_NAME = "JamesUser";
    private static final String PROP_USERNAME = "username";
    private static final String PROP_PASSWORD = "password";
    private static final String PROP_ALGO = "algorithm";

    private final YTDBGraphTraversalSource g;
    private volatile Algorithm algo;

    private final Cache<Username, Optional<User>> userCache = Caffeine.newBuilder()
        .maximumSize(10_000)
        .expireAfterAccess(Duration.ofMinutes(15))
        .build();

    public static final String DEFAULT_ALGORITHM = "PBKDF2-SHA512-210000";

    @Inject
    public YouTrackDBUsersDAO(YTDBGraphTraversalSource g) {
        this.g = g;
        this.algo = Algorithm.of(DEFAULT_ALGORITHM);
    }

    @Override
    public void configure(HierarchicalConfiguration<ImmutableNode> config) {
        algo = Algorithm.of(config.getString("algorithm", DEFAULT_ALGORITHM), config.getString("hashingMode", PLAIN.name()));
    }

    @Override
    public void addUser(Username username, String password) throws UsersRepositoryException {
        DefaultUser user = new DefaultUser(username, algo, algo);
        user.setPassword(password);

        try {
            YouTrackDBTransactions.executeStrictTx(g, tx -> tx.addV(CLASS_NAME)
                .property(PROP_USERNAME, username.asString())
                .property(PROP_PASSWORD, user.getHashedPassword())
                .property(PROP_ALGO, user.getHashAlgorithm().asString())
                .iterate());
            userCache.put(username, Optional.of(user));
        } catch (Exception e) {
            if (YouTrackDBTransactions.hasCause(e, RecordDuplicatedException.class)) {
                throw new AlreadyExistInUsersRepositoryException("User " + username.asString() + " already exists");
            }
            throw new UsersRepositoryException("Failed to add user " + username.asString(), e);
        }
    }

    @Override
    public Optional<User> getUserByName(Username name) throws UsersRepositoryException {
        Optional<User> cached = userCache.getIfPresent(name);
        if (cached != null) {
            return cached;
        }

        try {
            List<Map<String, Object>> rows = YouTrackDBTransactions.queryRows(g,
                "SELECT password, algorithm FROM JamesUser WHERE username = :uname LIMIT 1", "uname", name.asString());
            if (rows.isEmpty()) {
                userCache.put(name, Optional.empty());
                return Optional.empty();
            }
            Map<String, Object> row = rows.get(0);
            Object storedPassword = row.get(PROP_PASSWORD);
            Object storedAlgo = row.get(PROP_ALGO);
            Algorithm userAlgo = storedAlgo != null ? Algorithm.of(storedAlgo.toString()) : algo;
            Optional<User> user = Optional.of(new DefaultUser(name, storedPassword != null ? storedPassword.toString() : "", userAlgo, algo));
            userCache.put(name, user);
            return user;
        } catch (Exception e) {
            throw new UsersRepositoryException("Failed to get user " + name.asString(), e);
        }
    }

    @Override
    public void updateUser(User user) throws UsersRepositoryException {
        if (!(user instanceof DefaultUser defaultUser)) {
            throw new UsersRepositoryException("Unsupported user type: " + user.getClass());
        }
        Username username = user.getUserName();

        try {
            YouTrackDBTransactions.executeStrictTx(g, tx -> {
                boolean exists = !YouTrackDBTransactions.queryRows(tx,
                    "SELECT 1 FROM JamesUser WHERE username = :uname LIMIT 1",
                    "uname", username.asString()).isEmpty();
                if (!exists) {
                    throw new UsersRepositoryException("User " + username.asString() + " not found to update");
                }
                tx.command("UPDATE JamesUser SET password = :pwd, algorithm = :algo WHERE username = :uname",
                    "pwd", defaultUser.getHashedPassword(),
                    "algo", defaultUser.getHashAlgorithm().asString(),
                    "uname", username.asString());
            });
            userCache.put(username, Optional.of(user));
        } catch (UsersRepositoryException e) {
            throw e;
        } catch (Exception e) {
            throw new UsersRepositoryException("Failed to update user " + username.asString(), e);
        }
    }

    @Override
    public void removeUser(Username name) throws UsersRepositoryException {
        boolean removed;
        try {
            removed = YouTrackDBTransactions.computeStrictTx(g, tx -> {
                boolean exists = !YouTrackDBTransactions.queryRows(tx,
                    "SELECT 1 FROM JamesUser WHERE username = :uname LIMIT 1",
                    "uname", name.asString()).isEmpty();
                if (!exists) {
                    return false;
                }
                tx.command("DELETE VERTEX JamesUser WHERE username = :uname", "uname", name.asString());
                return true;
            });
        } catch (Exception e) {
            throw new UsersRepositoryException("Failed to remove user " + name.asString(), e);
        }
        if (!removed) {
            throw new UsersRepositoryException("Unable to remove unknown user " + name.asString());
        }
        userCache.invalidate(name);
    }

    @Override
    public boolean contains(Username name) throws UsersRepositoryException {
        Optional<User> cached = userCache.getIfPresent(name);
        if (cached != null) {
            return cached.isPresent();
        }
        return getUserByName(name).isPresent();
    }

    @Override
    public int countUsers() throws UsersRepositoryException {
        try {
            List<Map<String, Object>> rows = YouTrackDBTransactions.queryRows(g, "SELECT count(*) AS total FROM JamesUser");
            if (!rows.isEmpty() && rows.get(0).get("total") instanceof Number total) {
                return Math.toIntExact(total.longValue());
            }
            return 0;
        } catch (Exception e) {
            throw new UsersRepositoryException("Failed to count users", e);
        }
    }

    @Override
    public Iterator<Username> list() throws UsersRepositoryException {
        try {
            List<Map<String, Object>> rows = YouTrackDBTransactions.queryRows(g, "SELECT username FROM JamesUser");
            List<Username> usernames = new ArrayList<>(rows.size());
            for (Map<String, Object> row : rows) {
                Object stored = row.get(PROP_USERNAME);
                if (stored != null) {
                    usernames.add(Username.of(stored.toString()));
                }
            }
            return usernames.iterator();
        } catch (Exception e) {
            throw new UsersRepositoryException("Failed to list users", e);
        }
    }
}
