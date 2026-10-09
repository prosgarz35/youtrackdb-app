package org.apache.james;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.nio.file.Path;
import java.util.Optional;

import org.apache.james.core.Username;
import org.apache.james.user.api.AlreadyExistInUsersRepositoryException;
import org.apache.james.user.api.UsersRepositoryException;
import org.apache.james.user.api.model.User;
import org.apache.james.user.lib.model.DefaultUser;
import org.apache.james.youtrackdb.YouTrackDBTransactions;
import org.apache.james.youtrackdb.YouTrackDBUsersDAO;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.jetbrains.youtrackdb.api.DatabaseType;
import com.jetbrains.youtrackdb.api.YouTrackDB;
import com.jetbrains.youtrackdb.api.YourTracks;
import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;

public class YouTrackDBUsersDAOTest {

    @TempDir
    Path tempDir;

    private YouTrackDB ytdb;
    private YTDBGraphTraversalSource g;
    private YouTrackDBUsersDAO usersDAO;

    @BeforeEach
    void setUp() {
        File dbDir = tempDir.resolve("var").resolve("youtrackdb").toFile();
        dbDir.mkdirs();
        ytdb = YourTracks.instance(dbDir.getAbsolutePath());
        ytdb.createIfNotExists("james", DatabaseType.DISK, "admin", "admin", "admin");
        g = ytdb.openTraversal("james", "admin", "admin");

        YouTrackDBTransactions.executeStrictTx(g, tx -> {
            tx.command("CREATE CLASS JamesUser IF NOT EXISTS EXTENDS V");
            tx.command("CREATE PROPERTY JamesUser.username IF NOT EXISTS STRING");
            tx.command("CREATE PROPERTY JamesUser.password IF NOT EXISTS STRING");
            tx.command("CREATE PROPERTY JamesUser.algorithm IF NOT EXISTS STRING");
            tx.command("CREATE INDEX JamesUser.username IF NOT EXISTS ON JamesUser (username) UNIQUE");
        });

        usersDAO = new YouTrackDBUsersDAO(g);
    }

    @AfterEach
    void tearDown() {
        if (g != null) {
            g.close();
        }
        if (ytdb != null) {
            ytdb.close();
        }
    }

    @Test
    void shouldAddAndRetrieveUserWithCache() throws Exception {
        Username username = Username.of("alice");
        usersDAO.addUser(username, "secret123");

        assertThat(usersDAO.contains(username)).isTrue();
        Optional<User> userOpt = usersDAO.getUserByName(username);
        assertThat(userOpt).isPresent();
        assertThat(userOpt.get().verifyPassword("secret123")).isTrue();
        assertThat(userOpt.get().verifyPassword("wrong")).isFalse();

        // Cached hit
        Optional<User> cached = usersDAO.getUserByName(username);
        assertThat(cached).isEqualTo(userOpt);
    }

    @Test
    void shouldFailOnDuplicateUser() throws Exception {
        Username username = Username.of("bob");
        usersDAO.addUser(username, "pass1");

        assertThatThrownBy(() -> usersDAO.addUser(username, "pass2"))
            .isInstanceOf(AlreadyExistInUsersRepositoryException.class);
    }

    @Test
    void shouldUpdateUserAndPasswordAndInvalidateCache() throws Exception {
        Username username = Username.of("charlie");
        usersDAO.addUser(username, "oldpass");

        Optional<User> user = usersDAO.getUserByName(username);
        assertThat(user).isPresent();
        assertThat(user.get().verifyPassword("oldpass")).isTrue();

        DefaultUser defaultUser = (DefaultUser) user.get();
        defaultUser.setPassword("newpass");
        usersDAO.updateUser(defaultUser);

        Optional<User> updated = usersDAO.getUserByName(username);
        assertThat(updated).isPresent();
        assertThat(updated.get().verifyPassword("newpass")).isTrue();
        assertThat(updated.get().verifyPassword("oldpass")).isFalse();
    }

    @Test
    void shouldRemoveUserAndInvalidateCache() throws Exception {
        Username username = Username.of("david");
        usersDAO.addUser(username, "mypass");
        assertThat(usersDAO.contains(username)).isTrue();

        usersDAO.removeUser(username);
        assertThat(usersDAO.contains(username)).isFalse();
        assertThat(usersDAO.getUserByName(username)).isEmpty();
    }
}
