package org.apache.james;

import static io.restassured.RestAssured.given;
import static io.restassured.RestAssured.when;
import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.concurrent.TimeUnit;

import org.apache.james.modules.protocols.ImapGuiceProbe;
import org.apache.james.modules.protocols.SmtpGuiceProbe;
import org.apache.james.utils.DataProbeImpl;
import org.apache.james.utils.SMTPMessageSender;
import org.apache.james.utils.TestIMAPClient;
import org.apache.james.utils.WebAdminGuiceProbe;
import org.apache.james.webadmin.WebAdminUtils;
import org.apache.james.youtrackdb.YouTrackDBJamesConfiguration;
import org.apache.james.youtrackdb.YouTrackDBJamesServerMain;
import org.awaitility.Awaitility;
import org.eclipse.jetty.http.HttpStatus;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jetbrains.youtrackdb.api.YouTrackDB;
import com.jetbrains.youtrackdb.api.YourTracks;
import io.restassured.RestAssured;

public class YouTrackDBBackupRestoreTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(YouTrackDBBackupRestoreTest.class);

    private static final String DOMAIN = "backup-test.local";
    private static final String USER = "user@" + DOMAIN;
    private static final String PASSWORD = "password123";
    private static final int INITIAL_MESSAGES = 5;

    @Test
    void shouldBackupDeleteAndRestoreSuccessfully(@TempDir Path workingDir) throws Exception {
        YouTrackDBJamesConfiguration config = YouTrackDBJamesConfiguration.builder()
            .workingDirectory(workingDir.toFile())
            .configurationFromClasspath()
            .build();

        GuiceJamesServer server = YouTrackDBJamesServerMain.createServer(config);
        server.start();

        int initialDomains = 0;
        int expectedTotal = INITIAL_MESSAGES + 1;
        File backupDir = workingDir.resolve("backups").toFile();
        backupDir.mkdirs();

        try {
            // 1. Setup Domain & User
            server.getProbe(DataProbeImpl.class)
                .fluent()
                .addDomain(DOMAIN)
                .addUser(USER, PASSWORD);

            org.apache.james.util.Port smtpPort = server.getProbe(SmtpGuiceProbe.class).getSmtpPort();
            int imapPort = server.getProbe(ImapGuiceProbe.class).getImapPort();
            WebAdminGuiceProbe webAdminGuiceProbe = server.getProbe(WebAdminGuiceProbe.class);
            RestAssured.requestSpecification = WebAdminUtils.buildRequestSpecification(webAdminGuiceProbe.getWebAdminPort()).build();

            // 2. Inject INITIAL_MESSAGES via SMTP (including large message > 64 KB to test Tier 3 sharded filesystem blob)
            SMTPMessageSender smtpSender = new SMTPMessageSender(DOMAIN);
            for (int i = 1; i <= INITIAL_MESSAGES; i++) {
                smtpSender.connect("127.0.0.1", smtpPort)
                    .authenticate(USER, PASSWORD)
                    .sendMessageWithHeaders(USER, USER, "Subject: Msg " + i + "\r\n\r\nBody " + i);
            }

            // Inject large message > 64 KB (75 KB) with wrapped lines (RFC 2821 line length <= 998 chars) to trigger sharded filesystem blob storage
            StringBuilder largeBody = new StringBuilder(75 * 1024);
            for (int line = 0; line < 1000; line++) {
                largeBody.append("X".repeat(75)).append("\r\n");
            }
            smtpSender.connect("127.0.0.1", smtpPort)
                .authenticate(USER, PASSWORD)
                .sendMessageWithHeaders(USER, USER, "Subject: Large Sharded Msg\r\n\r\n" + largeBody);

            // Await delivery via IMAP
            TestIMAPClient imapClient = new TestIMAPClient();
            Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .pollInterval(500, TimeUnit.MILLISECONDS)
                .until(() -> {
                    try {
                        imapClient.connect("127.0.0.1", imapPort)
                            .login(USER, PASSWORD)
                            .select(TestIMAPClient.INBOX);
                        boolean ok = imapClient.getMessageCount(TestIMAPClient.INBOX) == expectedTotal;
                        imapClient.disconnect();
                        return ok;
                    } catch (Exception e) {
                        return false;
                    }
                });

            LOGGER.info("All {} messages (including large sharded message) delivered to INBOX", expectedTotal);

            // 3. Online Hot Backup via WebAdmin (as async Task)
            String taskId = given()
                .queryParam("backupDir", backupDir.getAbsolutePath())
            .when()
                .post("/youtrackdb/backup")
            .then()
                .statusCode(HttpStatus.CREATED_201)
                .extract()
                .jsonPath()
                .getString("taskId");

            when()
                .get("/tasks/" + taskId + "/await")
            .then()
                .statusCode(HttpStatus.OK_200)
                .body("status", org.hamcrest.Matchers.equalTo("completed"));

            File[] backupFiles = backupDir.listFiles();
            assertThat(backupFiles).isNotNull().isNotEmpty();
            assertThat(backupDir.toPath().resolve("blobs")).exists();
            LOGGER.info("YouTrackDB backup (with blobs) successfully generated at {}", backupDir.getAbsolutePath());

            // 4. Verify initial domains and users count via WebAdmin
            int initialUsers = when()
                .get("/youtrackdb/check")
            .then()
                .statusCode(HttpStatus.OK_200)
                .body("status", org.hamcrest.Matchers.equalTo("HEALTHY"))
                .extract()
                .jsonPath()
                .getInt("totalUsers");

            initialDomains = when()
                .get("/youtrackdb/check")
            .then()
                .statusCode(HttpStatus.OK_200)
                .extract()
                .jsonPath()
                .getInt("totalDomains");

            assertThat(initialUsers).isEqualTo(1);
            assertThat(initialDomains).isGreaterThanOrEqualTo(1);
            LOGGER.info("Initial counts before deletion: users={}, domains={}", initialUsers, initialDomains);

            // Delete user via WebAdmin
            when()
                .delete("/users/" + USER)
            .then()
                .statusCode(HttpStatus.NO_CONTENT_204);

            int usersAfterDeletion = when()
                .get("/youtrackdb/check")
            .then()
                .statusCode(HttpStatus.OK_200)
                .extract()
                .jsonPath()
                .getInt("totalUsers");

            assertThat(usersAfterDeletion).isEqualTo(0);
            LOGGER.info("User {} deleted. Remaining users in YouTrackDB: {}", USER, usersAfterDeletion);

        } finally {
            server.stop();
        }

        // 5. Disaster recovery: Wipe both live database directory and live blobs directory
        File ytdbDir = workingDir.resolve("var").resolve("youtrackdb").toFile();
        File liveBlobsDir = workingDir.resolve("var").resolve("blobs").toFile();
        assertThat(ytdbDir).exists();

        // Clear live database directory and live blobs directory
        deleteDirectoryRecursively(ytdbDir.toPath());
        deleteDirectoryRecursively(liveBlobsDir.toPath());
        ytdbDir.mkdirs();

        // Perform restore via YouTrackDB API into james database
        try (YouTrackDB youTrackDB = YourTracks.instance(ytdbDir.getAbsolutePath())) {
            youTrackDB.restore("james", backupDir.getAbsolutePath());
        }

        // Restore filesystem blobs from backup
        Path backupBlobsDir = backupDir.toPath().resolve("blobs");
        if (Files.exists(backupBlobsDir)) {
            copyDirectoryRecursively(backupBlobsDir, liveBlobsDir.toPath());
        }
        LOGGER.info("Restored YouTrackDB backup and filesystem blobs from {} into {}", backupDir.getAbsolutePath(), workingDir.toAbsolutePath());

        // 6. Restart Server with restored database and blobs
        GuiceJamesServer restoredServer = YouTrackDBJamesServerMain.createServer(config);
        restoredServer.start();

        try {
            WebAdminGuiceProbe restoredWebAdminProbe = restoredServer.getProbe(WebAdminGuiceProbe.class);
            RestAssured.requestSpecification = WebAdminUtils.buildRequestSpecification(restoredWebAdminProbe.getWebAdminPort()).build();

            // Verify integrity check via WebAdmin
            int restoredUsers = when()
                .get("/youtrackdb/check")
            .then()
                .statusCode(HttpStatus.OK_200)
                .body("status", org.hamcrest.Matchers.equalTo("HEALTHY"))
                .body("databaseOpen", org.hamcrest.Matchers.equalTo(true))
                .extract()
                .jsonPath()
                .getInt("totalUsers");

            int restoredDomains = when()
                .get("/youtrackdb/check")
            .then()
                .statusCode(HttpStatus.OK_200)
                .extract()
                .jsonPath()
                .getInt("totalDomains");

            // Verify original user and domain are fully restored from backup!
            assertThat(restoredUsers).isEqualTo(1);
            assertThat(restoredDomains).isEqualTo(initialDomains);

            // Verify user can be queried via WebAdmin
            when()
                .get("/users")
            .then()
                .statusCode(HttpStatus.OK_200)
                .body("username", org.hamcrest.Matchers.hasItem(USER));

            // 7. Verify restored messages (including large sharded blob) via IMAP
            int restoredImapPort = restoredServer.getProbe(ImapGuiceProbe.class).getImapPort();
            TestIMAPClient restoredImapClient = new TestIMAPClient();
            restoredImapClient.connect("127.0.0.1", restoredImapPort)
                .login(USER, PASSWORD)
                .select(TestIMAPClient.INBOX);

            assertThat(restoredImapClient.getMessageCount(TestIMAPClient.INBOX)).isEqualTo(expectedTotal);
            String fetchUids = restoredImapClient.sendCommand("UID FETCH 1 (UID)");
            assertThat(fetchUids).contains("UID 1");
            restoredImapClient.disconnect();

            LOGGER.info("RESTORE SUCCESSFUL: User {} and all {} messages restored and verified!", USER, expectedTotal);
        } finally {
            restoredServer.stop();
        }
    }

    private static void copyDirectoryRecursively(Path source, Path target) throws IOException {
        if (!Files.exists(source)) {
            return;
        }
        try (var stream = Files.walk(source)) {
            stream.forEach(src -> {
                try {
                    Path dest = target.resolve(source.relativize(src));
                    if (Files.isDirectory(src)) {
                        if (!Files.exists(dest)) {
                            Files.createDirectories(dest);
                        }
                    } else {
                        if (dest.getParent() != null && !Files.exists(dest.getParent())) {
                            Files.createDirectories(dest.getParent());
                        }
                        Files.copy(src, dest, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                    }
                } catch (IOException e) {
                    throw new RuntimeException("Failed copying directory: " + src, e);
                }
            });
        }
    }

    private static void deleteDirectoryRecursively(Path path) throws IOException {
        if (Files.exists(path)) {
            Files.walk(path)
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);
        }
    }
}
