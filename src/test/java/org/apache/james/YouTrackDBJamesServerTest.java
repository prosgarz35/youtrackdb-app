package org.apache.james;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Durations.FIVE_HUNDRED_MILLISECONDS;
import static org.awaitility.Durations.ONE_MINUTE;

import org.apache.james.core.quota.QuotaSizeLimit;
import org.apache.james.modules.QuotaProbesImpl;
import org.apache.james.modules.protocols.ImapGuiceProbe;
import org.apache.james.modules.protocols.SmtpGuiceProbe;
import org.apache.james.utils.DataProbeImpl;
import org.apache.james.utils.SMTPMessageSender;
import org.apache.james.utils.TestIMAPClient;
import org.apache.james.youtrackdb.YouTrackDBJamesConfiguration;
import org.apache.james.youtrackdb.YouTrackDBJamesServerMain;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionFactory;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class YouTrackDBJamesServerTest implements JamesServerConcreteContract {

    @RegisterExtension
    static JamesServerExtension jamesServerExtension = new JamesServerBuilder<YouTrackDBJamesConfiguration>(tmpDir ->
        YouTrackDBJamesConfiguration.builder()
            .workingDirectory(tmpDir)
            .configurationFromClasspath()
            .build())
        .server(YouTrackDBJamesServerMain::createServer)
        .lifeCycle(JamesServerExtension.Lifecycle.PER_CLASS)
        .build();

    private static final ConditionFactory AWAIT = Awaitility.await()
        .atMost(ONE_MINUTE)
        .with()
        .pollInterval(FIVE_HUNDRED_MILLISECONDS);

    static final String DOMAIN = "james.local";
    private static final String USER = "toto@" + DOMAIN;
    private static final String PASSWORD = "123456";

    private TestIMAPClient testIMAPClient;
    private SMTPMessageSender smtpMessageSender;

    @BeforeEach
    void setUp() {
        this.testIMAPClient = new TestIMAPClient();
        this.smtpMessageSender = new SMTPMessageSender(DOMAIN);
    }

    @Test
    void guiceServerShouldUpdateQuota(GuiceJamesServer jamesServer) throws Exception {
        try {
            jamesServer.getProbe(DataProbeImpl.class)
                .fluent()
                .addDomain(DOMAIN);
        } catch (Exception ignored) {
        }
        try {
            jamesServer.getProbe(DataProbeImpl.class)
                .fluent()
                .addUser(USER, PASSWORD);
        } catch (Exception ignored) {
        }
        jamesServer.getProbe(QuotaProbesImpl.class).setGlobalMaxStorage(QuotaSizeLimit.size(50 * 1024));

        int imapPort = jamesServer.getProbe(ImapGuiceProbe.class).getImapPort();
        smtpMessageSender.connect(JAMES_SERVER_HOST, jamesServer.getProbe(SmtpGuiceProbe.class).getSmtpPort())
            .authenticate(USER, PASSWORD)
            .sendMessageWithHeaders(USER, USER, "header: toto\r\n\r\n" + "0123456789\n".repeat(1024));

        AWAIT.until(() -> testIMAPClient.connect(JAMES_SERVER_HOST, imapPort)
            .login(USER, PASSWORD)
            .select(TestIMAPClient.INBOX)
            .hasAMessage());

        assertThat(
            testIMAPClient.connect(JAMES_SERVER_HOST, imapPort)
                .login(USER, PASSWORD)
                .getQuotaRoot(TestIMAPClient.INBOX))
            .startsWith("* QUOTAROOT \"INBOX\" #private&toto@james.local\r\n" +
                "* QUOTA #private&toto@james.local (STORAGE 12 50)\r\n")
            .endsWith("OK GETQUOTAROOT completed.\r\n");
    }

    @Test
    void imapServerShouldAdvertiseImap4rev1AndImap4rev2Capabilities(GuiceJamesServer jamesServer) throws Exception {
        // Ensure domain and user exist idempotently
        try {
            jamesServer.getProbe(DataProbeImpl.class)
                .fluent()
                .addDomain(DOMAIN);
        } catch (Exception ignored) {
        }
        try {
            jamesServer.getProbe(DataProbeImpl.class)
                .fluent()
                .addUser(USER, PASSWORD);
        } catch (Exception ignored) {
        }

        int imapPort = jamesServer.getProbe(ImapGuiceProbe.class).getImapPort();
        try (java.net.Socket socket = new java.net.Socket(JAMES_SERVER_HOST, imapPort);
             java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.InputStreamReader(socket.getInputStream(), java.nio.charset.StandardCharsets.US_ASCII));
             java.io.PrintWriter writer = new java.io.PrintWriter(new java.io.OutputStreamWriter(socket.getOutputStream(), java.nio.charset.StandardCharsets.US_ASCII), true)) {

            // 1. Initial greeting must maintain strict backwards compatibility with RFC 3501 (IMAP4rev1)
            String banner = reader.readLine();
            assertThat(banner).startsWith("* OK JAMES IMAP4rev1 Server");

            // 2. Query CAPABILITY
            writer.println("a001 CAPABILITY");
            String capabilityLine = reader.readLine();
            String okLine = reader.readLine();

            assertThat(okLine).startsWith("a001 OK");
            assertThat(capabilityLine).startsWith("* CAPABILITY ");

            // Mandatory base IMAP4rev1 (RFC 3501)
            assertThat(capabilityLine)
                .contains("IMAP4REV1");

            // Mandatory RFC 9051 (IMAP4rev2) capabilities implemented by YouTrackDB & James:
            // MOVE (RFC 6855), OBJECTID (RFC 8474), SAVEDATE (RFC 8514), QUOTA (RFC 9208),
            // UNSELECT (RFC 3691), QRESYNC (RFC 7162 superseding CONDSTORE), UIDPLUS (RFC 4315),
            // METADATA (RFC 5464), ENABLE (RFC 5161), ESEARCH (RFC 4731), STATUS=SIZE (RFC 8438)
            assertThat(capabilityLine)
                .contains("MOVE")
                .contains("OBJECTID")
                .contains("SAVEDATE")
                .contains("QUOTA")
                .contains("UNSELECT")
                .contains("QRESYNC")
                .contains("UIDPLUS")
                .contains("METADATA")
                .contains("ENABLE")
                .contains("ESEARCH");

            // 3. Authenticate and test RFC 5161 / RFC 9051 ENABLE command (Authenticated state)
            writer.println("a002 LOGIN " + USER + " " + PASSWORD);
            String loginOk = reader.readLine();
            assertThat(loginOk).startsWith("a002 OK");

            // 4. Test RFC 5161 / RFC 9051 ENABLE IMAP4rev2 negotiation
            writer.println("a003 ENABLE IMAP4rev2");
            String enableLine = reader.readLine();
            String enableOk = reader.readLine();
            assertThat(enableLine).isEqualTo("* ENABLED IMAP4REV2");
            assertThat(enableOk).startsWith("a003 OK");

            // 5. Test RFC 9051 section 6.2.4 UNAUTHENTICATE command (returns to NON_AUTHENTICATED state)
            writer.println("a004 UNAUTHENTICATE");
            String unauthOk = reader.readLine();
            assertThat(unauthOk).startsWith("a004 OK");

            // Verify session is back in NON_AUTHENTICATED state by successfully re-logging in
            writer.println("a005 LOGIN " + USER + " " + PASSWORD);
            String reLoginOk = reader.readLine();
            assertThat(reLoginOk).startsWith("a005 OK");

            // 6. Clean logout
            writer.println("a006 LOGOUT");
            reader.readLine(); // * BYE
            reader.readLine(); // a006 OK
        }
    }

    @Test
    void imapServerShouldSupportAllFourClientArchetypes(GuiceJamesServer jamesServer) throws Exception {
        try {
            jamesServer.getProbe(DataProbeImpl.class).fluent().addDomain(DOMAIN);
        } catch (Exception ignored) {
        }
        try {
            jamesServer.getProbe(DataProbeImpl.class).fluent().addUser(USER, PASSWORD);
        } catch (Exception ignored) {
        }

        int imapPort = jamesServer.getProbe(ImapGuiceProbe.class).getImapPort();

        // =========================================================================================
        // Клиент 1: IMAP4rev1 only (RFC 3501 strictly, no modular extensions, no ENABLE)
        // =========================================================================================
        try (java.net.Socket socket = new java.net.Socket(JAMES_SERVER_HOST, imapPort);
             java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.InputStreamReader(socket.getInputStream(), java.nio.charset.StandardCharsets.US_ASCII));
             java.io.PrintWriter writer = new java.io.PrintWriter(new java.io.OutputStreamWriter(socket.getOutputStream(), java.nio.charset.StandardCharsets.US_ASCII), true)) {

            String banner = reader.readLine();
            assertThat(banner).startsWith("* OK JAMES IMAP4rev1 Server");

            writer.println("c11 CAPABILITY");
            String capLine = reader.readLine();
            String capOk = reader.readLine();
            assertThat(capLine).contains("IMAP4REV1");
            assertThat(capOk).startsWith("c11 OK");

            writer.println("c12 LOGIN " + USER + " " + PASSWORD);
            assertThat(reader.readLine()).startsWith("c12 OK");

            // RFC 3501: SELECT returns * N RECENT
            writer.println("c13 SELECT INBOX");
            java.util.List<String> selectResponses = new java.util.ArrayList<>();
            String line;
            while ((line = reader.readLine()) != null) {
                selectResponses.add(line);
                if (line.startsWith("c13 OK")) {
                    break;
                }
            }
            assertThat(selectResponses.stream().anyMatch(s -> s.matches("\\* \\d+ RECENT"))).isTrue();

            // RFC 3501: SEARCH returns classic * SEARCH
            writer.println("c14 SEARCH ALL");
            String searchResp = reader.readLine();
            String searchOk = reader.readLine();
            assertThat(searchResp).startsWith("* SEARCH");
            assertThat(searchResp).doesNotStartWith("* ESEARCH");
            assertThat(searchOk).startsWith("c14 OK");

            writer.println("c15 LOGOUT");
            reader.readLine(); // * BYE
            reader.readLine(); // OK
        }

        // =========================================================================================
        // Клиент 2: IMAP4rev2 only (RFC 9051 strictly: parses IMAP4rev2, calls ENABLE IMAP4rev2)
        // =========================================================================================
        try (java.net.Socket socket = new java.net.Socket(JAMES_SERVER_HOST, imapPort);
             java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.InputStreamReader(socket.getInputStream(), java.nio.charset.StandardCharsets.US_ASCII));
             java.io.PrintWriter writer = new java.io.PrintWriter(new java.io.OutputStreamWriter(socket.getOutputStream(), java.nio.charset.StandardCharsets.US_ASCII), true)) {

            reader.readLine(); // Banner
            writer.println("c21 CAPABILITY");
            String capLine = reader.readLine();
            reader.readLine(); // OK
            assertThat(capLine).contains("IMAP4REV1").contains("ENABLE");

            writer.println("c22 LOGIN " + USER + " " + PASSWORD);
            assertThat(reader.readLine()).startsWith("c22 OK");

            // RFC 9051 Appendix A: calls ENABLE IMAP4rev2
            writer.println("c23 ENABLE IMAP4rev2");
            assertThat(reader.readLine()).isEqualTo("* ENABLED IMAP4REV2");
            assertThat(reader.readLine()).startsWith("c23 OK");

            // RFC 9051: SELECT suppresses RECENT
            writer.println("c24 SELECT INBOX");
            java.util.List<String> selectResponses = new java.util.ArrayList<>();
            String line;
            while ((line = reader.readLine()) != null) {
                selectResponses.add(line);
                if (line.startsWith("c24 OK")) {
                    break;
                }
            }
            assertThat(selectResponses.stream().noneMatch(s -> s.matches("\\* \\d+ RECENT"))).isTrue();

            // RFC 9051: SEARCH automatically uses * ESEARCH
            writer.println("c25 SEARCH ALL");
            String searchResp = reader.readLine();
            String searchOk = reader.readLine();
            assertThat(searchResp).startsWith("* ESEARCH");
            assertThat(searchOk).startsWith("c25 OK");

            // RFC 9051 section 6.2.4: UNAUTHENTICATE command resets auth state
            writer.println("c26 UNAUTHENTICATE");
            assertThat(reader.readLine()).startsWith("c26 OK");

            writer.println("c27 LOGOUT");
            reader.readLine(); // * BYE
            reader.readLine(); // OK
        }

        // =========================================================================================
        // Клиент 3: IMAP4rev1 с частичной модульной поддержкой (rev1 + IDLE, MOVE, QRESYNC, UNSELECT)
        // =========================================================================================
        try (java.net.Socket socket = new java.net.Socket(JAMES_SERVER_HOST, imapPort);
             java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.InputStreamReader(socket.getInputStream(), java.nio.charset.StandardCharsets.US_ASCII));
             java.io.PrintWriter writer = new java.io.PrintWriter(new java.io.OutputStreamWriter(socket.getOutputStream(), java.nio.charset.StandardCharsets.US_ASCII), true)) {

            reader.readLine(); // Banner
            writer.println("c31 CAPABILITY");
            String capLine = reader.readLine();
            reader.readLine(); // OK
            // Modular client checks discrete capability tokens without enabling rev2:
            assertThat(capLine).contains("MOVE").contains("UNSELECT").contains("QRESYNC").contains("UIDPLUS");

            writer.println("c32 LOGIN " + USER + " " + PASSWORD);
            assertThat(reader.readLine()).startsWith("c32 OK");

            // Enable single module QRESYNC only (RFC 5161)
            writer.println("c33 ENABLE QRESYNC");
            assertThat(reader.readLine()).isEqualTo("* ENABLED QRESYNC");
            assertThat(reader.readLine()).startsWith("c33 OK");

            // Must RETAIN rev1 behavior: * N RECENT is still returned!
            writer.println("c34 SELECT INBOX");
            java.util.List<String> selectResponses = new java.util.ArrayList<>();
            String line;
            while ((line = reader.readLine()) != null) {
                selectResponses.add(line);
                if (line.startsWith("c34 OK")) {
                    break;
                }
            }
            assertThat(selectResponses.stream().anyMatch(s -> s.matches("\\* \\d+ RECENT"))).isTrue();

            // Direct use of modular command UNSELECT (RFC 3691)
            writer.println("c35 UNSELECT");
            assertThat(reader.readLine()).startsWith("c35 OK");

            writer.println("c36 LOGOUT");
            reader.readLine(); // * BYE
            reader.readLine(); // OK
        }

        // =========================================================================================
        // Клиент 4: Полная поддержка обоих протоколов (Dual-stack rev1 & rev2)
        // =========================================================================================
        try (java.net.Socket socket = new java.net.Socket(JAMES_SERVER_HOST, imapPort);
             java.io.BufferedReader reader = new java.io.BufferedReader(new java.io.InputStreamReader(socket.getInputStream(), java.nio.charset.StandardCharsets.US_ASCII));
             java.io.PrintWriter writer = new java.io.PrintWriter(new java.io.OutputStreamWriter(socket.getOutputStream(), java.nio.charset.StandardCharsets.US_ASCII), true)) {

            reader.readLine(); // Banner
            writer.println("c41 CAPABILITY");
            String capLine = reader.readLine();
            reader.readLine(); // OK
            // Dual-stack client verifies both markers are present simultaneously:
            assertThat(capLine).contains("IMAP4REV1").contains("IMAP4REV2");

            writer.println("c42 LOGIN " + USER + " " + PASSWORD);
            assertThat(reader.readLine()).startsWith("c42 OK");

            // Client starts in rev1 mode, executes standard commands
            writer.println("c43 SELECT INBOX");
            java.util.List<String> selectResponses = new java.util.ArrayList<>();
            String line;
            while ((line = reader.readLine()) != null) {
                selectResponses.add(line);
                if (line.startsWith("c43 OK")) {
                    break;
                }
            }
            assertThat(selectResponses.stream().anyMatch(s -> s.matches("\\* \\d+ RECENT"))).isTrue();

            // Client upgrades session dynamically to IMAP4rev2:
            writer.println("c44 ENABLE IMAP4rev2");
            assertThat(reader.readLine()).isEqualTo("* ENABLED IMAP4REV2");
            assertThat(reader.readLine()).startsWith("c44 OK");

            // Following commands now strictly follow RFC 9051:
            writer.println("c45 SEARCH ALL");
            assertThat(reader.readLine()).startsWith("* ESEARCH");
            assertThat(reader.readLine()).startsWith("c45 OK");

            // UNAUTHENTICATE resets session back to non-authenticated
            writer.println("c46 UNAUTHENTICATE");
            assertThat(reader.readLine()).startsWith("c46 OK");

            // Re-login with another session
            writer.println("c47 LOGIN " + USER + " " + PASSWORD);
            assertThat(reader.readLine()).startsWith("c47 OK");

            writer.println("c48 LOGOUT");
            reader.readLine(); // * BYE
            reader.readLine(); // OK
        }
    }
}
