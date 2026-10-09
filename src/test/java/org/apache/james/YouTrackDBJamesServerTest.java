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

import com.google.common.base.Strings;

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
            .sendMessageWithHeaders(USER, USER, "header: toto\r\n\r\n" + Strings.repeat("0123456789\n", 1024));

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

            // Mandatory base IMAP4rev1 (RFC 3501) and IMAP4rev2 (RFC 9051)
            assertThat(capabilityLine)
                .contains("IMAP4REV1")
                .contains("IMAP4REV2");

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

            // 4. Test ENABLE CONDSTORE negotiation (RFC 5161)
            writer.println("a003 ENABLE CONDSTORE");
            String enableLine = reader.readLine();
            String enableOk = reader.readLine();
            assertThat(enableLine).startsWith("* ENABLED");
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
}
