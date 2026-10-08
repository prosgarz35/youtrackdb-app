package org.apache.james;

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Durations.FIVE_HUNDRED_MILLISECONDS;
import static org.awaitility.Durations.ONE_MINUTE;

import java.io.File;

import org.apache.james.modules.protocols.ImapGuiceProbe;
import org.apache.james.modules.protocols.SmtpGuiceProbe;
import org.apache.james.utils.DataProbeImpl;
import org.apache.james.utils.SMTPMessageSender;
import org.apache.james.utils.TestIMAPClient;
import org.apache.james.youtrackdb.YouTrackDBJamesConfiguration;
import org.apache.james.youtrackdb.YouTrackDBJamesServerMain;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionFactory;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class YouTrackDBMailboxServerRestartTest {

    private static final ConditionFactory AWAIT = Awaitility.await()
        .atMost(ONE_MINUTE)
        .with()
        .pollInterval(FIVE_HUNDRED_MILLISECONDS);

    private static final String DOMAIN = "james.local";
    private static final String USER = "bob@" + DOMAIN;
    private static final String PASSWORD = "password123";

    @Test
    void mailboxesAndMessagesShouldSurviveServerRestart(@TempDir File tempDir) throws Exception {
        YouTrackDBJamesConfiguration configuration = YouTrackDBJamesConfiguration.builder()
            .workingDirectory(tempDir)
            .configurationFromClasspath()
            .build();

        // 1. First Server Run: create user, custom mailbox via IMAP, and deliver an email via SMTP
        GuiceJamesServer server1 = YouTrackDBJamesServerMain.createServer(configuration);
        server1.start();
        try {
            server1.getProbe(DataProbeImpl.class)
                .fluent()
                .addDomain(DOMAIN)
                .addUser(USER, PASSWORD);

            int imapPort1 = server1.getProbe(ImapGuiceProbe.class).getImapPort();

            // Create custom mailbox via IMAP
            TestIMAPClient imapClient1 = new TestIMAPClient();
            imapClient1.connect("127.0.0.1", imapPort1)
                .login(USER, PASSWORD)
                .create("CustomFolder")
                .disconnect();

            SMTPMessageSender smtpSender = new SMTPMessageSender(DOMAIN);
            smtpSender.connect("127.0.0.1", server1.getProbe(SmtpGuiceProbe.class).getSmtpPort())
                .authenticate(USER, PASSWORD)
                .sendMessageWithHeaders(USER, USER, "Subject: Persistent Mail\r\n\r\nHello persistent mailbox world!");

            TestIMAPClient checkInboxClient = new TestIMAPClient();
            AWAIT.until(() -> checkInboxClient.connect("127.0.0.1", imapPort1)
                .login(USER, PASSWORD)
                .select(TestIMAPClient.INBOX)
                .hasAMessage());

        } finally {
            server1.stop();
        }

        // 2. Second Server Run: restart server on the same data directory
        GuiceJamesServer server2 = YouTrackDBJamesServerMain.createServer(configuration);
        server2.start();
        try {
            int imapPort2 = server2.getProbe(ImapGuiceProbe.class).getImapPort();

            TestIMAPClient imapClient2 = new TestIMAPClient();
            imapClient2.connect("127.0.0.1", imapPort2)
                .login(USER, PASSWORD);

            // Verify INBOX message survived restart
            imapClient2.select(TestIMAPClient.INBOX);
            assertThat(imapClient2.hasAMessage()).isTrue();

            String messageBody = imapClient2.readFirstMessage();
            assertThat(messageBody).contains("Hello persistent mailbox world!");

            // Verify custom mailbox survived restart
            imapClient2.select("CustomFolder");
            assertThat(imapClient2.list()).anyMatch(line -> line.contains("CustomFolder"));

        } finally {
            server2.stop();
        }
    }
}
