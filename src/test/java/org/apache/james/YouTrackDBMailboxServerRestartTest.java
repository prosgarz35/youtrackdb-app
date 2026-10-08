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
        String statusBefore;
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
            checkInboxClient.connect("127.0.0.1", imapPort1)
                .login(USER, PASSWORD);

            AWAIT.until(() -> {
                checkInboxClient.select(TestIMAPClient.INBOX);
                return checkInboxClient.hasAMessage();
            });

            // Set flags before shutdown
            checkInboxClient.setFlagsForAllMessagesInMailbox("\\Seen $CustomFlag");

            // Capture status/UIDVALIDITY before shutdown
            statusBefore = checkInboxClient.sendCommand("STATUS INBOX (UIDVALIDITY UIDNEXT)");
            checkInboxClient.disconnect();

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

            // RFC 3501: Verify UIDVALIDITY is unchanged after restart
            String statusAfter = imapClient2.sendCommand("STATUS INBOX (UIDVALIDITY UIDNEXT)");
            // Extract UIDVALIDITY value
            java.util.regex.Pattern pValidity = java.util.regex.Pattern.compile("UIDVALIDITY\\s+(\\d+)");
            java.util.regex.Matcher mBefore = pValidity.matcher(statusBefore);
            java.util.regex.Matcher mAfter = pValidity.matcher(statusAfter);
            assertThat(mBefore.find()).isTrue();
            assertThat(mAfter.find()).isTrue();
            assertThat(mAfter.group(1)).isEqualTo(mBefore.group(1));

            // Verify UIDNEXT before delivering 2nd message is 2
            java.util.regex.Pattern pNext = java.util.regex.Pattern.compile("UIDNEXT\\s+(\\d+)");
            java.util.regex.Matcher mNextAfter = pNext.matcher(statusAfter);
            assertThat(mNextAfter.find()).isTrue();
            assertThat(mNextAfter.group(1)).isEqualTo("2");

            // Verify flags survived restart using UID FETCH (RFC 3501 UID-based check)
            String fetchFlags = imapClient2.sendCommand("UID FETCH 1 (FLAGS)");
            assertThat(fetchFlags).contains("\\Seen");
            assertThat(fetchFlags).contains("$CustomFlag");

            // Verify custom mailbox survived restart
            imapClient2.select("CustomFolder");
            assertThat(imapClient2.list()).anyMatch(line -> line.contains("CustomFolder"));

            // Deliver a 2nd message after restart and verify UID increments monotonically without duplicates
            SMTPMessageSender smtpSender2 = new SMTPMessageSender(DOMAIN);
            smtpSender2.connect("127.0.0.1", server2.getProbe(SmtpGuiceProbe.class).getSmtpPort())
                .authenticate(USER, PASSWORD)
                .sendMessageWithHeaders(USER, USER, "Subject: Second Mail\r\n\r\nSecond message post restart!");

            AWAIT.until(() -> {
                imapClient2.select(TestIMAPClient.INBOX);
                return imapClient2.getMessageCount(TestIMAPClient.INBOX) >= 2;
            });

            // Exact regex matching for (UID 1) and (UID 2)
            String fetchUids = imapClient2.sendCommand("FETCH 1:2 (UID)");
            assertThat(fetchUids).matches(java.util.regex.Pattern.compile("(?s).*\\bUID\\s+1\\b.*\\bUID\\s+2\\b.*"));

            // Verify UIDNEXT after 2nd message is now 3
            String statusAfterSecond = imapClient2.sendCommand("STATUS INBOX (UIDNEXT)");
            java.util.regex.Matcher mNextSecond = pNext.matcher(statusAfterSecond);
            assertThat(mNextSecond.find()).isTrue();
            assertThat(mNextSecond.group(1)).isEqualTo("3");

        } finally {
            server2.stop();
        }
    }
}
