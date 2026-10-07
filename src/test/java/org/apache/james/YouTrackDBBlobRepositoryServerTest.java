package org.apache.james;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Properties;

import jakarta.mail.Session;
import jakarta.mail.internet.MimeMessage;

import org.apache.james.mailrepository.api.MailKey;
import org.apache.james.mailrepository.api.MailRepository;
import org.apache.james.mailrepository.api.MailRepositoryUrl;
import org.apache.james.server.core.MailImpl;
import org.apache.james.utils.MailRepositoryProbeImpl;
import org.apache.james.youtrackdb.YouTrackDBJamesConfiguration;
import org.apache.james.youtrackdb.YouTrackDBJamesServerMain;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

/**
 * The repositories declared in the test mailetcontainer.xml (blob://...) must be served by the blob store of the
 * real Guice wiring (Modules.override in YouTrackDBJamesServerMain), not by MemoryMailRepository.
 */
class YouTrackDBBlobRepositoryServerTest {
    private static final MailRepositoryUrl ERROR = MailRepositoryUrl.from("blob://var/mail/error/");

    @RegisterExtension
    static JamesServerExtension jamesServerExtension = new JamesServerBuilder<YouTrackDBJamesConfiguration>(tmpDir ->
        YouTrackDBJamesConfiguration.builder()
            .workingDirectory(tmpDir)
            .configurationFromClasspath()
            .build())
        .server(YouTrackDBJamesServerMain::createServer)
        .lifeCycle(JamesServerExtension.Lifecycle.PER_CLASS)
        .build();

    @Test
    void errorRepositoryShouldBeBackedByTheBlobStore(GuiceJamesServer server) throws Exception {
        MailRepositoryProbeImpl probe = server.getProbe(MailRepositoryProbeImpl.class);

        MailRepository repository = probe.getMailRepositoryStore().select(ERROR);

        assertThat(repository.getClass().getName()).doesNotContain("Memory");
        assertThat(probe.listRepositoryUrls()).contains(ERROR);
    }

    @Test
    void mailShouldBeStoredAndRetrievedThroughTheServerWiring(GuiceJamesServer server) throws Exception {
        MailRepositoryProbeImpl probe = server.getProbe(MailRepositoryProbeImpl.class);
        MimeMessage message = new MimeMessage(Session.getDefaultInstance(new Properties()));
        message.setSubject("stored in blob");
        message.setText("body");
        message.saveChanges();

        probe.getMailRepositoryStore().select(ERROR).removeAll();
        MailKey key = probe.getMailRepositoryStore().select(ERROR).store(MailImpl.builder()
            .name("e2e-mail")
            .sender("sender@james.local")
            .addRecipient("rcpt@james.local")
            .mimeMessage(message)
            .build());

        assertThat(probe.getRepositoryMailCount(ERROR)).isEqualTo(1L);
        assertThat(probe.getMail(ERROR, key).getMessage().getSubject()).isEqualTo("stored in blob");
        probe.getMailRepositoryStore().select(ERROR).removeAll();
    }

    @Test
    void mailShouldSurviveServerRestart(GuiceJamesServer server) throws Exception {
        MailRepositoryProbeImpl probe = server.getProbe(MailRepositoryProbeImpl.class);
        probe.getMailRepositoryStore().select(ERROR).removeAll();

        MimeMessage message = new MimeMessage(Session.getDefaultInstance(new Properties()));
        message.setSubject("survive server restart");
        message.setText("content across guice server restart");
        message.saveChanges();

        MailKey key = probe.getMailRepositoryStore().select(ERROR).store(MailImpl.builder()
            .name("restart-mail")
            .sender("sender@james.local")
            .addRecipient("rcpt@james.local")
            .mimeMessage(message)
            .build());

        server.stop();
        server.start();

        MailRepositoryProbeImpl restartedProbe = server.getProbe(MailRepositoryProbeImpl.class);
        assertThat(restartedProbe.getRepositoryMailCount(ERROR)).isEqualTo(1L);
        assertThat(restartedProbe.getMail(ERROR, key).getMessage().getSubject()).isEqualTo("survive server restart");
        restartedProbe.getMailRepositoryStore().select(ERROR).removeAll();
    }
}

