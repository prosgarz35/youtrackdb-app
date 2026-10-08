package org.apache.james;

import static io.restassured.RestAssured.given;
import static io.restassured.RestAssured.when;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;

import org.apache.james.probe.DataProbe;
import org.apache.james.utils.DataProbeImpl;
import org.apache.james.utils.WebAdminGuiceProbe;
import org.apache.james.webadmin.WebAdminUtils;
import org.apache.james.webadmin.routes.DomainsRoutes;
import org.apache.james.webadmin.routes.UserRoutes;
import org.apache.james.youtrackdb.YouTrackDBJamesConfiguration;
import org.apache.james.youtrackdb.YouTrackDBJamesServerMain;
import org.eclipse.jetty.http.HttpStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

import io.restassured.RestAssured;

class YouTrackDBWebAdminServerIntegrationTest implements JamesServerConcreteContract {

    private static final String DOMAIN = "domain.local";
    private static final String USERNAME = "alice@" + DOMAIN;
    private static final String SPECIFIC_DOMAIN = DomainsRoutes.DOMAINS + "/" + DOMAIN;
    private static final String SPECIFIC_USER = UserRoutes.USERS + "/" + USERNAME;

    @RegisterExtension
    static JamesServerExtension jamesServerExtension = new JamesServerBuilder<YouTrackDBJamesConfiguration>(tmpDir ->
        YouTrackDBJamesConfiguration.builder()
            .workingDirectory(tmpDir)
            .configurationFromClasspath()
            .build())
        .server(YouTrackDBJamesServerMain::createServer)
        .lifeCycle(JamesServerExtension.Lifecycle.PER_CLASS)
        .build();

    private DataProbe dataProbe;
    private GuiceJamesServer server;

    @BeforeEach
    void setUp(GuiceJamesServer guiceJamesServer) throws Exception {
        this.server = guiceJamesServer;
        dataProbe = guiceJamesServer.getProbe(DataProbeImpl.class);
        WebAdminGuiceProbe webAdminGuiceProbe = guiceJamesServer.getProbe(WebAdminGuiceProbe.class);

        RestAssured.requestSpecification = WebAdminUtils.buildRequestSpecification(webAdminGuiceProbe.getWebAdminPort())
            .build();
    }

    @Test
    void webAdminShouldManageDomains() throws Exception {
        when()
            .put(SPECIFIC_DOMAIN)
        .then()
            .statusCode(HttpStatus.NO_CONTENT_204);

        assertThat(dataProbe.listDomains()).contains(DOMAIN);

        when()
            .get(DomainsRoutes.DOMAINS)
        .then()
            .statusCode(HttpStatus.OK_200)
            .body("", hasItem(DOMAIN));

        when()
            .delete(SPECIFIC_DOMAIN)
        .then()
            .statusCode(HttpStatus.NO_CONTENT_204);

        assertThat(dataProbe.listDomains()).doesNotContain(DOMAIN);
    }

    @Test
    void webAdminShouldManageUsers() throws Exception {
        dataProbe.addDomain(DOMAIN);

        given()
            .body("{\"password\":\"secret\"}")
        .when()
            .put(SPECIFIC_USER)
        .then()
            .statusCode(HttpStatus.NO_CONTENT_204);

        assertThat(dataProbe.listUsers()).contains(USERNAME);

        when()
            .get(UserRoutes.USERS)
        .then()
            .statusCode(HttpStatus.OK_200)
            .body("username", hasItem(USERNAME));

        when()
            .delete(SPECIFIC_USER)
        .then()
            .statusCode(HttpStatus.NO_CONTENT_204);

        assertThat(dataProbe.listUsers()).doesNotContain(USERNAME);
    }

    @Test
    void webAdminShouldCheckIntegrity() {
        when()
            .get("/youtrackdb/check")
        .then()
            .statusCode(HttpStatus.OK_200)
            .body("status", is("HEALTHY"))
            .body("databaseOpen", is(true));
    }

    @Test
    void webAdminShouldExecuteBackup() {
        String taskId = when()
            .post("/youtrackdb/backup")
        .then()
            .statusCode(HttpStatus.CREATED_201)
            .header("Location", org.hamcrest.Matchers.startsWith("/tasks/"))
            .extract()
            .jsonPath()
            .getString("taskId");

        when()
            .get("/tasks/" + taskId + "/await")
        .then()
            .statusCode(HttpStatus.OK_200)
            .body("status", org.hamcrest.Matchers.equalTo("completed"))
            .body("type", org.hamcrest.Matchers.equalTo("youtrackdb-backup"));
    }

    @Test
    void webAdminShouldRecomputeCurrentQuotas() throws Exception {
        dataProbe.addDomain(DOMAIN);
        dataProbe.addUser(USERNAME, "secret");

        org.apache.james.mailbox.probe.MailboxProbe mailboxProbe = server.getProbe(org.apache.james.modules.MailboxProbeImpl.class);
        org.apache.james.mailbox.model.MailboxPath inboxPath = org.apache.james.mailbox.model.MailboxPath.inbox(org.apache.james.core.Username.of(USERNAME));
        mailboxProbe.createMailbox(inboxPath.getNamespace(), inboxPath.getUser().asString(), inboxPath.getName());

        byte[] mailContent = "Subject: Test Quota\r\n\r\nHello quota recompute body!".getBytes(java.nio.charset.StandardCharsets.UTF_8);
        mailboxProbe.appendMessage(USERNAME, inboxPath, new java.io.ByteArrayInputStream(mailContent), new java.util.Date(), false, new jakarta.mail.Flags());

        // Corrupt the quota counter: manually decrease/corrupt usage in CurrentQuotaManager so it is incorrect
        org.apache.james.mailbox.quota.CurrentQuotaManager currentQuotaManager = server.getProbe(org.apache.james.modules.QuotaProbesImpl.class)
            .getQuotaRoot(inboxPath) != null ? server.getProbe(org.apache.james.modules.MailboxProbeImpl.class) != null ? null : null : null;
        
        com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource g = server.getProbe(org.apache.james.youtrackdb.YouTrackDBProbe.class).getTraversalSource();
        org.apache.james.youtrackdb.YouTrackDBTransactions.executeStrictTx(g, tx -> {
            tx.command("UPDATE JamesQuotaUsage SET messageCount = 999, size = 999999 WHERE quotaRoot LIKE :qr",
                "qr", "%" + USERNAME);
        });

        // Verify corrupted value is currently seen
        when()
            .get("/quota/users/" + USERNAME)
        .then()
            .statusCode(HttpStatus.OK_200)
            .body("occupation.count", org.hamcrest.Matchers.equalTo(999))
            .body("occupation.size", org.hamcrest.Matchers.equalTo(999999));

        // Trigger RecomputeCurrentQuotas task via WebAdmin
        String taskId = given()
            .queryParam("task", "RecomputeCurrentQuotas")
        .when()
            .post("/quota/users")
        .then()
            .statusCode(HttpStatus.CREATED_201)
            .header("Location", org.hamcrest.Matchers.startsWith("/tasks/"))
            .extract()
            .jsonPath()
            .getString("taskId");

        when()
            .get("/tasks/" + taskId + "/await")
        .then()
            .statusCode(HttpStatus.OK_200)
            .body("status", org.hamcrest.Matchers.equalTo("completed"))
            .body("type", org.hamcrest.Matchers.equalTo("recompute-current-quotas"));

        // Verify the user quota has been corrected to exactly 1 message and the exact non-zero byte size!
        when()
            .get("/quota/users/" + USERNAME)
        .then()
            .statusCode(HttpStatus.OK_200)
            .body("occupation.count", org.hamcrest.Matchers.equalTo(1))
            .body("occupation.size", org.hamcrest.Matchers.equalTo(mailContent.length));
    }
}

