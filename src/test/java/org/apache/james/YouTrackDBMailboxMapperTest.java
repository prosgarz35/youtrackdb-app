package org.apache.james;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.nio.file.Path;
import java.util.List;

import org.apache.james.core.Username;
import org.apache.james.mailbox.acl.ACLDiff;
import org.apache.james.mailbox.exception.MailboxExistsException;
import org.apache.james.mailbox.exception.MailboxNotFoundException;
import org.apache.james.mailbox.model.Mailbox;
import org.apache.james.mailbox.model.MailboxACL;
import org.apache.james.mailbox.model.MailboxPath;
import org.apache.james.mailbox.model.UidValidity;
import org.apache.james.mailbox.model.search.MailboxQuery;
import org.apache.james.mailbox.model.search.PrefixedWildcard;
import org.apache.james.mailbox.store.user.model.Subscription;
import org.apache.james.youtrackdb.YouTrackDBMailboxId;
import org.apache.james.youtrackdb.YouTrackDBMailboxMapper;
import org.apache.james.youtrackdb.YouTrackDBSubscriptionMapper;
import org.apache.james.youtrackdb.YouTrackDBTransactions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.jetbrains.youtrackdb.api.DatabaseType;
import com.jetbrains.youtrackdb.api.YouTrackDB;
import com.jetbrains.youtrackdb.api.YourTracks;
import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;

public class YouTrackDBMailboxMapperTest {

    @TempDir
    Path tempDir;

    private YouTrackDB ytdb;
    private YTDBGraphTraversalSource g;
    private YouTrackDBMailboxMapper mailboxMapper;
    private YouTrackDBSubscriptionMapper subscriptionMapper;

    @BeforeEach
    void setUp() {
        File dbDir = tempDir.resolve("var").resolve("youtrackdb").toFile();
        dbDir.mkdirs();
        ytdb = YourTracks.instance(dbDir.getAbsolutePath());
        ytdb.createIfNotExists("james", DatabaseType.DISK, "admin", "admin", "admin");
        g = ytdb.openTraversal("james", "admin", "admin");

        YouTrackDBTransactions.executeStrictTx(g, tx -> {
            tx.command("CREATE CLASS JamesMailbox IF NOT EXISTS EXTENDS V");
            tx.command("CREATE PROPERTY JamesMailbox.mailboxId IF NOT EXISTS STRING");
            tx.command("CREATE PROPERTY JamesMailbox.namespace IF NOT EXISTS STRING");
            tx.command("CREATE PROPERTY JamesMailbox.user IF NOT EXISTS STRING");
            tx.command("CREATE PROPERTY JamesMailbox.name IF NOT EXISTS STRING");
            tx.command("CREATE PROPERTY JamesMailbox.uidValidity IF NOT EXISTS LONG");
            tx.command("CREATE PROPERTY JamesMailbox.acl IF NOT EXISTS STRING");
            tx.command("CREATE INDEX JamesMailbox.mailboxId IF NOT EXISTS UNIQUE");
            tx.command("CREATE INDEX JamesMailbox.path IF NOT EXISTS ON JamesMailbox (namespace, user, name) UNIQUE");

            tx.command("CREATE CLASS JamesSubscription IF NOT EXISTS EXTENDS V");
            tx.command("CREATE PROPERTY JamesSubscription.user IF NOT EXISTS STRING");
            tx.command("CREATE PROPERTY JamesSubscription.mailbox IF NOT EXISTS STRING");
            tx.command("CREATE INDEX JamesSubscription.userAndMailbox IF NOT EXISTS ON JamesSubscription (user, mailbox) UNIQUE");

            tx.command("CREATE CLASS JamesMailboxAnnotation IF NOT EXISTS EXTENDS V");
            tx.command("CREATE PROPERTY JamesMailboxAnnotation.mailboxId IF NOT EXISTS STRING");
            tx.command("CREATE PROPERTY JamesMailboxAnnotation.key IF NOT EXISTS STRING");
            tx.command("CREATE PROPERTY JamesMailboxAnnotation.value IF NOT EXISTS STRING");

            tx.command("CREATE CLASS JamesMailboxMessage IF NOT EXISTS EXTENDS V");
            tx.command("CREATE PROPERTY JamesMailboxMessage.mailboxId IF NOT EXISTS STRING");
            tx.command("CREATE PROPERTY JamesMailboxMessage.uid IF NOT EXISTS LONG");
        });

        mailboxMapper = new YouTrackDBMailboxMapper(g);
        subscriptionMapper = new YouTrackDBSubscriptionMapper(g);
    }

    @AfterEach
    void tearDown() {
        if (g != null) {
            try {
                g.close();
            } catch (Exception ignored) {
            }
        }
        if (ytdb != null) {
            try {
                ytdb.close();
            } catch (Exception ignored) {
            }
        }
    }

    @Test
    void createAndFindMailboxByPathShouldWork() {
        MailboxPath path = MailboxPath.forUser(Username.of("alice"), "INBOX");
        UidValidity uidValidity = UidValidity.of(12345L);

        Mailbox created = mailboxMapper.create(path, uidValidity).block();
        assertThat(created).isNotNull();
        assertThat(created.getName()).isEqualTo("INBOX");
        assertThat(created.getUidValidity()).isEqualTo(uidValidity);

        Mailbox found = mailboxMapper.findMailboxByPath(path).block();
        assertThat(found).isNotNull();
        assertThat(found.getMailboxId()).isEqualTo(created.getMailboxId());
        assertThat(found.getUidValidity()).isEqualTo(uidValidity);
    }

    @Test
    void createDuplicateMailboxShouldFail() {
        MailboxPath path = MailboxPath.forUser(Username.of("alice"), "INBOX");
        mailboxMapper.create(path, UidValidity.of(12345L)).block();

        assertThatThrownBy(() -> mailboxMapper.create(path, UidValidity.of(67890L)).block())
            .hasCauseInstanceOf(MailboxExistsException.class);
    }

    @Test
    void renameMailboxShouldUpdatePathAndPreserveId() {
        MailboxPath originalPath = MailboxPath.forUser(Username.of("alice"), "Drafts");
        Mailbox created = mailboxMapper.create(originalPath, UidValidity.of(100L)).block();

        MailboxPath newPath = MailboxPath.forUser(Username.of("alice"), "Archive");
        created.setName("Archive");

        mailboxMapper.rename(created).block();

        assertThat(mailboxMapper.findMailboxByPath(originalPath).blockOptional()).isEmpty();
        Mailbox renamed = mailboxMapper.findMailboxByPath(newPath).block();
        assertThat(renamed).isNotNull();
        assertThat(renamed.getMailboxId()).isEqualTo(created.getMailboxId());
    }

    @Test
    void deleteMailboxShouldRemoveFromStore() {
        MailboxPath path = MailboxPath.forUser(Username.of("alice"), "Trash");
        Mailbox created = mailboxMapper.create(path, UidValidity.of(200L)).block();

        mailboxMapper.delete(created).block();

        assertThat(mailboxMapper.findMailboxByPath(path).blockOptional()).isEmpty();
        assertThatThrownBy(() -> mailboxMapper.findMailboxById(created.getMailboxId()).block())
            .hasCauseInstanceOf(MailboxNotFoundException.class);
    }

    @Test
    void hasChildrenAndQueryMatchingShouldWork() {
        Username alice = Username.of("alice");
        mailboxMapper.create(MailboxPath.forUser(alice, "INBOX"), UidValidity.of(1L)).block();
        Mailbox sub1 = mailboxMapper.create(MailboxPath.forUser(alice, "INBOX.Sub1"), UidValidity.of(2L)).block();
        mailboxMapper.create(MailboxPath.forUser(alice, "INBOX.Sub1.Child"), UidValidity.of(3L)).block();

        Mailbox inbox = mailboxMapper.findMailboxByPath(MailboxPath.forUser(alice, "INBOX")).block();
        assertThat(mailboxMapper.hasChildren(inbox, '.').block()).isTrue();
        assertThat(mailboxMapper.hasChildren(sub1, '.').block()).isTrue();

        Mailbox child = mailboxMapper.findMailboxByPath(MailboxPath.forUser(alice, "INBOX.Sub1.Child")).block();
        assertThat(mailboxMapper.hasChildren(child, '.').block()).isFalse();

        // Search with wildcard
        List<Mailbox> matches = mailboxMapper.findMailboxWithPathLike(
            MailboxQuery.builder()
                .userAndNamespaceFrom(MailboxPath.forUser(alice, "INBOX"))
                .expression(new PrefixedWildcard("INBOX."))
                .build()
                .asUserBound())
            .collectList()
            .block();

        assertThat(matches).hasSize(2)
            .extracting(Mailbox::getName)
            .containsExactlyInAnyOrder("INBOX.Sub1", "INBOX.Sub1.Child");
    }

    @Test
    void aclOperationsShouldPersistAndComputeDiff() throws Exception {
        MailboxPath path = MailboxPath.forUser(Username.of("alice"), "Shared");
        Mailbox created = mailboxMapper.create(path, UidValidity.of(500L)).block();

        MailboxACL.EntryKey bobKey = MailboxACL.EntryKey.createUserEntryKey(Username.of("bob"));
        MailboxACL.Rfc4314Rights readRights = MailboxACL.Rfc4314Rights.fromSerializedRfc4314Rights("lre");

        ACLDiff diff = mailboxMapper.updateACL(created, MailboxACL.command().key(bobKey).rights(readRights).asReplacement()).block();
        assertThat(diff.getNewACL().getEntries()).containsKey(bobKey);

        Mailbox loaded = mailboxMapper.findMailboxById(created.getMailboxId()).block();
        assertThat(loaded.getACL().getEntries().get(bobKey)).isEqualTo(readRights);
    }

    @Test
    void subscriptionsShouldPersistAcrossOperations() throws Exception {
        Username user = Username.of("alice");
        Subscription s1 = new Subscription(user, "INBOX");
        Subscription s2 = new Subscription(user, "Sent");

        subscriptionMapper.save(s1);
        subscriptionMapper.save(s2);
        // Duplicate save must be idempotent
        subscriptionMapper.save(s1);

        List<Subscription> subs = subscriptionMapper.findSubscriptionsForUser(user);
        assertThat(subs).extracting(Subscription::getMailbox)
            .containsExactlyInAnyOrder("INBOX", "Sent");

        subscriptionMapper.delete(s1);
        List<Subscription> afterDelete = subscriptionMapper.findSubscriptionsForUser(user);
        assertThat(afterDelete).extracting(Subscription::getMailbox)
            .containsExactly("Sent");
    }

    @Test
    void deleteShouldCascadeRemoveAnnotationsAndMessages() throws Exception {
        MailboxPath path = MailboxPath.forUser(Username.of("alice"), "Trash");
        Mailbox created = mailboxMapper.create(path, UidValidity.of(101L)).block();
        String mId = created.getMailboxId().serialize();

        // Seed an annotation and a message for this mailbox
        YouTrackDBTransactions.executeStrictTx(g, tx -> {
            tx.addV("JamesMailboxAnnotation")
                .property("mailboxId", mId)
                .property("key", "/shared/comment")
                .property("value", "sample-annotation")
                .iterate();

            tx.addV("JamesMailboxMessage")
                .property("mailboxId", mId)
                .property("uid", 1L)
                .iterate();
        });

        // Verify they exist prior to delete
        long annotBefore = g.computeInTx(tx -> tx.V().hasLabel("JamesMailboxAnnotation").has("mailboxId", mId).count().next());
        long msgsBefore = g.computeInTx(tx -> tx.V().hasLabel("JamesMailboxMessage").has("mailboxId", mId).count().next());
        assertThat(annotBefore).isEqualTo(1L);
        assertThat(msgsBefore).isEqualTo(1L);

        // Delete mailbox
        mailboxMapper.delete(created).block();

        // Verify mailbox, annotations, and messages are completely removed
        long mboxAfter = g.computeInTx(tx -> tx.V().hasLabel("JamesMailbox").has("mailboxId", mId).count().next());
        long annotAfter = g.computeInTx(tx -> tx.V().hasLabel("JamesMailboxAnnotation").has("mailboxId", mId).count().next());
        long msgsAfter = g.computeInTx(tx -> tx.V().hasLabel("JamesMailboxMessage").has("mailboxId", mId).count().next());

        assertThat(mboxAfter).isZero();
        assertThat(annotAfter).isZero();
        assertThat(msgsAfter).isZero();
    }
}
