package org.apache.james.youtrackdb;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.UUID;

import org.apache.james.mailbox.model.MailboxAnnotation;
import org.apache.james.mailbox.model.MailboxAnnotationKey;
import org.apache.james.mailbox.model.MailboxId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.jetbrains.youtrackdb.api.DatabaseType;
import com.jetbrains.youtrackdb.api.YouTrackDB;
import com.jetbrains.youtrackdb.api.YourTracks;
import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;

class YouTrackDBAnnotationMapperTest {

    private static final MailboxAnnotationKey PRIVATE_USER_KEY = new MailboxAnnotationKey("/private/commentuser");
    private static final MailboxAnnotationKey PRIVATE_KEY = new MailboxAnnotationKey("/private/comment");
    private static final MailboxAnnotationKey PRIVATE_CHILD_KEY = new MailboxAnnotationKey("/private/comment/user");
    private static final MailboxAnnotationKey PRIVATE_GRANDCHILD_KEY = new MailboxAnnotationKey("/private/comment/user/name");
    private static final MailboxAnnotationKey SHARED_KEY = new MailboxAnnotationKey("/shared/comment");

    private static final MailboxAnnotation PRIVATE_ANNOTATION = MailboxAnnotation.newInstance(PRIVATE_KEY, "My private comment");
    private static final MailboxAnnotation PRIVATE_CHILD_ANNOTATION = MailboxAnnotation.newInstance(PRIVATE_CHILD_KEY, "My private comment");
    private static final MailboxAnnotation PRIVATE_ANNOTATION_UPDATE = MailboxAnnotation.newInstance(PRIVATE_KEY, "My updated private comment");
    private static final MailboxAnnotation SHARED_ANNOTATION = MailboxAnnotation.newInstance(SHARED_KEY, "My shared comment");
    private static final MailboxAnnotation PRIVATE_GRANDCHILD_ANNOTATION = MailboxAnnotation.newInstance(PRIVATE_GRANDCHILD_KEY, "My private comment");

    private Path tempDir;
    private YouTrackDB youTrackDB;
    private YTDBGraphTraversalSource g;
    private YouTrackDBAnnotationMapper annotationMapper;
    private MailboxId mailboxId;

    @BeforeEach
    public void setUp() {
        try {
            tempDir = Files.createTempDirectory("ytdb-annotation-test");
            File dbDir = tempDir.resolve("ytdb").toFile();
            dbDir.mkdirs();
            youTrackDB = YourTracks.instance(dbDir.getAbsolutePath());
            youTrackDB.createIfNotExists("test", DatabaseType.DISK, "admin", "admin", "admin");
            g = youTrackDB.openTraversal("test", "admin", "admin");

            g.executeInTx(tx -> {
                tx.command("CREATE CLASS " + YouTrackDBAnnotationMapper.CLASS + " IF NOT EXISTS EXTENDS V");
                tx.command("CREATE PROPERTY " + YouTrackDBAnnotationMapper.CLASS + "." + YouTrackDBAnnotationMapper.PROP_MAILBOX_ID + " IF NOT EXISTS STRING");
                tx.command("CREATE PROPERTY " + YouTrackDBAnnotationMapper.CLASS + "." + YouTrackDBAnnotationMapper.PROP_KEY + " IF NOT EXISTS STRING");
                tx.command("CREATE PROPERTY " + YouTrackDBAnnotationMapper.CLASS + "." + YouTrackDBAnnotationMapper.PROP_VALUE + " IF NOT EXISTS STRING");
                tx.command("CREATE INDEX " + YouTrackDBAnnotationMapper.CLASS + ".mailboxAndKey IF NOT EXISTS ON " + YouTrackDBAnnotationMapper.CLASS + " (mailboxId, key) UNIQUE");
            });

            annotationMapper = new YouTrackDBAnnotationMapper(g);
            mailboxId = YouTrackDBMailboxId.of(UUID.randomUUID().toString());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @AfterEach
    void tearDown() throws Exception {
        if (g != null) {
            g.close();
        }
        if (youTrackDB != null) {
            youTrackDB.close();
        }
        if (tempDir != null && Files.exists(tempDir)) {
            try (var s = Files.walk(tempDir)) {
                s.sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
            } catch (IOException ignored) {
            }
        }
    }

    @Test
    void insertAnnotationShouldThrowExceptionWithNilData() {
        assertThatThrownBy(() -> annotationMapper.insertAnnotation(mailboxId, MailboxAnnotation.nil(PRIVATE_KEY)))
            .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void insertAnnotationShouldCreateNewAnnotation() {
        annotationMapper.insertAnnotation(mailboxId, PRIVATE_ANNOTATION);
        assertThat(annotationMapper.getAllAnnotations(mailboxId)).containsExactly(PRIVATE_ANNOTATION);
    }

    @Test
    void insertAnnotationShouldUpdateExistedAnnotation() {
        annotationMapper.insertAnnotation(mailboxId, PRIVATE_ANNOTATION);
        annotationMapper.insertAnnotation(mailboxId, PRIVATE_ANNOTATION_UPDATE);
        assertThat(annotationMapper.getAllAnnotations(mailboxId)).containsExactly(PRIVATE_ANNOTATION_UPDATE);
    }

    @Test
    void deleteAnnotationShouldDeleteStoredAnnotation() {
        annotationMapper.insertAnnotation(mailboxId, PRIVATE_ANNOTATION);
        annotationMapper.deleteAnnotation(mailboxId, PRIVATE_KEY);
        assertThat(annotationMapper.getAllAnnotations(mailboxId)).isEmpty();
    }

    @Test
    void getEmptyAnnotationsWithNonStoredAnnotations() {
        assertThat(annotationMapper.getAllAnnotations(mailboxId)).isEmpty();
    }

    @Test
    void getAllAnnotationsShouldRetrieveStoredAnnotations() {
        annotationMapper.insertAnnotation(mailboxId, PRIVATE_ANNOTATION);
        annotationMapper.insertAnnotation(mailboxId, SHARED_ANNOTATION);
        assertThat(annotationMapper.getAllAnnotations(mailboxId)).containsOnly(PRIVATE_ANNOTATION, SHARED_ANNOTATION);
    }

    @Test
    void getAnnotationsByKeysShouldReturnStoredAnnotationWithFilter() {
        annotationMapper.insertAnnotation(mailboxId, PRIVATE_ANNOTATION);
        annotationMapper.insertAnnotation(mailboxId, PRIVATE_CHILD_ANNOTATION);
        assertThat(annotationMapper.getAnnotationsByKeys(mailboxId, java.util.Set.of(PRIVATE_KEY)))
            .containsOnly(PRIVATE_ANNOTATION);
    }

    @Test
    void getAnnotationsByKeysWithAllDepthShouldReturnDescendantAnnotations() {
        annotationMapper.insertAnnotation(mailboxId, PRIVATE_ANNOTATION);
        annotationMapper.insertAnnotation(mailboxId, PRIVATE_CHILD_ANNOTATION);
        annotationMapper.insertAnnotation(mailboxId, PRIVATE_GRANDCHILD_ANNOTATION);
        assertThat(annotationMapper.getAnnotationsByKeysWithAllDepth(mailboxId, java.util.Set.of(PRIVATE_KEY)))
            .containsOnly(PRIVATE_ANNOTATION, PRIVATE_CHILD_ANNOTATION, PRIVATE_GRANDCHILD_ANNOTATION);
    }

    @Test
    void getAnnotationsByKeysWithOneDepthShouldReturnDirectChildren() {
        annotationMapper.insertAnnotation(mailboxId, PRIVATE_ANNOTATION);
        annotationMapper.insertAnnotation(mailboxId, PRIVATE_CHILD_ANNOTATION);
        annotationMapper.insertAnnotation(mailboxId, PRIVATE_GRANDCHILD_ANNOTATION);
        assertThat(annotationMapper.getAnnotationsByKeysWithOneDepth(mailboxId, java.util.Set.of(PRIVATE_KEY)))
            .containsOnly(PRIVATE_ANNOTATION, PRIVATE_CHILD_ANNOTATION);
    }

    @Test
    void countAnnotationsShouldReturnCount() {
        annotationMapper.insertAnnotation(mailboxId, PRIVATE_ANNOTATION);
        annotationMapper.insertAnnotation(mailboxId, SHARED_ANNOTATION);
        assertThat(annotationMapper.countAnnotations(mailboxId)).isEqualTo(2);
    }

    @Test
    void existShouldReturnTrueWhenExists() {
        annotationMapper.insertAnnotation(mailboxId, PRIVATE_ANNOTATION);
        assertThat(annotationMapper.exist(mailboxId, PRIVATE_ANNOTATION)).isTrue();
    }

    @Test
    void concurrentInsertsShouldResolveWithoutException() throws Exception {
        int threads = 4;
        java.util.concurrent.ExecutorService executor = java.util.concurrent.Executors.newFixedThreadPool(threads);
        java.util.concurrent.CountDownLatch startLatch = new java.util.concurrent.CountDownLatch(1);
        java.util.concurrent.CountDownLatch doneLatch = new java.util.concurrent.CountDownLatch(threads);
        java.util.concurrent.atomic.AtomicInteger errorCount = new java.util.concurrent.atomic.AtomicInteger();

        for (int i = 0; i < threads; i++) {
            final int idx = i;
            executor.submit(() -> {
                try {
                    startLatch.await();
                    annotationMapper.insertAnnotation(mailboxId,
                        MailboxAnnotation.newInstance(PRIVATE_KEY, "val-" + idx));
                } catch (Exception e) {
                    e.printStackTrace();
                    errorCount.incrementAndGet();
                } finally {
                    doneLatch.countDown();
                }
            });
        }

        startLatch.countDown();
        boolean completed = doneLatch.await(10, java.util.concurrent.TimeUnit.SECONDS);
        executor.shutdown();

        assertThat(completed).isTrue();
        assertThat(errorCount.get()).isZero();
        assertThat(annotationMapper.getAllAnnotations(mailboxId)).hasSize(1);
        assertThat(annotationMapper.getAllAnnotations(mailboxId).getFirst().getValue().get())
            .startsWith("val-");
    }
}
