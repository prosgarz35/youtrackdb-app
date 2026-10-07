package org.apache.james.youtrackdb;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.HashMap;
import java.util.Map;

import jakarta.inject.Inject;

import org.apache.james.filesystem.api.FileSystem;
import org.apache.james.task.TaskManager;
import org.apache.james.webadmin.Routes;
import org.apache.james.webadmin.tasks.TaskFromRequest;
import org.apache.james.webadmin.utils.ErrorResponder;
import org.apache.james.webadmin.utils.JsonTransformer;
import org.eclipse.jetty.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jetbrains.youtrackdb.api.YouTrackDB;
import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;
import spark.Request;
import spark.Response;
import spark.Service;

public class YouTrackDBAdminRoutes implements Routes {
    private static final Logger LOGGER = LoggerFactory.getLogger(YouTrackDBAdminRoutes.class);
    public static final String YOUTRACKDB_BASE_PATH = "/youtrackdb";

    private final YouTrackDB youTrackDB;
    private final YTDBGraphTraversalSource traversalSource;
    private final FileSystem fileSystem;
    private final TaskManager taskManager;
    private final JsonTransformer jsonTransformer;

    @Inject
    public YouTrackDBAdminRoutes(YouTrackDB youTrackDB,
                                YTDBGraphTraversalSource traversalSource,
                                FileSystem fileSystem,
                                TaskManager taskManager,
                                JsonTransformer jsonTransformer) {
        this.youTrackDB = youTrackDB;
        this.traversalSource = traversalSource;
        this.fileSystem = fileSystem;
        this.taskManager = taskManager;
        this.jsonTransformer = jsonTransformer;
    }

    @Override
    public String getBasePath() {
        return YOUTRACKDB_BASE_PATH;
    }

    @Override
    public void define(Service service) {
        TaskFromRequest backupTaskFromRequest = this::createBackupTask;
        service.post(YOUTRACKDB_BASE_PATH + "/backup", backupTaskFromRequest.asRoute(taskManager), jsonTransformer);
        service.post(YOUTRACKDB_BASE_PATH + "/blobs/gc", this::cleanupOrphanBlobs, jsonTransformer);
        service.get(YOUTRACKDB_BASE_PATH + "/check", this::checkIntegrity, jsonTransformer);
    }

    private YouTrackDBBackupTask createBackupTask(Request request) throws FileNotFoundException {
        String backupDirParam = request.queryParams("backupDir");
        File baseDir = fileSystem.getBasedir();
        File backupDir;
        if (backupDirParam != null && !backupDirParam.isBlank()) {
            if (backupDirParam.contains("..")) {
                throw new IllegalArgumentException("Path traversal not allowed in backupDir");
            }
            File requested = new File(backupDirParam);
            backupDir = requested.isAbsolute() ? requested : new File(baseDir, backupDirParam);
        } else {
            backupDir = new File(baseDir, "var/backups");
        }
        File blobsSourceDir = new File(baseDir, "var/blobs");
        return new YouTrackDBBackupTask(traversalSource, backupDir, blobsSourceDir);
    }

    private Object checkIntegrity(Request request, Response response) {
        try {
            boolean dbOpen = youTrackDB.isOpen();
            long[] counts = {0, 0, 0};
            if (dbOpen) {
                try {
                    traversalSource.executeInTx(tx -> {
                        counts[0] = tx.V().hasLabel(YouTrackDBBlobStoreDAO.CLASS_NAME).count().next();
                        counts[1] = tx.V().hasLabel("JamesUser").count().next();
                        counts[2] = tx.V().hasLabel("JamesDomain").count().next();
                    });
                } catch (Exception e) {
                    LOGGER.warn("Failed to get counts: {}", e.getMessage(), e);
                }
            }

            Map<String, Object> result = new HashMap<>();
            result.put("status", dbOpen ? "HEALTHY" : "UNHEALTHY");
            result.put("databaseOpen", dbOpen);
            result.put("totalBlobs", counts[0]);
            result.put("totalUsers", counts[1]);
            result.put("totalDomains", counts[2]);

            response.status(HttpStatus.OK_200);
            return result;
        } catch (Exception e) {
            LOGGER.error("Failed to check YouTrackDB integrity", e);
            throw ErrorResponder.builder()
                .statusCode(HttpStatus.INTERNAL_SERVER_ERROR_500)
                .type(ErrorResponder.ErrorType.SERVER_ERROR)
                .message("Check failed: " + e.getMessage())
                .haltError();
        }
    }

    private Object cleanupOrphanBlobs(Request request, Response response) {
        try {
            File blobsSourceDir = new File(fileSystem.getBasedir(), "var/blobs");
            long[] deleted = {0};
            if (blobsSourceDir.exists()) {
                java.util.Set<String> activeBlobIds = traversalSource.computeInTx(tx -> {
                    java.util.Set<String> set = new java.util.HashSet<>();
                    var list = tx.yql("SELECT blobId FROM JamesBlob").toList();
                    for (Object item : list) {
                        if (item instanceof Map<?, ?> m) {
                            Object bId = m.get("blobId");
                            if (bId != null) {
                                set.add(bId.toString());
                            }
                        }
                    }
                    return set;
                });

                long gracePeriodCutoff = System.currentTimeMillis() - java.time.Duration.ofHours(1).toMillis();
                try (var stream = java.nio.file.Files.walk(blobsSourceDir.toPath())) {
                    stream.filter(java.nio.file.Files::isRegularFile)
                        .forEach(path -> {
                            String fileName = path.getFileName().toString();
                            if (!fileName.contains(".tmp.") && !activeBlobIds.contains(fileName)) {
                                try {
                                    long lastModified = java.nio.file.Files.getLastModifiedTime(path).toMillis();
                                    // Only delete if older than grace period to protect active/in-flight writes
                                    if (lastModified < gracePeriodCutoff) {
                                        java.nio.file.Files.deleteIfExists(path);
                                        deleted[0]++;
                                    }
                                } catch (Exception ignored) {
                                }
                            }
                        });
                }
            }

            Map<String, Object> result = new HashMap<>();
            result.put("status", "COMPLETED");
            result.put("deletedOrphanBlobs", deleted[0]);
            response.status(HttpStatus.OK_200);
            return result;
        } catch (Exception e) {
            LOGGER.error("Failed to run orphan blobs cleanup", e);
            throw ErrorResponder.builder()
                .statusCode(HttpStatus.INTERNAL_SERVER_ERROR_500)
                .type(ErrorResponder.ErrorType.SERVER_ERROR)
                .message("Blobs GC failed: " + e.getMessage())
                .haltError();
        }
    }
}
