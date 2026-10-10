package org.apache.james.youtrackdb;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.List;
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
    private final YouTrackDBMimePartsGc mimePartsGc;

    @Inject
    public YouTrackDBAdminRoutes(YouTrackDB youTrackDB,
                                YTDBGraphTraversalSource traversalSource,
                                FileSystem fileSystem,
                                TaskManager taskManager,
                                JsonTransformer jsonTransformer,
                                YouTrackDBMimePartsGc mimePartsGc) {
        this.youTrackDB = youTrackDB;
        this.traversalSource = traversalSource;
        this.fileSystem = fileSystem;
        this.taskManager = taskManager;
        this.jsonTransformer = jsonTransformer;
        this.mimePartsGc = mimePartsGc;
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
        service.post(YOUTRACKDB_BASE_PATH + "/mime-parts/gc", this::cleanupOrphanMimeParts, jsonTransformer);
        service.get(YOUTRACKDB_BASE_PATH + "/check", this::checkIntegrity, jsonTransformer);
    }

    private YouTrackDBBackupTask createBackupTask(Request request) throws FileNotFoundException {
        String backupDirParam = request.queryParams("backupDir");
        File baseDir = fileSystem.getBasedir();
        java.nio.file.Path basePath = baseDir.toPath().toAbsolutePath().normalize();
        java.nio.file.Path blobsPath = basePath.resolve("var/blobs").normalize();
        java.nio.file.Path storePath = basePath.resolve("var/youtrackdb").normalize();
        File backupDir;
        if (backupDirParam != null && !backupDirParam.isBlank()) {
            java.nio.file.Path targetPath = java.nio.file.Paths.get(backupDirParam);
            if (!targetPath.isAbsolute()) {
                targetPath = basePath.resolve(targetPath);
            }
            targetPath = targetPath.normalize();
            if (!targetPath.startsWith(basePath) || targetPath.startsWith(blobsPath) || targetPath.startsWith(storePath)) {
                throw new IllegalArgumentException("Path traversal or forbidden destination: backupDir must be within server base directory and outside blobs/database store");
            }
            backupDir = targetPath.toFile();
        } else {
            backupDir = basePath.resolve("var/backups").toFile();
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
                    List<Map<String, Object>> blobRows = YouTrackDBTransactions.queryRows(traversalSource,
                        "SELECT count(*) AS total FROM JamesBlob");
                    if (!blobRows.isEmpty() && blobRows.getFirst().get("total") instanceof Number n) {
                        counts[0] = n.longValue();
                    }
                    List<Map<String, Object>> userRows = YouTrackDBTransactions.queryRows(traversalSource,
                        "SELECT count(*) AS total FROM JamesUser");
                    if (!userRows.isEmpty() && userRows.getFirst().get("total") instanceof Number n) {
                        counts[1] = n.longValue();
                    }
                    List<Map<String, Object>> domainRows = YouTrackDBTransactions.queryRows(traversalSource,
                        "SELECT count(*) AS total FROM JamesDomain");
                    if (!domainRows.isEmpty() && domainRows.getFirst().get("total") instanceof Number n) {
                        counts[2] = n.longValue();
                    }
                } catch (Exception e) {
                    LOGGER.warn("Failed to get counts: {}", e.getMessage(), e);
                }
            }

            Map<String, Object> result = Map.of(
                "status", dbOpen ? "HEALTHY" : "UNHEALTHY",
                "databaseOpen", dbOpen,
                "totalBlobs", counts[0],
                "totalUsers", counts[1],
                "totalDomains", counts[2]
            );

            response.status(dbOpen ? HttpStatus.OK_200 : HttpStatus.SERVICE_UNAVAILABLE_503);
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

    /** Run twice, a few minutes apart: the first run only reports what it would delete (pendingOrphanParts). */
    private Object cleanupOrphanMimeParts(Request request, Response response) {
        try {
            YouTrackDBMimePartsGc.Result gcResult = mimePartsGc.collect();
            response.status(HttpStatus.OK_200);
            return Map.of(
                "status", "COMPLETED",
                "referencedParts", gcResult.referencedParts(),
                "deletedOrphanParts", gcResult.deletedParts(),
                "pendingOrphanParts", gcResult.pendingParts()
            );
        } catch (Exception e) {
            LOGGER.error("Failed to run MIME parts cleanup", e);
            throw ErrorResponder.builder()
                .statusCode(HttpStatus.INTERNAL_SERVER_ERROR_500)
                .type(ErrorResponder.ErrorType.SERVER_ERROR)
                .message("MIME parts GC failed: " + e.getMessage())
                .haltError();
        }
    }

    private Object cleanupOrphanBlobs(Request request, Response response) {
        try {
            File blobsSourceDir = new File(fileSystem.getBasedir(), "var/blobs");
            long[] deleted = {0};
            if (blobsSourceDir.exists()) {
                List<Map<String, Object>> rows = YouTrackDBTransactions.queryRows(traversalSource,
                    "SELECT blobId FROM JamesBlob");
                java.util.Set<String> activeBlobIds = new java.util.HashSet<>(rows.size());
                for (Map<String, Object> row : rows) {
                    Object bId = row.get("blobId");
                    if (bId != null) {
                        activeBlobIds.add(bId.toString());
                    }
                }

                java.util.Set<String> keepNames = YouTrackDBBlobStoreDAO.fileNamesToKeep(activeBlobIds);
                long gracePeriodCutoff = System.currentTimeMillis() - java.time.Duration.ofHours(1).toMillis();
                try (var stream = java.nio.file.Files.walk(blobsSourceDir.toPath())) {
                    stream.filter(java.nio.file.Files::isRegularFile)
                        .forEach(path -> {
                            String fileName = path.getFileName().toString();
                            if (!fileName.contains(".tmp.") && !keepNames.contains(fileName)) {
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

            response.status(HttpStatus.OK_200);
            return Map.of(
                "status", "COMPLETED",
                "deletedOrphanBlobs", deleted[0]
            );
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
