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
        service.get(YOUTRACKDB_BASE_PATH + "/check", this::checkIntegrity, jsonTransformer);
    }

    private YouTrackDBBackupTask createBackupTask(Request request) throws FileNotFoundException {
        String backupDirParam = request.queryParams("backupDir");
        File backupDir;
        if (backupDirParam != null && !backupDirParam.isBlank()) {
            backupDir = new File(backupDirParam);
        } else {
            backupDir = new File(fileSystem.getBasedir(), "var/backups");
        }
        return new YouTrackDBBackupTask(traversalSource, backupDir);
    }

    private Object checkIntegrity(Request request, Response response) {
        try {
            boolean dbOpen = youTrackDB.isOpen();
            long[] counts = {0, 0, 0};
            if (dbOpen) {
                try {
                    counts[0] = traversalSource.computeInTx(tx -> tx.V().hasLabel(YouTrackDBBlobStoreDAO.CLASS_NAME).count().next());
                    counts[1] = traversalSource.computeInTx(tx -> tx.V().hasLabel("JamesUser").count().next());
                    counts[2] = traversalSource.computeInTx(tx -> tx.V().hasLabel("JamesDomain").count().next());
                } catch (Exception ignored) {
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
}
