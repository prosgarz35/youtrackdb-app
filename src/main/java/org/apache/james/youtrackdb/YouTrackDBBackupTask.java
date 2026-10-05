package org.apache.james.youtrackdb;

import java.io.File;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Instant;
import java.util.Optional;

import org.apache.james.task.Task;
import org.apache.james.task.TaskExecutionDetails;
import org.apache.james.task.TaskType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;

public class YouTrackDBBackupTask implements Task {
    private static final Logger LOGGER = LoggerFactory.getLogger(YouTrackDBBackupTask.class);
    public static final TaskType TASK_TYPE = TaskType.of("youtrackdb-backup");

    public static class AdditionalInformation implements TaskExecutionDetails.AdditionalInformation {
        private final Instant timestamp;
        private final String backupDir;
        private final long sizeBytes;

        public AdditionalInformation(Instant timestamp, String backupDir, long sizeBytes) {
            this.timestamp = timestamp;
            this.backupDir = backupDir;
            this.sizeBytes = sizeBytes;
        }

        @Override
        public Instant timestamp() {
            return timestamp;
        }

        public String getBackupDir() {
            return backupDir;
        }

        public long getSizeBytes() {
            return sizeBytes;
        }
    }

    private final YTDBGraphTraversalSource traversalSource;
    private final File backupDir;
    private volatile AdditionalInformation additionalInformation;

    public YouTrackDBBackupTask(YTDBGraphTraversalSource traversalSource, File backupDir) {
        this.traversalSource = traversalSource;
        this.backupDir = backupDir;
        this.additionalInformation = new AdditionalInformation(Clock.systemUTC().instant(), backupDir.getAbsolutePath(), 0);
    }

    @Override
    public Result run() {
        try {
            if (!backupDir.exists() && !backupDir.mkdirs()) {
                throw new IllegalStateException("Cannot create backup directory: " + backupDir.getAbsolutePath());
            }

            LOGGER.info("Executing YouTrackDB online hot backup task into {}", backupDir.getAbsolutePath());
            Path targetPath = backupDir.toPath();
            traversalSource.backup(targetPath);
            LOGGER.info("YouTrackDB backup completed into {}", backupDir.getAbsolutePath());

            long totalSize = 0;
            File[] files = backupDir.listFiles();
            if (files != null) {
                for (File f : files) {
                    totalSize += f.length();
                }
            }

            this.additionalInformation = new AdditionalInformation(
                Clock.systemUTC().instant(),
                backupDir.getAbsolutePath(),
                totalSize);

            return Result.COMPLETED;
        } catch (Exception e) {
            LOGGER.error("YouTrackDB backup task failed", e);
            return Result.PARTIAL;
        }
    }

    @Override
    public TaskType type() {
        return TASK_TYPE;
    }

    @Override
    public Optional<TaskExecutionDetails.AdditionalInformation> details() {
        return Optional.ofNullable(additionalInformation);
    }
}
