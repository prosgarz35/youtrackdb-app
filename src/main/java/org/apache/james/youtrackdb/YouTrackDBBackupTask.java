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

    public record AdditionalInformation(Instant timestamp, String backupDir, long sizeBytes)
        implements TaskExecutionDetails.AdditionalInformation {

        public String getBackupDir() {
            return backupDir;
        }

        public long getSizeBytes() {
            return sizeBytes;
        }
    }

    private final YTDBGraphTraversalSource traversalSource;
    private final File backupDir;
    private final File blobsSourceDir;
    private volatile AdditionalInformation additionalInformation;

    public YouTrackDBBackupTask(YTDBGraphTraversalSource traversalSource, File backupDir, File blobsSourceDir) {
        this.traversalSource = traversalSource;
        this.backupDir = backupDir;
        this.blobsSourceDir = blobsSourceDir;
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
            LOGGER.info("YouTrackDB database backup completed into {}", backupDir.getAbsolutePath());

            // Backup filesystem blobs alongside the database snapshot
            if (blobsSourceDir != null && blobsSourceDir.exists()) {
                Path blobsBackupDir = targetPath.resolve("blobs");
                LOGGER.info("Backing up filesystem blobs from {} to {}", blobsSourceDir.getAbsolutePath(), blobsBackupDir.toAbsolutePath());
                copyDirectoryRecursively(blobsSourceDir.toPath(), blobsBackupDir);
            }

            long totalSize = calculateDirectorySize(targetPath);

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

    private static void copyDirectoryRecursively(Path source, Path target) throws java.io.IOException {
        if (!java.nio.file.Files.exists(source)) {
            return;
        }
        try (var stream = java.nio.file.Files.walk(source)) {
            stream.forEach(src -> {
                if (src.getFileName() != null && src.getFileName().toString().contains(".tmp.")) {
                    return;
                }
                try {
                    Path dest = target.resolve(source.relativize(src));
                    if (java.nio.file.Files.isDirectory(src)) {
                        if (!java.nio.file.Files.exists(dest)) {
                            java.nio.file.Files.createDirectories(dest);
                        }
                    } else {
                        if (dest.getParent() != null && !java.nio.file.Files.exists(dest.getParent())) {
                            java.nio.file.Files.createDirectories(dest.getParent());
                        }
                        try {
                            java.nio.file.Files.copy(src, dest, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
                        } catch (java.nio.file.NoSuchFileException ignored) {
                            // File was concurrently deleted or rotated: safe to skip during live snapshot
                        }
                    }
                } catch (java.io.IOException e) {
                    throw new RuntimeException("Failed copying blob file during backup: " + src, e);
                }
            });
        }
    }

    private static long calculateDirectorySize(Path path) {
        if (!java.nio.file.Files.exists(path)) {
            return 0;
        }
        try (var stream = java.nio.file.Files.walk(path)) {
            return stream.filter(p -> !java.nio.file.Files.isDirectory(p))
                .mapToLong(p -> {
                    try {
                        return java.nio.file.Files.size(p);
                    } catch (Exception e) {
                        return 0L;
                    }
                }).sum();
        } catch (Exception e) {
            return 0;
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
