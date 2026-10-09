package org.apache.james.youtrackdb;

import java.io.Closeable;
import java.io.File;
import java.io.FileNotFoundException;
import java.util.Map;

import jakarta.annotation.PreDestroy;
import jakarta.inject.Inject;

import org.apache.commons.configuration2.Configuration;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.james.filesystem.api.FileSystem;
import org.apache.james.server.core.configuration.ConfigurationProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.inject.AbstractModule;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.jetbrains.youtrackdb.api.DatabaseType;
import com.jetbrains.youtrackdb.api.YouTrackDB;
import com.jetbrains.youtrackdb.api.YourTracks;
import com.jetbrains.youtrackdb.api.gremlin.YTDBGraphTraversalSource;

public class YouTrackDBCommonModule extends AbstractModule {
    private static final Logger LOGGER = LoggerFactory.getLogger(YouTrackDBCommonModule.class);
    private static final String DEFAULT_PATH = "var/youtrackdb";
    private static final String DB_NAME = "james";
    private static final String DB_USER = "admin";
    private static final String DB_PASS = "admin";

    @Singleton
    public static class YouTrackDBHolder implements Closeable {
        private final YouTrackDB youTrackDB;
        private final YTDBGraphTraversalSource traversalSource;

        @Inject
        public YouTrackDBHolder(ConfigurationProvider configurationProvider, FileSystem fileSystem) throws FileNotFoundException {
            String path = DEFAULT_PATH;
            String dbUser = DB_USER;
            String dbPass = DB_PASS;
            Configuration ytdbConfig = new org.apache.commons.configuration2.BaseConfiguration();
            // High-throughput storage defaults for mail workloads (strict ACID, zero loss)
            ytdbConfig.setProperty("youtrackdb.storage.diskCache.bufferSize", 2048);
            ytdbConfig.setProperty("youtrackdb.storage.diskCache.writeCachePart", 15);
            ytdbConfig.setProperty("youtrackdb.storage.diskCache.writeCachePageFlushInterval", 25);
            ytdbConfig.setProperty("youtrackdb.storage.wal.bufferSize", 128);
            ytdbConfig.setProperty("youtrackdb.storage.wal.cacheSize", 65536);
            ytdbConfig.setProperty("youtrackdb.storage.wal.commitTimeout", 50);
            ytdbConfig.setProperty("youtrackdb.memory.directMemory.preallocate", true);
            ytdbConfig.setProperty("youtrackdb.statement.cacheSize", 500);
            String dbName = DB_NAME;
            try {
                Configuration conf = configurationProvider.getConfiguration("youtrackdb");
                path = conf.getString("youtrackdb.path", DEFAULT_PATH);
                dbName = conf.getString("youtrackdb.database", DB_NAME);
                dbUser = conf.getString("youtrackdb.user", DB_USER);
                dbPass = conf.getString("youtrackdb.password", DB_PASS);
                // Merge overrides from configuration file
                var keys = conf.getKeys();
                while (keys.hasNext()) {
                    String k = keys.next();
                    ytdbConfig.setProperty(k, conf.getProperty(k));
                }
            } catch (ConfigurationException e) {
                LOGGER.info("youtrackdb.properties not found, using default settings with path {}", DEFAULT_PATH);
            }

            File dir = new File(path).isAbsolute() ? new File(path) : new File(fileSystem.getBasedir(), path);
            if (!dir.exists()) {
                dir.mkdirs();
            }

            // Create custom WAL directory if configured
            String customWalPath = ytdbConfig.getString("youtrackdb.storage.wal.path", null);
            if (customWalPath != null && !customWalPath.isBlank()) {
                File walDir = new File(customWalPath).isAbsolute() ? new File(customWalPath) : new File(fileSystem.getBasedir(), customWalPath);
                if (!walDir.exists()) {
                    walDir.mkdirs();
                }
                ytdbConfig.setProperty("youtrackdb.storage.wal.path", walDir.getAbsolutePath());
                LOGGER.info("Using dedicated WAL path for YouTrackDB: {}", walDir.getAbsolutePath());
            }

            LOGGER.info("Initializing embedded YouTrackDB environment at {}", dir.getAbsolutePath());
            YouTrackDB ytdb = null;
            YTDBGraphTraversalSource ts = null;
            try {
                ytdb = YourTracks.instance(dir.getAbsolutePath(), ytdbConfig);
                ytdb.createIfNotExists(dbName, DatabaseType.DISK, ytdbConfig, dbUser, dbPass, "admin");
                ts = ytdb.openTraversal(dbName, dbUser, dbPass);
                initSchema(ts);
                this.youTrackDB = ytdb;
                this.traversalSource = ts;
            } catch (Exception e) {
                if (ts != null) {
                    try {
                        ts.close();
                    } catch (Exception ignored) {
                    }
                }
                if (ytdb != null && ytdb.isOpen()) {
                    try {
                        ytdb.close();
                    } catch (Exception ignored) {
                    }
                }
                throw e;
            }
        }

        private void initSchema(YTDBGraphTraversalSource traversalSource) {
            try {
                LOGGER.info("Verifying/initializing schema in YouTrackDB");
                // Class for Users
                traversalSource.executeInTx(g -> {
                    g.command("CREATE CLASS JamesUser IF NOT EXISTS EXTENDS V");
                    g.command("CREATE PROPERTY JamesUser.username IF NOT EXISTS STRING");
                    g.command("CREATE PROPERTY JamesUser.password IF NOT EXISTS STRING");
                    g.command("CREATE PROPERTY JamesUser.algorithm IF NOT EXISTS STRING");
                    g.command("CREATE INDEX JamesUser.username IF NOT EXISTS UNIQUE");

                    // Class for Domains
                    g.command("CREATE CLASS JamesDomain IF NOT EXISTS EXTENDS V");
                    g.command("CREATE PROPERTY JamesDomain.domain IF NOT EXISTS STRING");
                    g.command("CREATE INDEX JamesDomain.domain IF NOT EXISTS UNIQUE");

                    // Class for RRT Mappings
                    g.command("CREATE CLASS JamesRRTMapping IF NOT EXISTS EXTENDS V");
                    g.command("CREATE PROPERTY JamesRRTMapping.source IF NOT EXISTS STRING");
                    g.command("CREATE PROPERTY JamesRRTMapping.mapping IF NOT EXISTS STRING");
                    // Composite index on (source, mapping) serves point lookups, uniqueness, and prefix lookups on source
                    g.command("CREATE INDEX JamesRRTMapping.sourceAndMapping IF NOT EXISTS ON JamesRRTMapping (source, mapping) UNIQUE");

                    // Class for Blobs
                    g.command("CREATE CLASS JamesBlob IF NOT EXISTS EXTENDS V");
                    g.command("CREATE PROPERTY JamesBlob.bucketAndBlobId IF NOT EXISTS STRING");
                    g.command("CREATE PROPERTY JamesBlob.bucket IF NOT EXISTS STRING");
                    g.command("CREATE PROPERTY JamesBlob.blobId IF NOT EXISTS STRING");
                    g.command("CREATE PROPERTY JamesBlob.storageType IF NOT EXISTS STRING");
                    g.command("CREATE PROPERTY JamesBlob.payload IF NOT EXISTS BINARY");
                    g.command("CREATE INDEX JamesBlob.bucketAndBlobId IF NOT EXISTS UNIQUE");
                    // Composite index on (bucket, blobId) serves prefix and range scans inside a bucket: see YouTrackDBBlobStoreDAO.listBlobs
                    g.command("CREATE INDEX JamesBlob.bucketAndBlobIdRange IF NOT EXISTS ON JamesBlob (bucket, blobId) NOTUNIQUE");

                    // Class for MailQueue Items
                    g.command("CREATE CLASS JamesQueueItem IF NOT EXISTS EXTENDS V");
                    g.command("CREATE PROPERTY JamesQueueItem.enqueueId IF NOT EXISTS STRING");
                    g.command("CREATE PROPERTY JamesQueueItem.queueName IF NOT EXISTS STRING");
                    g.command("CREATE PROPERTY JamesQueueItem.mailName IF NOT EXISTS STRING");
                    g.command("CREATE PROPERTY JamesQueueItem.nextDelivery IF NOT EXISTS LONG");
                    g.command("CREATE PROPERTY JamesQueueItem.serializedMail IF NOT EXISTS BINARY");
                    g.command("CREATE INDEX JamesQueueItem.enqueueId IF NOT EXISTS UNIQUE");
                    g.command("CREATE INDEX JamesQueueItem.queueAndMail IF NOT EXISTS ON JamesQueueItem (queueName, mailName) NOTUNIQUE");
                    g.command("CREATE INDEX JamesQueueItem.queueAndDelivery IF NOT EXISTS ON JamesQueueItem (queueName, nextDelivery) NOTUNIQUE");

                    // Class for Mailbox
                    g.command("CREATE CLASS JamesMailbox IF NOT EXISTS EXTENDS V");
                    g.command("CREATE PROPERTY JamesMailbox.mailboxId IF NOT EXISTS STRING");
                    g.command("CREATE PROPERTY JamesMailbox.namespace IF NOT EXISTS STRING");
                    g.command("CREATE PROPERTY JamesMailbox.user IF NOT EXISTS STRING");
                    g.command("CREATE PROPERTY JamesMailbox.name IF NOT EXISTS STRING");
                    g.command("CREATE PROPERTY JamesMailbox.uidValidity IF NOT EXISTS LONG");
                    g.command("CREATE PROPERTY JamesMailbox.lastUid IF NOT EXISTS LONG");
                    g.command("CREATE PROPERTY JamesMailbox.highestModSeq IF NOT EXISTS LONG");
                    g.command("CREATE PROPERTY JamesMailbox.acl IF NOT EXISTS STRING");
                    g.command("CREATE INDEX JamesMailbox.mailboxId IF NOT EXISTS UNIQUE");
                    g.command("CREATE INDEX JamesMailbox.path IF NOT EXISTS ON JamesMailbox (namespace, user, name) UNIQUE");

                    // Class for Subscriptions
                    g.command("CREATE CLASS JamesSubscription IF NOT EXISTS EXTENDS V");
                    g.command("CREATE PROPERTY JamesSubscription.user IF NOT EXISTS STRING");
                    g.command("CREATE PROPERTY JamesSubscription.mailbox IF NOT EXISTS STRING");
                    g.command("CREATE INDEX JamesSubscription.userAndMailbox IF NOT EXISTS ON JamesSubscription (user, mailbox) UNIQUE");

                    // Class for Mailbox Messages
                    g.command("CREATE CLASS JamesMailboxMessage IF NOT EXISTS EXTENDS V");
                    g.command("CREATE PROPERTY JamesMailboxMessage.mailboxId IF NOT EXISTS STRING");
                    g.command("CREATE PROPERTY JamesMailboxMessage.messageId IF NOT EXISTS STRING");
                    g.command("CREATE PROPERTY JamesMailboxMessage.threadId IF NOT EXISTS STRING");
                    g.command("CREATE PROPERTY JamesMailboxMessage.uid IF NOT EXISTS LONG");
                    g.command("CREATE PROPERTY JamesMailboxMessage.modSeq IF NOT EXISTS LONG");
                    g.command("CREATE PROPERTY JamesMailboxMessage.internalDate IF NOT EXISTS LONG");
                    g.command("CREATE PROPERTY JamesMailboxMessage.saveDate IF NOT EXISTS LONG");
                    g.command("CREATE PROPERTY JamesMailboxMessage.size IF NOT EXISTS LONG");
                    g.command("CREATE PROPERTY JamesMailboxMessage.bodyStartOctet IF NOT EXISTS INTEGER");
                    g.command("CREATE PROPERTY JamesMailboxMessage.flags IF NOT EXISTS EMBEDDEDSET STRING");
                    g.command("CREATE PROPERTY JamesMailboxMessage.userFlags IF NOT EXISTS EMBEDDEDSET STRING");
                    g.command("CREATE PROPERTY JamesMailboxMessage.content IF NOT EXISTS BINARY");
                    g.command("CREATE INDEX JamesMailboxMessage.mailboxAndUid IF NOT EXISTS ON JamesMailboxMessage (mailboxId, uid) UNIQUE");
                    g.command("CREATE INDEX JamesMailboxMessage.mailboxAndFlags IF NOT EXISTS ON JamesMailboxMessage (mailboxId, flags) NOTUNIQUE");
                    g.command("CREATE INDEX JamesMailboxMessage.mailboxAndModSeq IF NOT EXISTS ON JamesMailboxMessage (mailboxId, modSeq) NOTUNIQUE");
                    g.command("CREATE INDEX JamesMailboxMessage.messageId IF NOT EXISTS NOTUNIQUE");

                    // Classes for Quotas
                    g.command("CREATE CLASS JamesQuotaLimit IF NOT EXISTS EXTENDS V");
                    g.command("CREATE PROPERTY JamesQuotaLimit.scope IF NOT EXISTS STRING");
                    g.command("CREATE PROPERTY JamesQuotaLimit.quotaKey IF NOT EXISTS STRING");
                    g.command("CREATE PROPERTY JamesQuotaLimit.maxStorage IF NOT EXISTS LONG");
                    g.command("CREATE PROPERTY JamesQuotaLimit.maxMessage IF NOT EXISTS LONG");
                    g.command("CREATE INDEX JamesQuotaLimit.scopeAndKey IF NOT EXISTS ON JamesQuotaLimit (scope, quotaKey) UNIQUE");

                    g.command("CREATE CLASS JamesQuotaUsage IF NOT EXISTS EXTENDS V");
                    g.command("CREATE PROPERTY JamesQuotaUsage.quotaRoot IF NOT EXISTS STRING");
                    g.command("CREATE PROPERTY JamesQuotaUsage.messageCount IF NOT EXISTS LONG");
                    g.command("CREATE PROPERTY JamesQuotaUsage.size IF NOT EXISTS LONG");
                    g.command("CREATE INDEX JamesQuotaUsage.quotaRoot IF NOT EXISTS ON JamesQuotaUsage (quotaRoot) UNIQUE");

                    // Class for MailRepository URLs
                    g.command("CREATE CLASS JamesMailRepositoryUrl IF NOT EXISTS EXTENDS V");
                    g.command("CREATE PROPERTY JamesMailRepositoryUrl.url IF NOT EXISTS STRING");
                    g.command("CREATE INDEX JamesMailRepositoryUrl.url IF NOT EXISTS ON JamesMailRepositoryUrl (url) UNIQUE");

                    // Class for Mailbox Annotations (RFC 5464)
                    g.command("CREATE CLASS JamesMailboxAnnotation IF NOT EXISTS EXTENDS V");
                    g.command("CREATE PROPERTY JamesMailboxAnnotation.mailboxId IF NOT EXISTS STRING");
                    g.command("CREATE PROPERTY JamesMailboxAnnotation.key IF NOT EXISTS STRING");
                    g.command("CREATE PROPERTY JamesMailboxAnnotation.value IF NOT EXISTS STRING");
                    g.command("CREATE INDEX JamesMailboxAnnotation.mailboxAndKey IF NOT EXISTS ON JamesMailboxAnnotation (mailboxId, key) UNIQUE");
                });

                // Validation: verify that all existing mailbox IDs follow canonical uppercase UUID format
                var rows = traversalSource.yql("SELECT mailboxId FROM JamesMailbox").toList();
                for (Object row : rows) {
                    String id = null;
                    if (row instanceof java.util.Map<?, ?> m) {
                        Object val = m.get("mailboxId");
                        if (val != null) {
                            id = val.toString();
                        }
                    } else if (row instanceof org.apache.tinkerpop.gremlin.structure.Vertex v) {
                        var p = v.property("mailboxId");
                        if (p.isPresent()) {
                            id = p.value().toString();
                        }
                    }
                    if (id != null) {
                        try {
                            java.util.UUID parsed = java.util.UUID.fromString(id);
                            if (!id.equals(parsed.toString().toUpperCase(java.util.Locale.US))) {
                                throw new IllegalStateException("Found non-canonical mailboxId in JamesMailbox: " + id
                                    + ". YouTrackDB James requires canonical uppercase UUID mailbox IDs.");
                            }
                        } catch (IllegalArgumentException e) {
                            throw new IllegalStateException("Found malformed mailboxId in JamesMailbox: " + id
                                + ". YouTrackDB James requires canonical uppercase UUID mailbox IDs.", e);
                        }
                    }
                }
            } catch (Exception e) {
                LOGGER.error("Schema initialization failed in YouTrackDB: {}", e.getMessage(), e);
                throw new RuntimeException("Fatal error: failed to initialize YouTrackDB schema", e);
            }
        }

        public YouTrackDB getYouTrackDB() {
            return youTrackDB;
        }

        public YTDBGraphTraversalSource getTraversalSource() {
            return traversalSource;
        }

        @Override
        @PreDestroy
        public void close() {
            LOGGER.info("Closing YouTrackDB TraversalSource and Manager");
            try {
                if (traversalSource != null) {
                    traversalSource.close();
                }
            } catch (Exception e) {
                LOGGER.warn("Error closing YouTrackDB TraversalSource", e);
            }
            try {
                if (youTrackDB != null && youTrackDB.isOpen()) {
                    youTrackDB.close();
                }
            } catch (Exception e) {
                LOGGER.warn("Error closing YouTrackDB manager", e);
            }
        }
    }

    @Override
    protected void configure() {
        bind(YouTrackDBHolder.class).asEagerSingleton();

        com.google.inject.multibindings.Multibinder.newSetBinder(binder(), org.apache.james.core.healthcheck.HealthCheck.class)
            .addBinding()
            .to(YouTrackDBHealthCheck.class);

        com.google.inject.multibindings.Multibinder.newSetBinder(binder(), org.apache.james.webadmin.Routes.class)
            .addBinding()
            .to(YouTrackDBAdminRoutes.class);
    }

    @Provides
    @Singleton
    YouTrackDB provideYouTrackDB(YouTrackDBHolder holder) {
        return holder.getYouTrackDB();
    }

    @Provides
    @Singleton
    YTDBGraphTraversalSource provideTraversalSource(YouTrackDBHolder holder) {
        return holder.getTraversalSource();
    }
}
