# ⚡ Apache James :: YouTrackDB Server (Embedded Graph & Hybrid DB Mail Server)

[![Java 21](https://img.shields.io/badge/Java-21%2B-ED8B00?style=for-the-badge&logo=openjdk&logoColor=white)](https://openjdk.org/)
[![Apache James 3.10](https://img.shields.io/badge/Apache%20James-3.10.0--SNAPSHOT-D22128?style=for-the-badge&logo=apache&logoColor=white)](https://james.apache.org/)
[![YouTrackDB](https://img.shields.io/badge/JetBrains-YouTrackDB%200.5.0-blueviolet?style=for-the-badge&logo=jetbrains&logoColor=white)](https://github.com/JetBrains/youtrackdb)
[![Zstd Compression](https://img.shields.io/badge/Storage-Transparent%20Zstd-27AE60?style=for-the-badge)](https://facebook.github.io/zstd/)
[![License: Apache 2.0](https://img.shields.io/badge/License-Apache%202.0-blue.svg?style=for-the-badge)](https://www.apache.org/licenses/LICENSE-2.0)

## Documentation

**Apache James YouTrackDB Server** is an enterprise-grade, self-contained mail server engineered on top of **JetBrains YouTrackDB** — a next-generation multi-model embedded database engine combining TinkerPop graph traversal, document storage, unique B-Tree indexing, direct memory management, and declarative **YQL (YouTrackDB SQL)** queries.

It provides a modern appliance architecture: **zero external database dependencies**, **zero DBA maintenance overhead**, instant deployment, transparent Zstd compression for message bodies, ACID durability for database records (WAL), and **about 2.4x the throughput of the PostgreSQL 17 variant in the bundled benchmark** (see the table below: one machine, 5,000 messages).

---

### 🎯 Key Architectural Advantages & Strengths

#### 1. In-VM Architecture & Direct Memory Engine
* **Single Process / Single Directory**: The entire mail stack (SMTP, IMAP, Spooler, Queues, Mailbox, Search, and Storage) runs inside a single JVM process.
* **Persistent vs In-Memory Subsystems (Appliance Model)**:
  * **Strictly Persisted in YouTrackDB (ACID / WAL)**: User accounts & credentials (`JamesUser`), Domains (`JamesDomain`), Virtual Aliases & Rewrites (`JamesRRTMapping`), Spooler Queue items (`JamesQueueItem`), Content-Addressed Blob Storage (`JamesBlob` + Tier 3 Zstd file tree), Mailboxes (`JamesMailbox`), Messages & Flags (`JamesMailboxMessage`), Quotas (`JamesQuotaLimit`, `JamesQuotaUsage`), Subscriptions (`JamesSubscription`), Mailbox Annotations (`JamesMailboxAnnotation`, RFC 5464), and Mail Repository URLs (`JamesMailRepositoryUrl`). All survive full server restarts with strict ACID durability via WAL.
  * **In-Memory Buffering & Transient State**: Message dispatching uses an in-memory `DelayQueue` hydrated from persisted `JamesQueueItem` records on startup; full-text search utilizes embedded Lucene indexing; mail attachments are stored as MIME payload parts within the persisted messages and blob store. JMAP state metadata and transient dead-letter event trackers operate in-memory.
* **Direct Memory Pre-allocation**: Off-heap buffer caches with `memory.directMemory.preallocate = true` avoid on-the-fly JVM pause stalls and eliminate garbage collector pressure.
* **No Network IPC Overhead**: Completely eliminates serialization, network socket hops, TCP connection pool starvation, and context switching found in client-server architectures like PostgreSQL, MySQL, or Cassandra.
* **Zero DBA Footprint**: No background vacuuming stalls, no complex replication clustering, and no external schema migration scripts.

#### 2. Multi-Model Hybrid Storage (Graph + Relational YQL + Key-Value)
* **B-Tree Point Lookups via Gremlin DSL**: Single-entity reads and writes (`save`, `readBytes`, `enQueue`) utilize direct TinkerPop traversal without SQL lexing/parsing overhead.
* **Direct YQL Set-Based & Projection Acceleration**: Hot path lookups, administrative queries, and bulk operations execute directly via parameterized YQL:
  * `SELECT password, algorithm FROM JamesUser WHERE username = :uname LIMIT 1` (Fast authentication lookup)
  * `SELECT 1 FROM JamesUser WHERE username = :uname LIMIT 1` (Instant $O(1)$ user existence check)
  * `SELECT 1 FROM JamesDomain WHERE domain = :domain LIMIT 1` (Instant $O(1)$ domain existence check)
  * `SELECT mapping FROM JamesRRTMapping WHERE source = :src` (Direct routing projection from B-Tree index)
  * `SELECT source, mapping FROM JamesRRTMapping` (Fast alias table dump)
  * `SELECT domain FROM JamesDomain` and `SELECT username FROM JamesUser` (Scalar projections)
  * `SELECT count(*) AS total FROM JamesUser` (Instant $O(1)$ count directly from cluster page headers)
  * `SELECT blobId FROM JamesBlob WHERE bucket = :bucket`
  * `SELECT DISTINCT(bucket) AS bucket FROM JamesBlob`
  * `DELETE VERTEX JamesBlob WHERE bucketAndBlobId = ?`
  * `DELETE VERTEX JamesQueueItem WHERE queueName = ?` (Bulk queue purge)
  * `SELECT 1` (Zero-allocation engine health-check ping)
* **Persistent Spooler with In-Memory DelayQueue & YouTrackDB WAL**:
  * Persistent spooler queue stores all queue items as `JamesQueueItem` vertices in YouTrackDB with composite indices:
    `CREATE INDEX JamesQueueItem.queueAndMail IF NOT EXISTS ON JamesQueueItem (queueName, mailName) UNIQUE`
    `CREATE INDEX JamesQueueItem.queueAndDelivery IF NOT EXISTS ON JamesQueueItem (queueName, nextDelivery) NOTUNIQUE`
  * Startup recovery restores pending messages directly via Gremlin vertex traversal, hydrating them into an in-memory `DelayQueue` ordered by `nextDelivery` timestamp for microsecond dispatch latency.
  * RFC 5321 exponential retry backoffs (`enQueue(mail, delay)`) are handled seamlessly without stalling head-of-line messages.
  * Batch removal `remove(Type, value)` and successful completions execute in strict transactions (`YouTrackDBTransactions.executeStrictTx`: explicit commit, so commit failures reach the caller), keeping database state synchronized with minimal WAL overhead.

#### 3. Tiered Hybrid Blob Storage Pipeline
Storage is dynamically partitioned based on payload dimensions:
* **Tier 1 (< 4 KB)**: Small headers and raw metadata are written directly into YouTrackDB data pages ($O(1)$ key lookup, zero file I/O).
* **Tier 2 (4 KB .. 64 KB)**: High-speed Zstandard (level 1) compression stored inside database pages.
* **Tier 3 (> 64 KB)**: **Streaming to disk** — payloads larger than 64 KB are streamed from the input stream into `ZstdOutputStream` on disk with fixed-size buffers (64 KB probe + 8 KB transfer), so heap usage does not grow with the payload size. Files are organized using 3-level directory sharding (`var/blobs/{bucket}/ab/cd/ef/{blobId}`) with atomic durability (`ATOMIC_MOVE` + `fsync`), eliminating database fragmentation and WAL bloat.

#### 4. ACID Durability (Write-Ahead Log)
* **Committed before `250 OK`**: every mail queue item is committed (explicit commit) before `enQueue` returns, i.e. before the SMTP `250 OK`. Durability across a power loss additionally relies on `youtrackdb.storage.callFsync=true` (the engine default); it is not covered by the tests below.
* **Crash & Contention Resilient**: Validated by extensive stress tests under concurrency, abrupt thread deaths, and abrupt network dropouts (`YouTrackDBAcidCrashTest`) with no corrupted records observed. These tests close the database normally; they do not simulate a power loss or `kill -9`.
* **Non-Blocking Dispatch**: Uses an in-memory `DelayQueue` for microsecond dispatching while persisting the backing state on disk.

#### 5. Online Incremental Backups
* Native online backup triggered via `POST /youtrackdb/backup` on the WebAdmin REST API.
* Uses the engine's incremental backup (`traversalSource.backup(path)`: the first call copies the whole database, later calls into the same folder copy only the changes) and copies the `var/blobs` tree into the same folder. The two parts are taken one after another without a common lock, so under heavy write load the database and the blob files are not guaranteed to be consistent with each other. The server stays online during the backup.

---

### 📊 Benchmark: YouTrackDB vs. PostgreSQL 17.11

A head-to-head load benchmark was executed on the same hardware environment under identical test conditions:
* **Workload**: End-to-end SMTP mail injection ➔ spooling ➔ mailbox delivery ➔ IMAP verification.
* **Volume**: **5,000 messages** under 8 concurrent worker threads.
* **Storage Mode**: Full in-database storage (headers, envelope metadata, mail bodies, and attachments).

#### Head-to-Head Comparison Table

| Metric / Parameter | `postgres-app` (PostgreSQL 17.11) | `youtrackdb-app` (Initial Baseline) | `youtrackdb-app` (YQL + Streaming Optimized) | Advantage / Gain vs PostgreSQL |
| :--- | :---: | :---: | :---: | :---: |
| **Total Injected Messages** | 5,000 | 5,000 | **5,000** | — |
| **Delivery & Verification Rate** | **5,000 / 5,000 (100%)** | **5,000 / 5,000 (100%)** | **5,000 / 5,000 (100%)** | **100% Reliable** |
| **Failed Injections / Errors** | **0** | **0** | **0** | **No failed injections** |
| **Total Benchmark Time** | **32.70 s** (32,700 ms) | 17.50 s | **13.70 s** (13,701 ms) | **2.4x faster** (-58% elapsed time) |
| **Throughput** | **152.93 msgs/sec** | 280–300 msgs/sec | **364.94 msgs/sec** | **+138.6% (+212 msgs/sec)** |
| **Min Latency** | **7.0 ms** | 4.0 ms | **4.0 ms** | **-43% lower** |
| **Average Latency (Avg)** | **51.85 ms** | 26.80 ms | **21.31 ms** | **2.4x lower** (-59%) |
| **Median Latency (P50)** | **42.0 ms** | 21.0 ms | **18.0 ms** | **2.3x lower** (-57%) |
| **95th Percentile (P95)** | **126.0 ms** | 68.0 ms | **44.0 ms** | **2.9x lower** (-65%) |
| **99th Percentile (P99)** | **287.0 ms** | 120.0 ms | **76.0 ms** | **3.8x lower** (-73.5%) |
| **Max Latency (Tail)** | **3,909.0 ms** | 277.0 ms | **256.0 ms** | **15.3x lower** (predictable tail) |
| **ACID Durability** | Active fsync | Active fsync + WAL | **Full ACID / active fsync + WAL** | — |
| **Full Test Suite (`mvn test`)**| > 2 minutes | ~58 seconds | **~53 seconds** | **> 2x faster verification** |

---

### 🛠️ Technology Stack & RFC Standards

* **Database Engine**: JetBrains YouTrackDB (`io.youtrackdb:youtrackdb-core:0.5.0-SNAPSHOT`) with Apache TinkerPop Gremlin DSL and declarative YQL.
* **Authentication & Users**: `YouTrackDBUsersDAO` with PBKDF2 password hashing and unique B-Tree indexing on `JamesUser.username`.
* **Domain Management**: `YouTrackDBDomainList` enforcing standard domain normalization.
* **Virtual Aliases**: `YouTrackDBRecipientRewriteTable` supporting alias, regex, error, forward, and group mapping rules with direct YQL projections.
* **Full-Text Search**: Embedded Apache Lucene (`LuceneSearchMailboxModule`).
* **Supported RFC Standards**:
  * **SMTP / SMTPS**: RFC 5321, RFC 4954, RFC 3207 (Ports 25, 465, 587).
  * **Email Format**: RFC 5322 (Internet Message Format) & MIME RFC 2045–2049.
  * **IMAP4rev1**: RFC 3501 (Ports 143, 993).
  * **ManageSieve**: RFC 5804 (Port 4190).
  * **WebAdmin API**: Administrative REST API (Port 8000).

---

### 🚀 Building & Running

#### Requirements
* Java 21+ OpenJDK
* Maven 3.9+

#### Build from Sources
```bash
# 1. Build and install YouTrackDB core
git clone https://github.com/JetBrains/youtrackdb.git
cd youtrackdb
mvn clean install -DskipTests

# 2. Build youtrackdb-app
cd /path/to/youtrackdb-app
mvn clean package -Dcheckstyle.skip=true -DskipTests
```

#### Run Tests & Benchmarks
```bash
# Run all unit and integration tests
mvn clean test -Dcheckstyle.skip=true

# Run the 5,000-message load benchmark
mvn test -Dcheckstyle.skip=true -Dtest=YouTrackDBBenchmarkTest
```

#### Launch the Server
```bash
java -Dworking.directory=. -jar target/james-server-youtrackdb-app.jar
```

---

### 🛡️ WebAdmin Administration & Operations

#### Health & Integrity Check
```bash
curl -X GET http://localhost:8000/youtrackdb/check
```
*Response:*
```json
{
  "status": "HEALTHY",
  "databaseOpen": true,
  "totalBlobs": 5000,
  "totalUsers": 12,
  "totalDomains": 1
}
```

#### Standard James HealthCheck
```bash
curl -X GET http://localhost:8000/healthcheck
```
Includes the native `YouTrackDBHealthCheck` component reporting the live operational status of the embedded database engine directly in the standard JSON health response.

#### Triggering an Online Hot Backup
```bash
curl -X POST "http://localhost:8000/youtrackdb/backup?backupDir=var/backups"
```
The server runs the backup as a WebAdmin task while it stays online: an incremental backup of the database plus a copy of the `var/blobs` tree (not atomic across the two).

#### Blobs Garbage Collection (Orphan Blobs GC)
```bash
curl -X POST http://localhost:8000/youtrackdb/blobs/gc
```
*Response:*
```json
{
  "status": "COMPLETED",
  "deletedOrphanBlobs": 0
}
```
Traverses the content-addressed blob directory and securely purges unreferenced orphaned payload files, reclaiming disk space.

---

### ⚙️ Configuration & Storage Layout

Default data directory layout in `var/`:
* `var/youtrackdb/` — Embedded YouTrackDB graph database files, Lucene index segments, WAL, and in-database binary blobs (< 64 KB).
* `var/blobs/` — Sharded directory structure for large attachments & message bodies (> 64 KB) with transparent Zstd compression.
* `var/backups/` — Destination directory for online backups.

#### 🗄️ Mailbox Storage Format & Canonical Identifiers
* **`JamesMailbox.mailboxId` Format**: Mailbox identifiers strictly follow canonical uppercase UUID representation (e.g. `12345678-ABCD-EF01-2345-6789ABCDEF01`). 
* **Startup Integrity Guard**: During schema initialization, YouTrackDB James inspects existing `JamesMailbox` records and refuses to boot with an explicit error if any lowercase or malformed identifier is detected.

Optional configuration file: `conf/youtrackdb.properties`
```properties
# Custom path for YouTrackDB storage (defaults to var/youtrackdb)
youtrackdb.path=var/youtrackdb

# Dedicated path for Write-Ahead Log (WAL) to isolate sequential journal I/O from page cache I/O (optional)
# youtrackdb.storage.wal.path=/fast_wal_nvme/youtrackdb_wal

# High-throughput storage defaults for mail workloads (durable commits, tuned for mail workloads)
youtrackdb.storage.diskCache.bufferSize=2048
youtrackdb.storage.diskCache.writeCachePart=15
youtrackdb.storage.diskCache.writeCachePageFlushInterval=25
youtrackdb.storage.wal.bufferSize=128
youtrackdb.storage.wal.cacheSize=65536
youtrackdb.storage.wal.commitTimeout=50
```
