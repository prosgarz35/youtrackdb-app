# ⚡ James YouTrack Mail Server

[![Java 21](https://img.shields.io/badge/Java-21-ED8B00?style=for-the-badge&logo=openjdk&logoColor=white)](https://www.microsoft.com/openjdk)
[![Apache James 3.10.0 (master)](https://img.shields.io/badge/Apache%20James-3.10.0%20(master)-D22128?style=for-the-badge&logo=apache&logoColor=white)](https://github.com/apache/james-project)
[![YouTrackDB 0.5.0 (develop)](https://img.shields.io/badge/YouTrackDB-0.5.0%20(develop)-blueviolet?style=for-the-badge&logo=jetbrains&logoColor=white)](https://github.com/JetBrains/youtrackdb)

## Overview

**James YouTrack Mail Server** is a modern, autonomous next-generation mail server appliance combining **[Apache James](https://github.com/apache/james-project)** mail protocols with the embedded multi-model graph database **[JetBrains YouTrackDB](https://github.com/JetBrains/youtrackdb)**.

It delivers a complete enterprise-grade mail stack running within a single JVM process, requiring no external databases, message brokers, or dedicated database administrators.

---

### 🌟 Why This Architecture Outperforms Traditional Mail Servers

1. **Single In-VM Process (True Appliance Architecture)**
   - No external database clusters (PostgreSQL, MySQL, Cassandra) to maintain or operate.
   - Mail server and database engine share the same JVM address space: zero network latency, no TCP socket serialization, and zero connection pool overhead.

2. **Multi-Model Graph & Document Engine (Graph + YQL + Document)**
   - Powered by YouTrackDB optimized for ultra-low latency: direct B-Tree graph traversals, precise declarative queries with YQL (YouTrackDB SQL), and off-heap direct memory management.
   - Instant startup with predictable tail latencies even under massive mail volumes.

3. **Strict Durability & Resilience (ACID & WAL)**
   - Built-in crash resilience against sudden power loss through a dedicated Write-Ahead Log (WAL) and synchronous transaction commits.
   - Every message is safely persisted to journal disk pages before issuing the `250 OK` acknowledgment to the client.

4. **Two-Tier Storage & Transparent Compression**
   - Transparent message body compression using Zstandard (Zstd) paired with three-tier content-addressed blob sharding.
   - Minimal SSD/NVMe wear via batched group page flushing and resident memory page caching.

5. **Strict IMAP4rev1 & RFC 9051 IMAP4rev2 Compliance**
   - Out-of-the-box dual-standard support (RFC 3501 and RFC 9051) with dynamic capability negotiation (`ENABLE IMAP4rev2`, `UNAUTHENTICATE`, automatic `ESEARCH`, suppression of deprecated `RECENT`).

---

### 🛠️ Supported Standards & Tech Stack

* **Database Engine**: JetBrains YouTrackDB 0.5.0 (`develop` branch) with direct B-Tree indexing and declarative YQL.
* **Mail Core**: Apache James 3.10.0 (`master` branch).
* **Platform**: Java 21 ([Microsoft Build of OpenJDK](https://www.microsoft.com/openjdk)).
* **Network Protocols**:
  * **SMTP / SMTPS**: RFC 5321, RFC 4954 (Auth), RFC 3207 (STARTTLS) on ports 25, 465, 587.
  * **Email Format**: RFC 5322 (Internet Message Format) & MIME RFC 2045–2049.
  * **IMAP4rev1 & IMAP4rev2**: RFC 3501 and RFC 9051 on ports 143, 993.
  * **IMAP Quotas & Metadata**: RFC 9208 (QUOTA) and RFC 5464 (METADATA).
  * **ManageSieve**: RFC 5804 on port 4190.
  * **WebAdmin REST API**: Port 8000.

---

### 🚀 Building & Running

#### Requirements
* **Java 21** ([Microsoft Build of OpenJDK](https://www.microsoft.com/openjdk))
* **Maven 3.9+**

#### Building from Source
The repository is fully standalone and ships with the bundled SNAPSHOT artifacts in `repo/`. No external repositories or clone steps are required:

```bash
git clone https://github.com/prosgarz35/youtrackdb-app.git
cd youtrackdb-app
mvn clean package -DskipTests
```

#### Launching the Server
The launcher scripts automatically set the verified production defaults (`-XX:+UseZGC -XX:+ZGenerational -Xms3g -Xmx3g`) and load properties from `conf/jvm.properties` (or `sample-configuration/jvm.properties`).

* **Linux / macOS:**
  ```bash
  ./run.sh
  ```
* **Windows:**
  ```powershell
  .\run.bat
  ```

To override heap memory or GC options, pass the standard `JAVA_OPTS` variable:
```bash
JAVA_OPTS="-XX:+UseG1GC -Xms3g -Xmx3g" ./run.sh
```

Alternatively, launch directly via `java`:
```bash
java -XX:+UseZGC -XX:+ZGenerational -Xms3g -Xmx3g -Dextra.props=conf/jvm.properties -jar target/james-server-youtrackdb-app.jar
```

---

### 🛡️ WebAdmin Administration & Operations

#### Database Health & Integrity Check
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

#### Online Hot Backup (Disaster Recovery)
```bash
curl -X POST "http://localhost:8000/youtrackdb/backup?backupDir=var/backups"
```

#### Orphan Blobs Garbage Collection
```bash
curl -X POST http://localhost:8000/youtrackdb/blobs/gc
```
