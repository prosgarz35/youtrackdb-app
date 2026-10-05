# ⚡ Apache James :: YouTrackDB Server (Embedded Graph DB Mail Server)

[![Java 21](https://img.shields.io/badge/Java-21%2B-ED8B00?style=for-the-badge&logo=openjdk&logoColor=white)](https://openjdk.org/)
[![YouTrackDB](https://img.shields.io/badge/JetBrains-YouTrackDB%200.5.0-blueviolet?style=for-the-badge&logo=jetbrains&logoColor=white)](https://github.com/JetBrains/youtrackdb)
[![Zstd Compression](https://img.shields.io/badge/Storage-Transparent%20Zstd-27AE60?style=for-the-badge)](https://facebook.github.io/zstd/)

**Apache James YouTrackDB Server** — легковесный автономный почтовый сервер на базе объектно-ориентированной графовой базы данных **JetBrains YouTrackDB** (активный преемник JetBrains Xodus / VDB).

---

## 🚀 Архитектура и стек

- **Core DB**: `io.youtrackdb:youtrackdb-core` (0.5.0-SNAPSHOT).
- **Embedded Engine**: `YourTracks.instance(dir)` c транзакционным Gremlin DSL `YTDBGraphTraversalSource`.
- **Users & Auth**: `YouTrackDBUsersDAO` (класс `JamesUser`, уникальный B-Tree индекс на `username`, хэширование PBKDF2/Argon2).
- **Domains**: `YouTrackDBDomainList` (класс `JamesDomain`, уникальный B-Tree индекс).
- **Virtual Aliases (RRT)**: `YouTrackDBRecipientRewriteTable` (класс `JamesRRTMapping`, составной индекс).
- **Blob Storage Pipeline**: `DeDuplicationBlobStore (SHA-256)` ➔ `ZstdBlobStoreDAO` ➔ `YouTrackDBBlobStoreDAO` (класс `JamesBlob`, хранение бинарных пэйлоадов с $O(1)$ выборкой по `bucketAndBlobId`).
- **Protocols**: SMTP (25, 465, 587), IMAP4rev1 (143, 993), ManageSieve (4190), WebAdmin REST API.

---

## 📦 Сборка и тестирование

```bash
cd D:\projects\youtrackdb-app
mvn clean test -Dcheckstyle.skip=true
```
