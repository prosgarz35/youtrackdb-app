# ⚡ James YouTrack Mail Server

[![Java 21](https://img.shields.io/badge/Java-21-ED8B00?style=for-the-badge&logo=openjdk&logoColor=white)](https://www.microsoft.com/openjdk)
[![Apache James 3.10.0 (master)](https://img.shields.io/badge/Apache%20James-3.10.0%20(master)-D22128?style=for-the-badge&logo=apache&logoColor=white)](https://github.com/apache/james-project)
[![YouTrackDB 0.5.0 (develop)](https://img.shields.io/badge/YouTrackDB-0.5.0%20(develop)-blueviolet?style=for-the-badge&logo=jetbrains&logoColor=white)](https://github.com/JetBrains/youtrackdb)

## Overview

**James YouTrack Mail Server** — это современный, полностью автономный почтовый сервер нового поколения, созданный на стыке стека **[Apache James](https://github.com/apache/james-project)** и встраиваемой мультимодельной графовой базы данных **[JetBrains YouTrackDB](https://github.com/JetBrains/youtrackdb)**.

Решение объединяет полный спектр почтовых протоколов корпоративного уровня в едином процессе без потребности во внешних базах данных и системных администраторах СУБД.

---

### 🌟 Почему это решение опережает классические почтовые серверы

1. **Единый In-VM процесс (True Appliance Architecture)**
   - Больше никаких тяжелых внешних кластеров СУБД (PostgreSQL, MySQL, Cassandra). 
   - Сервер почты и база данных работают в общем адресном пространстве одного процесса JVM: нулевой сетевой оверхед, отсутствие сериализации через TCP-сокеты и накладных расходов на пулы соединений.

2. **Мультимодельная база данных (Graph + YQL + Document)**
   - База данных YouTrackDB оптимизирована под сверхнизкие задержки: прямой обход графа связей через B-Tree, точечные запросы через декларативный язык YQL (YouTrackDB SQL) и прямой доступ к памяти (Direct Memory).
   - Быстрый запуск, отсутствие блокировок и деградации производительности на больших объемах почты.

3. **Строгая надежность и сохранность данных (ACID & WAL)**
   - Защита от сбоев питания и аварийных остановок благодаря полноценному Write-Ahead-Log (WAL) и синхронной фиксации транзакций.
   - Любое письмо гарантированно сохранено в журнале до ответа клиенту `250 OK`.

4. **Двухуровневое сжатие и эффективное дисковое хранилище**
   - Прозрачное сжатие тел писем через Zstandard (Zstd) и трехуровневое шардирование контента.
   - Минимальный износ дисков (SSD/NVMe) за счет пакетной групповой записи и резидентных страниц в памяти.

5. **Полная совместимость со стандартами IMAP4rev1 и IMAP4rev2**
   - Честная поддержка обоих стандартов (RFC 3501 и RFC 9051) с динамическим переключением возможностей (`ENABLE IMAP4rev2`, `UNAUTHENTICATE`, автоматический `ESEARCH`, подавление устаревшего `RECENT`) «из коробки» без дополнительных конфигураций.

---

### 🛠️ Поддерживаемые стандарты и стек

* **База данных**: JetBrains YouTrackDB 0.5.0 (ветка `develop`) с прямым B-Tree доступом и декларативным YQL.
* **Почтовое ядро**: Apache James 3.10.0 (ветка `master`).
* **Платформа**: Java 21 (Microsoft Build of OpenJDK).
* **Сетевые протоколы**:
  * **SMTP / SMTPS**: RFC 5321, RFC 4954 (Auth), RFC 3207 (STARTTLS) на портах 25, 465, 587.
  * **Email Format**: RFC 5322 (Internet Message Format) & MIME RFC 2045–2049.
  * **IMAP4rev1 & IMAP4rev2**: RFC 3501 и RFC 9051 на портах 143, 993.
  * **IMAP Quotas & Metadata**: RFC 9208 (QUOTA) и RFC 5464 (METADATA).
  * **ManageSieve**: RFC 5804 на порту 4190.
  * **WebAdmin REST API**: порт 8000.

---

### 🚀 Сборка и запуск

#### Требования
* **Java 21** ([Microsoft Build of OpenJDK](https://www.microsoft.com/openjdk))
* **Maven 3.9+**

#### Сборка из исходников
```bash
# 1. Сборка ядра YouTrackDB
git clone https://github.com/JetBrains/youtrackdb.git
cd youtrackdb
mvn clean install -DskipTests

# 2. Сборка почтового сервера
cd /path/to/youtrackdb-app
mvn clean package -DskipTests
```

#### Запуск сервера
* **Linux / macOS:**
  ```bash
  java -XX:+UseZGC -XX:+ZGenerational -Xms2g -Xmx4g -jar target/james-server-youtrackdb-app.jar
  ```
* **Windows:**
  ```powershell
  java -XX:+UseZGC -XX:+ZGenerational -Xms2g -Xmx4g -jar .\target\james-server-youtrackdb-app.jar
  ```

---

### 🛡️ Управление и мониторинг через WebAdmin

#### Проверка состояния базы данных
```bash
curl -X GET http://localhost:8000/youtrackdb/check
```
*Ответ:*
```json
{
  "status": "HEALTHY",
  "databaseOpen": true,
  "totalBlobs": 5000,
  "totalUsers": 12,
  "totalDomains": 1
}
```

#### Горячий бэкап (Disaster Recovery)
```bash
curl -X POST "http://localhost:8000/youtrackdb/backup?backupDir=var/backups"
```

#### Очистка осиротевших блобов (Garbage Collection)
```bash
curl -X POST http://localhost:8000/youtrackdb/blobs/gc
```
