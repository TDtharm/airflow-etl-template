# ODBC vs JDBC vs Native Connector

เปรียบเทียบ 3 แนวทางเชื่อมต่อ Database — เมื่อไหร่ใช้อะไร

---

## Overview

```
┌──────────────┐     ┌──────────────┐     ┌──────────────┐
│   Native     │     │     ODBC     │     │     JDBC     │
│  Connector   │     │  Connector   │     │  Connector   │
├──────────────┤     ├──────────────┤     ├──────────────┤
│ Python lib   │     │ Python lib   │     │ Java lib     │
│ (psycopg2,   │     │ (pyodbc)     │     │ (JDBC driver)│
│  pymssql,    │     │      ↓       │     │      ↓       │
│  impyla)     │     │ ODBC Driver  │     │ JVM + JDBC   │
│      ↓       │     │ Manager      │     │ Driver       │
│ Wire protocol│     │ (unixODBC)   │     │      ↓       │
│      ↓       │     │      ↓       │     │ Wire protocol│
│   Database   │     │   Database   │     │   Database   │
└──────────────┘     └──────────────┘     └──────────────┘
```

---

## เปรียบเทียบ

| | Native Connector | ODBC | JDBC |
|---|---|---|---|
| **ภาษา** | Python native | C/C++ driver + Python wrapper | Java |
| **Dependencies** | pip install เท่านั้น | System driver (apt/yum) + pip | JVM + JAR file |
| **Setup** | ง่ายสุด | กลาง (ต้อง config DSN) | ยาก (ต้อง JVM + classpath) |
| **Performance** | เร็วสุด (direct protocol) | เร็ว (thin layer overhead) | ช้าสุด (JVM overhead) |
| **Portability** | เฉพาะ DB นั้น | 1 API → หลาย DB | 1 API → หลาย DB |
| **ใช้ใน Python** | ✅ ง่าย | ✅ ง่าย (pyodbc) | ⚠️ ต้อง JayDeBeApi/jpype |
| **Docker** | image เล็ก | ต้อง install driver | ต้อง install JRE + JAR |

---

## Native Connector (ใช้ใน template)

Library ที่ implement wire protocol ของ DB โดยตรง — ไม่ผ่าน middleware

### ตัวอย่างใน template

| DB | Library | Protocol |
|---|---|---|
| PostgreSQL | `psycopg2` | libpq (PG wire protocol) |
| MSSQL | `pymssql` | TDS (Tabular Data Stream) |
| Impala/Kudu | `impyla` | HiveServer2 Thrift |

### Install

```bash
# Native — pip/uv install เท่านั้น
uv add psycopg2-binary pymssql impyla
```

### Usage

```python
import psycopg2

conn = psycopg2.connect(
    host="pg-server", port=5432,
    dbname="mydb", user="etl", password="secret"
)
cur = conn.cursor()
cur.execute("SELECT * FROM users LIMIT 10")
rows = cur.fetchall()
```

### ข้อดี
- **Setup ง่ายสุด** — `pip install` จบ
- **เร็วสุด** — communicate ตรงกับ DB, ไม่มี middleware layer
- **Docker image เล็ก** — ไม่ต้อง system packages (ยกเว้น psycopg2 ต้อง libpq)
- **Debug ง่าย** — error message ชัดเจน, stack trace ตรงไปตรงมา

### ข้อเสีย
- **DB-specific** — library คนละตัวต่อ DB (psycopg2 ≠ pymssql ≠ impyla)
- **API ต่างกัน** — parameter style, cursor behavior ต่างกัน
- **ไม่มี standard** — แต่ละ library มี feature/quirk ต่างกัน

---

## ODBC (Open Database Connectivity)

Standard API กลาง — เขียน code ครั้งเดียว connect ได้หลาย DB (ผ่าน driver)

### Architecture

```
Python (pyodbc)
    ↓
ODBC Driver Manager (unixODBC)
    ↓
ODBC Driver (vendor-specific)
    ├── ODBC Driver 18 for SQL Server
    ├── PostgreSQL ODBC (psqlODBC)
    ├── Cloudera ODBC Driver for Impala
    └── ...
    ↓
Database
```

### Install

```bash
# 1. System packages (ODBC Driver Manager + Driver)
# Ubuntu/Debian
apt-get install unixodbc unixodbc-dev

# MSSQL ODBC Driver
curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
apt-get install msodbcsql18

# PostgreSQL ODBC
apt-get install odbc-postgresql

# 2. Python library
uv add pyodbc
```

### Config — odbc.ini / odbcinst.ini

```ini
# /etc/odbcinst.ini — driver registration
[ODBC Driver 18 for SQL Server]
Description=Microsoft ODBC Driver 18 for SQL Server
Driver=/opt/microsoft/msodbcsql18/lib64/libmsodbcsql-18.so

[PostgreSQL]
Description=PostgreSQL ODBC driver
Driver=/usr/lib/x86_64-linux-gnu/odbc/psqlodbcw.so

# /etc/odbc.ini — DSN (Data Source Name)
[MyMSSQL]
Driver=ODBC Driver 18 for SQL Server
Server=mssql-server,1433
Database=mydb

[MyPostgres]
Driver=PostgreSQL
Servername=pg-server
Port=5432
Database=mydb
```

### Usage

```python
import pyodbc

# Connection string (DSN-less)
conn = pyodbc.connect(
    "DRIVER={ODBC Driver 18 for SQL Server};"
    "SERVER=mssql-server,1433;"
    "DATABASE=mydb;"
    "UID=etl;PWD=secret;"
    "TrustServerCertificate=yes;"
)

# หรือใช้ DSN
conn = pyodbc.connect("DSN=MyMSSQL;UID=etl;PWD=secret")

cur = conn.cursor()
cur.execute("SELECT * FROM users")
rows = cur.fetchall()
```

### ข้อดี
- **Standard API** — code เหมือนกันทุก DB (เปลี่ยนแค่ connection string)
- **Vendor support** — driver จาก vendor มี feature ครบ (bulk copy, encryption)
- **fast_executemany** — MSSQL bulk insert เร็วกว่า pymssql
- **Enterprise features** — Kerberos, TLS, Azure AD มักรองรับดีกว่า native

### ข้อเสีย
- **System dependency** — ต้อง install driver ที่ OS level
- **Config ยุ่งยาก** — odbc.ini, odbcinst.ini, driver path
- **Docker image ใหญ่** — ต้อง install unixodbc + driver binary
- **Platform-specific** — driver binary ต่าง OS (linux/mac/windows)
- **Debug ยาก** — error message มาจาก driver manager layer

### เมื่อไหร่ใช้ ODBC แทน Native

| Scenario | ใช้ ODBC |
|---|---|
| ต้อง `fast_executemany` (MSSQL bulk) | ✅ |
| ต้อง Kerberos auth กับ MSSQL | ✅ |
| ต้อง Azure AD / OAuth auth | ✅ |
| ต้อง TLS/SSL cert pinning | ✅ |
| Corporate policy บังคับ ODBC | ✅ |
| Vendor ให้แค่ ODBC driver (Cloudera Impala) | ✅ |

---

## JDBC (Java Database Connectivity)

Standard API ของ Java — ต้อง JVM ถึงจะใช้ได้

### Architecture

```
Python (JayDeBeApi / jpype)
    ↓
JPype (Python → JVM bridge)
    ↓
JVM (Java Virtual Machine)
    ↓
JDBC Driver (JAR file)
    ├── postgresql-42.7.jar
    ├── mssql-jdbc-12.6.jar
    ├── ImpalaJDBC42.jar
    └── ...
    ↓
Database
```

### Install

```bash
# 1. Java Runtime
apt-get install default-jre  # ~200 MB

# 2. JDBC Driver (JAR files)
wget https://jdbc.postgresql.org/download/postgresql-42.7.4.jar -P /opt/jdbc/
wget https://cloudera.com/downloads/ImpalaJDBC42.jar -P /opt/jdbc/

# 3. Python libraries
uv add JayDeBeApi jpype1
```

### Usage

```python
import jaydebeapi

# PostgreSQL via JDBC
conn = jaydebeapi.connect(
    "org.postgresql.Driver",
    "jdbc:postgresql://pg-server:5432/mydb",
    ["etl", "secret"],
    "/opt/jdbc/postgresql-42.7.4.jar",
)

cur = conn.cursor()
cur.execute("SELECT * FROM users")
rows = cur.fetchall()
```

```python
# Impala via JDBC (Cloudera)
conn = jaydebeapi.connect(
    "com.cloudera.impala.jdbc.Driver",
    "jdbc:impala://impala-server:21050/mydb;"
    "AuthMech=1;KrbHostFQDN=impala.realm.com;KrbServiceName=impala",
    None,
    "/opt/jdbc/ImpalaJDBC42.jar",
)
```

### ข้อดี
- **Vendor support สูงสุด** — ทุก DB มี JDBC driver อย่างเป็นทางการ
- **Feature ครบ** — encryption, Kerberos, connection pooling ใน driver
- **Spark/Hadoop ecosystem** — ใช้ JDBC driver เดียวกับ Spark
- **Cross-platform** — JAR file ใช้ได้ทุก OS ที่มี JVM

### ข้อเสีย
- **JVM overhead** — start JVM ~2s, RAM +200-500 MB
- **ช้าที่สุด** — Python → JPype → JVM → JDBC Driver → DB
- **Docker image ใหญ่มาก** — JRE ~200 MB + JAR files
- **Debug ยากมาก** — Java exception → Python wrapper (stack trace ยาว)
- **Type conversion** — Java types ↔ Python types ไม่ seamless
- **GIL issue** — JPype + GIL = ไม่ thread-safe บาง scenario

### เมื่อไหร่ใช้ JDBC

| Scenario | ใช้ JDBC |
|---|---|
| DB มีแค่ JDBC driver (legacy system) | ✅ |
| ต้องการ driver เดียวกับ Spark job | ✅ |
| Cloudera/Hortonworks environment (vendor bundle) | ✅ |
| ต้อง Kerberos + ไม่มี native/ODBC option | ✅ |

---

## Performance Benchmark

### Connect + Query 10K rows

| Method | Connect Time | Query Time | Total | RAM Overhead |
|---|---|---|---|---|
| **psycopg2** (native) | 50ms | 200ms | 250ms | ~10 MB |
| **pyodbc** (ODBC) | 80ms | 220ms | 300ms | ~15 MB |
| **JayDeBeApi** (JDBC) | 2000ms (JVM start) | 350ms | 2350ms | ~300 MB |

### Bulk Insert 100K rows

| Method | Time | Throughput |
|---|---|---|
| psycopg2 `execute_values` | 3s | 33K rows/s |
| psycopg2 `COPY FROM` | 1s | 100K rows/s |
| pyodbc `fast_executemany` | 4s | 25K rows/s |
| pyodbc normal | 60s | 1.6K rows/s |
| JayDeBeApi batch | 15s | 6.6K rows/s |

---

## Decision Matrix

```
เลือก Connector?
│
├─ Python project (ETL template)
│   ├─ PostgreSQL ──→ psycopg2 (native) ✅
│   ├─ MSSQL
│   │   ├─ Simple ETL ──→ pymssql (native) ✅
│   │   ├─ ต้อง bulk insert ──→ pyodbc + fast_executemany
│   │   └─ ต้อง Kerberos/Azure AD ──→ pyodbc (ODBC)
│   ├─ Impala/Hive
│   │   ├─ Thrift available ──→ impyla (native) ✅
│   │   └─ ต้อง Cloudera JDBC only ──→ JayDeBeApi (JDBC)
│   └─ Oracle/DB2/legacy
│       ├─ มี native lib (cx_Oracle) ──→ native
│       └─ มีแค่ JDBC ──→ JayDeBeApi
│
├─ Spark/Java project ──→ JDBC (standard)
│
└─ .NET/C++ project ──→ ODBC (standard)
```

---

## Template Recommendation

| DB | Primary (ใช้ใน template) | Alternative (เมื่อต้องการ) |
|---|---|---|
| **PostgreSQL** | `psycopg2` (native) | `asyncpg` (async), `pyodbc` (ODBC) |
| **MSSQL** | `pymssql` (native) | `pyodbc` + fast_executemany (bulk) |
| **Impala/Kudu** | `impyla` (native Thrift) | Cloudera JDBC (corporate env) |
| **Iceberg** | `impyla` (via Impala SQL) | Spark JDBC (heavy workload) |

### ทำไม template ใช้ Native

1. **Zero system dependency** — `uv add psycopg2-binary pymssql impyla` จบ
2. **Docker image เล็ก** — ไม่ต้อง install ODBC driver / JRE
3. **เร็วที่สุด** — no middleware overhead
4. **Debug ง่าย** — Python traceback ตรงไปตรงมา
5. **CI/CD simple** — ไม่ต้อง provision system packages

### เมื่อไหร่เปลี่ยนจาก Native → ODBC

```python
# ถ้าต้อง fast_executemany (MSSQL bulk insert > 50K rows)
import pyodbc

conn = pyodbc.connect(
    "DRIVER={ODBC Driver 18 for SQL Server};"
    f"SERVER={host},{port};DATABASE={db};UID={user};PWD={pwd}"
)
cur = conn.cursor()
cur.fast_executemany = True
cur.executemany("INSERT INTO t (a,b,c) VALUES (?,?,?)", rows)
```

```dockerfile
# Dockerfile — ต้องเพิ่ม ODBC driver
FROM python:3.11-slim

# Install MSSQL ODBC Driver
RUN apt-get update && apt-get install -y \
    unixodbc unixodbc-dev gnupg2 curl \
    && curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add - \
    && curl https://packages.microsoft.com/config/debian/12/prod.list \
       > /etc/apt/sources.list.d/mssql-release.list \
    && apt-get update \
    && ACCEPT_EULA=Y apt-get install -y msodbcsql18 \
    && rm -rf /var/lib/apt/lists/*
```

---

## Hybrid — ใช้ทั้ง Native + ODBC

```python
# connector/database/mssql.py — support ทั้ง 2 mode
class MSSQLConnector:
    def __init__(self, host, port, db, user, pwd, use_odbc=False):
        if use_odbc:
            import pyodbc
            self.conn = pyodbc.connect(
                f"DRIVER={{ODBC Driver 18 for SQL Server}};"
                f"SERVER={host},{port};DATABASE={db};UID={user};PWD={pwd}"
            )
        else:
            import pymssql
            self.conn = pymssql.connect(host, user, pwd, db, port=port)
```

---

## Summary

| | Native | ODBC | JDBC |
|---|---|---|---|
| **Speed** | ⭐⭐⭐ | ⭐⭐ | ⭐ |
| **Setup** | ⭐⭐⭐ | ⭐⭐ | ⭐ |
| **Docker size** | ⭐⭐⭐ | ⭐⭐ | ⭐ |
| **Feature completeness** | ⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐ |
| **Cross-DB portability** | ⭐ | ⭐⭐⭐ | ⭐⭐⭐ |
| **Enterprise auth** | ⭐⭐ | ⭐⭐⭐ | ⭐⭐⭐ |
| **Recommended for** | Python ETL ✅ | Bulk/Enterprise | Legacy/Spark |
