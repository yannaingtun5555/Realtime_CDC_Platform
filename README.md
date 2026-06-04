# Realtime CDC Platform (Work in Progress 🚧)

## 📌 Overview

This project is a **Realtime Change Data Capture (CDC) Platform** designed to capture, process, and stream database changes into downstream systems for analytics, monitoring, and data-driven applications.

The system is currently under active development and aims to provide a scalable, production-ready data pipeline using modern data engineering tools.

---

## 🎯 Goals

* Capture real-time changes from source databases
* Stream data reliably through a distributed messaging system
* Process and enrich data using stream processing
* Store processed data into analytical storage systems
* Provide a foundation for real-time dashboards and insights

---

## 🏗️ Architecture (Current)

```
Source DB → CDC Connector → Kafka → Stream Processing (Flink) → Sink (Iceberg / DB)
```

### Components:

* **CDC Layer**

  * Captures changes (INSERT, UPDATE, DELETE) from source database
  * Planned: Debezium-based connectors

* **Messaging Layer**

  * Apache Kafka used as the event streaming backbone
  * Topics:

    * `cdc.inventory.customers`
    * `cdc.inventory.orders`
    * `internal.capture`
    * `internal.enriched`

* **Processing Layer**

  * Apache Flink for real-time stream processing
  * Jobs:

    * CDC Capture Job
    * CDC Enrich Job
    * CDC Sink Job

* **Storage Layer (WIP)**

  * Iceberg (planned / partial)
  * External destination DB (configurable)

* **Object Storage**

  * MinIO (S3-compatible storage)

---

## ⚙️ Tech Stack

* **Backend / Processing**

  * Java (Flink jobs in progress)
  * PyFlink (being phased out)

* **Streaming**

  * Apache Kafka

* **Storage**

  * Apache Iceberg (in progress)
  * MinIO (S3)

* **Orchestration**

  * Docker + Docker Compose

---

## 🚀 Current Status

✔ Kafka topics auto-created
✔ Flink jobs submission automated
✔ CDC pipeline structure defined
✔ MinIO integration added
⚠️ Iceberg integration incomplete
⚠️ Some jobs still transitioning from PyFlink → Java
⚠️ No full production-ready sink yet

---

## ❗ Known Limitations

* Sink layer not finalized (Iceberg / DB decision ongoing)
* Schema evolution handling not fully implemented
* Error handling and retry logic still basic
* Monitoring and alerting not implemented yet
* Manual configuration required for some components

---

## 🔄 Work in Progress

* Migrating all Flink jobs to Java
* Improving job orchestration & automation
* Adding schema registry support
* Enhancing fault tolerance
* Designing flexible sink connectors
* Optimizing Docker build performance

---

## 🧪 How to Run (Dev Setup)

```bash
# Start services
docker-compose up -d

# Submit Flink jobs
./submit-jobs.sh
```

Check Flink UI:

```
http://localhost:8081
```

MinIO Console:

```
http://localhost:9001
```

---

## 📂 Project Structure (Simplified)

```
.
├── flink/                 # Flink jobs
├── docker-compose.yml    # Infrastructure setup
├── scripts/              # Job submission scripts
├── connectors/           # CDC connectors
└── config/               # Config files
```

---

## 🧠 Future Vision

This project aims to evolve into a **plug-and-play CDC platform** that can:

* Support multiple databases
* Provide real-time analytics pipelines
* Enable scalable event-driven architectures
* Be deployed in production environments easily

---

## 👨‍💻 Author

Yan Naing Htun

---

## 📌 Notes

This project is actively evolving. Expect breaking changes, incomplete features, and ongoing refactoring.

Contributions, ideas, and improvements are welcome.
