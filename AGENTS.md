# AGENTS.md

> [!NOTE]
> This file is a machine-readable specification designed for AI coding agents. It serves as a dedicated instruction guide to ensure consistency, safety, and efficiency during development.

---

## 1. Project Overview & Context

This repository contains the implementation, infrastructure configs, benchmark scripts, and experimental results for building and evaluating a **Data Lakehouse system based on Lambda Architecture**.

*   **Primary Goal:** Design, deploy, and benchmark a Lambda Architecture (Batch Layer + Speed Layer + Serving Layer) using open-source technologies, all containerized via Docker Compose on local hardware.
*   **Core Research Questions:**
    1.  Does Lambda Architecture reduce query latency compared to Batch-only approaches?
    2.  How accurate are Speed Views (approximate) vs Batch Views (exact) after reprocessing?
    3.  How much does Iceberg Compaction improve read performance after streaming ingestion?
*   **Key Technologies:** Apache Kafka, Apache Spark (Batch & Streaming), Apache Iceberg, MinIO, ClickHouse, Redis, FastAPI, dbt, Dagster, Docker Compose.

---

## 2. Tech Stack & Environment

*   **Language & Version:** Python 3.10+
*   **Core Libraries:** PySpark, kafka-python, redis-py, clickhouse-connect, FastAPI, dbt-spark, dagster, pyiceberg.
*   **Infrastructure:** All services run locally via Docker Compose (Kafka, Spark, MinIO, ClickHouse, Redis, Dagster).
*   **Local Setup:**
    ```bash
    # Clone repository
    git clone <repo-url>
    cd lakehouse-lambda-benchmark

    # Copy environment variables
    cp .env.example .env

    # Start all infrastructure
    docker compose up -d

    # Install Python dependencies (for scripts & benchmarks)
    pip install -r requirements.txt
    ```

---

## 3. Project Directory Structure

*   [configs/](configs) - Service-specific configuration files.
    *   [kafka/](configs/kafka) - Kafka broker & topic configurations.
    *   [spark/](configs/spark) - Spark session & cluster configs.
    *   [iceberg/](configs/iceberg) - Iceberg catalog & table configs.
    *   [clickhouse/](configs/clickhouse) - ClickHouse server & schema configs.
    *   [redis/](configs/redis) - Redis configuration.
    *   [dagster/](configs/dagster) - Dagster workspace configs.
*   [src/](src) - Core source code, organized by Lambda Architecture layer.
    *   [ingestion/](src/ingestion) - Kafka producers & batch data loaders.
    *   [batch_layer/](src/batch_layer) - Spark Batch jobs, Iceberg utils, ClickHouse sync.
    *   [speed_layer/](src/speed_layer) - Spark Structured Streaming → Redis.
    *   [serving_layer/](src/serving_layer) - FastAPI Query Merger (Batch + Speed).
    *   [data_quality/](src/data_quality) - DQ checks, quarantine logic, DQ metrics.
    *   [utils/](src/utils) - Shared utilities (config loader, logging).
*   [dbt_project/](dbt_project) - dbt transformation models (Bronze → Silver → Gold).
*   [dagster_project/](dagster_project) - Dagster asset definitions & schedules.
*   [scripts/](scripts) - Helper scripts & benchmark runners.
    *   [benchmarks/](scripts/benchmarks) - Benchmark execution scripts for 3 scenarios.
*   [results/](results) - Benchmark results (CSV logs, plots).
    *   [plots/](results/plots) - Generated charts and visualizations.
    *   [logs/](results/logs) - Raw benchmark log files.
*   [datasets/](datasets) - Dataset configs, schema definitions, sample data.
*   [docs/](docs) - Project documentation, proposals, architecture docs.
    *   [plans/](docs/plans) - Sprint plans & task tracking.
    *   [process/](docs/process) - Process documentation (tiếng Việt).
*   [tests/](tests) - Unit & integration tests.
*   [dashboard/](dashboard) - Streamlit dashboard app.
*   [references/](references) - Academic papers & BibTeX.

---

## 4. Setup & Execution Commands

### Infrastructure Management
*   **Start all services:**
    ```bash
    docker compose up -d
    ```
*   **Stop all services:**
    ```bash
    docker compose down
    ```
*   **View service logs:**
    ```bash
    docker compose logs -f <service-name>
    ```
*   **Reset all data (destructive):**
    ```bash
    docker compose down -v
    docker compose up -d
    ```

### Data Pipeline Execution
*   **Seed sample data:**
    ```bash
    python scripts/seed_data.py --sample
    ```
*   **Run full Batch Pipeline (Bronze → Silver → Gold → ClickHouse):**
    ```bash
    python src/batch_layer/spark_batch_jobs.py --mode full
    ```
*   **Start Speed Layer (Kafka → Spark Streaming → Redis):**
    ```bash
    python src/speed_layer/spark_streaming.py
    ```
*   **Start Serving API:**
    ```bash
    uvicorn src.serving_layer.api_routes:app --host 0.0.0.0 --port 8000
    ```

### Benchmark Execution
*   **Run all 3 benchmarks:**
    ```bash
    python scripts/benchmarks/run_all_benchmarks.py
    ```
*   **Run individual benchmarks:**
    ```bash
    python scripts/benchmarks/bench_latency.py      # Benchmark 1: Query Latency
    python scripts/benchmarks/bench_reprocess.py     # Benchmark 2: Reprocessing Correctness
    python scripts/benchmarks/bench_compaction.py    # Benchmark 3: Compaction Efficiency
    ```

---

## 5. Coding Standards & Conventions

### Python Style Rules (PEP 8)
*   **Naming Conventions:**
    *   Variables, attributes, and function names: `snake_case` (e.g., `batch_cutoff`, `get_speed_view`).
    *   Class names: `PascalCase` (e.g., `QueryMerger`, `DataQualityChecker`).
    *   Constants: `UPPER_CASE` (e.g., `KAFKA_BOOTSTRAP_SERVERS`, `REDIS_TTL_SECONDS`).
*   **Formatting:** Use exactly **4 spaces** for indentation. Never use tabs.
*   **Documentation:** All main classes and functions must include clear Docstrings. Outline parameters (`Args`) and return values (`Returns`).
*   **KISS & DRY:** Keep implementation simple. Abstract reusable logic (e.g., ClickHouse queries, Redis read/write, Iceberg operations) into unified helper modules in [src/utils/](src/utils).

### SQL & dbt Style Rules
*   Use lowercase for SQL keywords (`select`, `from`, `where`).
*   dbt model names: `snake_case`, prefixed by layer (`bronze_`, `silver_`, `gold_`).
*   Always include a `{{ config(...) }}` block at the top of dbt models.

---

## 6. Git Workflow & Conventional Commits

*   **Branching Strategy:** Create new features branching off `develop` and prefix with `feature/` (e.g., `feature/batch-pipeline-setup`).
*   **Commit Messages (Conventional Commits style):**
    *   `feat: <description>` for new features.
    *   `fix: <description>` for bug fixes.
    *   `docs: <description>` for documentation changes.
    *   `refactor: <description>` for structure edits without behavioral changes.
    *   `infra: <description>` for Docker/infrastructure changes.
    *   `bench: <description>` for benchmark-related changes.
*   **Dependency Management:** Add packages to [requirements.txt](requirements.txt) with precise versioning.

---

## 7. Boundaries & Constraints

### ALWAYS
*   **Preserve all comments and docstrings** that are unrelated to your code changes.
*   **Handle connection errors gracefully:** Include `try...except` blocks around Kafka/ClickHouse/Redis/MinIO connections. Log errors and continue pipeline execution where possible.
*   **Verify Docker services are running** before executing any pipeline scripts.
*   **Use `.env` for secrets:** Never hardcode credentials (MinIO access keys, Redis passwords) in source code.
*   **Log benchmark results to CSV:** All benchmark scripts must output structured CSV logs to [results/logs/](results/logs).

### NEVER
*   **Do NOT commit large data files:** Never commit raw datasets (`.parquet`, `.csv` > 1MB), MinIO data, or Docker volumes to Git. Keep them in `.gitignore`.
*   **Do NOT modify Docker Compose services** without updating the corresponding config in [configs/](configs).
*   **Do NOT use tabs:** Ensure the editor is configured to convert tabs to 4 spaces.

### ASK FIRST
*   *Ask before introducing new Docker services* to `docker-compose.yml`.
*   *Ask before altering the Iceberg table schema* or ClickHouse table DDL.
*   *Ask before changing the benchmark CSV output format* in [results/](results).
