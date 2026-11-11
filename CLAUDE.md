# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

**Read [AGENTS.md](AGENTS.md) first.** It holds the project's coding standards, commit conventions, and the ALWAYS / NEVER / ASK FIRST rules. In short, ask before you add Docker services, change Iceberg or ClickHouse DDL, or change the benchmark CSV format. This file adds only what AGENTS.md leaves out.

This is a university thesis project (TLCN, HCMUTE). Most docs, comments, and log messages are in Vietnamese, often without diacritics. Keep that style when you edit nearby code. Files under `docs/process/` are written in Vietnamese.

## Commands

```bash
# Tests (pytest.ini: testpaths=tests; conftest.py adds the repo root to sys.path so `src.*` imports work)
pytest                                   # all tests
pytest tests/test_query_merger.py -v     # one file
pytest tests/test_query_merger.py::TestClassName::test_name   # one test
pytest -m "not integration"              # skip tests needing live Docker services
python scripts/run_tests.py              # unittest-based runner over the 6 unit suites (no Docker needed)

# One-command end-to-end local run (starts needed containers, fetches Binance REST, generates mock data,
# replays to Kafka, runs batch, lets speed layer stream, smoke-checks FastAPI)
python scripts/run_lambda_pipeline.py --top-n 5 --mock-days 1 --replay-limit 5000
python scripts/e2e_full_pipeline.py --top-n 2 --mock-days 1 --replay-limit 1000   # smoke variant

# Batch layer
docker compose run --rm batch-pipeline                       # real Iceberg write (--write-iceberg --iceberg-strict)
python src/batch_layer/spark_batch_jobs.py                   # host run: ClickHouse sync only, no Iceberg
python src/batch_layer/spark_batch_jobs.py --from-kafka --daemon --interval 60
python src/batch_layer/spark_batch_jobs.py --input datasets/sample --no-clickhouse

# Speed layer (run in Docker; on Windows hosts Spark needs hadoop/bin/winutils.exe)
docker compose up -d speed-layer && docker compose logs -f speed-layer

# Serving API / dashboard
uvicorn src.serving_layer.api_routes:app --host 0.0.0.0 --port 8000
python scripts/check_system.py           # service health check

# Benchmarks -> results/logs/*.csv, plots via scripts/benchmarks/plot_benchmarks.py
python scripts/benchmarks/run_all_benchmarks.py
```

There is no linter or formatter configured.

Service UIs: FastAPI `:8000/docs`, Streamlit `:8501`, Dagster `:3000`, MinIO `:9001`, ClickHouse HTTP `:8123`, Spark `:8080`. From the host, Kafka is `localhost:9094`. Inside containers it is `kafka:9092`.

## Architecture

The data is crypto trade events (Binance aggTrades, mostly BTCUSDT plus other top-N symbols) aggregated into 1-minute OHLCV/VWAP candles.

**Data flow**
- Ingestion (`src/ingestion/`, `scripts/stream_*.py`, `scripts/generate_mock_data.py`) produces `TradeEvent` JSON to the Kafka topic `crypto_trades_raw`. `src/ingestion/models.py` and `datasets/schemas/trade_event_schema.json` define the data contract. `fault_injector.py` deliberately injects duplicates and bad records (`is_injected`, `fault_type`) so the DQ layer and the reconciliation benchmarks have something to catch.
- **Speed layer** (`src/speed_layer/spark_streaming.py`): Spark Structured Streaming reads Kafka and computes 1-minute tumbling windows with a 1-minute watermark in `window_aggregator.py`. Each micro-batch, `foreachBatch` writes to ClickHouse `lakehouse.speed_agg` (provisional) and to Redis (hot cache for the latest candle only). `KAFKA_FAIL_ON_DATA_LOSS=true` is intentional (fail closed). An existing checkpoint always overrides `startingOffsets`.
- **Batch layer** (`src/batch_layer/spark_batch_jobs.py`): despite the name, the core Bronze→Silver→Gold path is **plain Python**. It loads JSON/CSV (by default the first non-empty dir among `datasets/mock/aggtrades`, `datasets/raw/aggtrades`, `datasets/sample`) or reads Kafka with a file offset checkpoint at `results/logs/batch_kafka_offsets.json`. It then runs `clean_and_deduplicate` from `src/data_quality/`, aggregates candles, and writes to ClickHouse `batch_agg` (reconciled), `dq_quarantine`, and `reconciliation_audit_metrics`. Finally it advances `system_watermark` to the max `window_end`. Spark and Iceberg are used only for the optional Bronze write (`--write-iceberg`, via `iceberg_utils.py` with the Iceberg REST catalog on MinIO) and for compaction (`compaction.py`, `maintenance.py`).
- **Serving layer** (`src/serving_layer/query_merger.py`, the core of the project): `AutoCorrectingQueryMerger` reads the batch watermark W and routes each `/api/market` query to one of three cases:
  1. `end <= W`: `batch_agg` only (RECONCILED)
  2. `start >= W`: `speed_agg` only (PROVISIONAL)
  3. Hybrid: `[start, W)` comes from batch and `[W, end)` from speed. Results are deduplicated by `(symbol, window_start)` with batch preferred, and the VWAP delta at the boundary is reported.

  `/api/reconciliation` compares batch and speed per window (statuses `MATCHED`, `RECONCILED_AND_HEALED`, `BATCH_ONLY`, `PROVISIONAL_ONLY`).
- **Orchestration**: `dagster_project/repository.py` re-expresses the batch path as assets (bronze → silver → gold → ClickHouse sync → watermark, plus Iceberg compaction). It runs batch every 15 minutes and compaction every 2 hours.
- **Dashboard**: `dashboard/app.py` (Streamlit) reads from FastAPI. The components live in `dashboard/components/`.
- **dbt** (`dbt_project/`) is a reference artefact only and is **not** on the runtime path.

**ClickHouse tables** use `ReplacingMergeTree(created_at)` ordered by `(symbol, window_start)`. Re-running batch inserts newer revisions instead of updating rows. Readers must take the newest revision (`FINAL` in `clickhouse_client.py`, `ORDER BY updated_at DESC` for the watermark). Base DDL lives in `configs/clickhouse/init.sql`, but `ClickHouseBatchSync.initialize_tables()` and `ClickHouseSpeedWriter.initialize_tables()` also create tables. Keep all three in sync.

**Host vs container config** (`src/utils/config.py`): `.env` uses container hostnames (`clickhouse`, `kafka:9092`, ...). On Windows (`os.name == "nt"`) the config rewrites these to `localhost` and `localhost:9094`, so the same `.env` works from both places. Always get connection settings through the `get_*_config()` helpers.

**Benchmarks** (`scripts/benchmarks/`): latency (RQ1), reprocess correctness (RQ2), compaction (RQ3), and fault/chaos tests. Only rows with `mode=real` in the compaction results count toward conclusions.

## Gotchas

- Parts of README.md and development.md are stale. They mention files that don't exist, such as `batch_loader.py`, `config_loader.py`, `tests/test_batch_layer.py`, `tests/test_serving_layer.py`, and `--mode sample` smoke flows. Trust the code over those docs.
- `report_materials/` holds copies of docs collected by `scripts/aggregate_report_materials.py`. Edit the originals, not the copies.
- `thesis_workspace/` contains the thesis writing (outline, specs, drafts), not code.
- `datasets/mock/` and `results/logs/*.csv` are gitignored generated output.
