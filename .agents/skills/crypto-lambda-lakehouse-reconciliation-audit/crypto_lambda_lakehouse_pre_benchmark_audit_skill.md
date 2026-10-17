---
name: crypto-lambda-lakehouse-reconciliation-audit
description: Use this skill when auditing, validating, fault-testing, and benchmarking a Lambda Architecture Data Lakehouse pipeline for real-time cryptocurrency market data reconciliation before starting a 24/7 continuous streaming run.
version: 1.0.0
tags:
  - data-lakehouse
  - lambda-architecture
  - crypto-market-data
  - real-time-reconciliation
  - chaos-testing
  - benchmark
---

# Crypto Lambda Lakehouse: Pre-Benchmark Audit & Reconciliation Verification Skill

## 1. Context & Agent Mission

The core skeleton of the **Lambda Architecture Data Lakehouse** for cryptocurrency real-time data reconciliation has been implemented. Because cryptocurrency markets operate $24/7/365$ without a market close for static end-of-day batch processing, the system must continuously balance **low latency (Speed Layer)** and **eventual completeness/accuracy (Batch Layer + Reconciliation Engine)**.

**Your Mission as the Engineering Agent:**
Before initiating the $24/7$ continuous streaming burn-in and collecting thesis benchmark metrics, you must **audit the existing codebase**, **verify the reconciliation logic against 5 classic cryptocurrency domain edge cases**, and **execute 4 automated fault-injection smoke tests**. Do not start the long-running $24/7$ stream until all Readiness Gates in Section 5 pass.

---

## 2. The 5 Domain Problems to Audit in the Codebase

Inspect the current project repository (Ingestion, Speed Layer, Batch Layer, Lakehouse Storage, and Serving/Reconciliation Layer) and verify how each of the following 5 real-world failure modes is handled:

### Problem 1: WebSocket Silent Drops & Sequence ID Gaps
* **Domain Reality:** CEX REST APIs enforce strict rate limits (e.g., Binance limits to $6,000$ request-weight/minute per IP; exceeding this triggers HTTP $429$ or IP ban HTTP $418$). Real-time pipelines rely on WebSocket push streams (`aggTrade`, `kline`, `depthUpdate`). However, WebSockets suffer from silent packet drops during network jitter or buffer overflows without raising connection errors.
* **Code Audit Checklist:**
  - [ ] Does the schema capture sequential identifiers (`agg_trade_id`, `first_update_id` $U$, `final_update_id` $u$)?
  - [ ] Does the Speed or Reconciliation layer detect sequence discontinuities where $U_{\text{current}} > u_{\text{previous}} + 1$ or $\text{trade\_id}_{i} - \text{trade\_id}_{i-1} > 1$?
  - [ ] Is there a Batch/REST backfill worker that fetches historical snapshots/trades for the missing range $[u_{\text{previous}} + 1, U_{\text{current}} - 1]$ while respecting rate-limit backoff?
* **Academic/Industry Reference for Thesis Logging:** *Binance Official Order Book Sync Spec*; *Axon Trade Engineering (2026), "FIX vs REST vs WebSocket for Crypto Trading"*; *NASSCOM Web 3.0 Community (2026)*.

### Problem 2: Out-of-Order Events, Late-Arriving Data & Clock Drift
* **Domain Reality:** High-frequency multi-pair streams exhibit a significant gap between Exchange Matching Engine time (`event_time`) and pipeline arrival time (`ingestion_time`). Empirical microstructure studies show median clock drift and transport skew between $16\text{ ms}$ and $347\text{ ms}$ (with jitter up to $\pm 99\text{ ms}$).
* **Code Audit Checklist:**
  - [ ] Are all window aggregations (e.g., $1\text{-min}$ / $5\text{-min}$ OHLCV candles, VWAP) computed strictly on `event_time` rather than `processing_time`?
  - [ ] Is an explicit Watermark configured in the Speed Layer (e.g., `.withWatermark("event_time", "10 seconds")`) so state store memory stays bounded?
  - [ ] Does the Batch Layer re-compute the exact same time windows over raw Bronze/Silver Lakehouse data *without* restrictive watermarks to rescue late-arriving records dropped by the Speed Layer?
* **Academic/Industry Reference for Thesis Logging:** *arXiv:2607.26245 (2026), "OpenMarket: A Synchronized Polymarket–Binance Dataset for High-Frequency Market Microstructure"*; *Akidau et al. (Google Dataflow Model, VLDB)*.

### Problem 3: Duplicate Payloads & Protocol Seam Race Conditions
* **Domain Reality:** At-least-once Kafka producers, collector retries, and the overlapping boundary ("protocol seam") when switching between REST snapshot recovery and live WebSocket streams inevitably produce duplicate trade records.
* **Code Audit Checklist:**
  - [ ] Does the Speed Layer apply bounded state deduplication (e.g., `dropDuplicatesWithinWatermark(["symbol", "trade_id"])`)?
  - [ ] Does the Batch/Lakehouse Layer enforce idempotent upserts (`MERGE INTO ... ON target.symbol = source.symbol AND target.trade_id = source.trade_id`) so re-running a batch job never inflates trading volume?
* **Academic/Industry Reference for Thesis Logging:** *Axon Trade Engineering (2026)*; *Coinbase Data Engineering System Design Guidelines (2026)*.

### Problem 4: Flash Crashes, Pump-and-Dump Traffic Spikes & Backpressure
* **Domain Reality:** Crypto markets lack traditional equity circuit breakers. Flash crashes or macro news events cause $10\times$ to $50\times$ message volume spikes within seconds, inducing streaming backpressure or OOM crashes.
* **Code Audit Checklist:**
  - [ ] Are rate-limiting configurations enabled on the streaming reader (e.g., Kafka `maxOffsetsPerTrigger` or Flink backpressure limits)?
  - [ ] Are checkpoint directories and RocksDB/state-store memory limits properly configured for sustained $24/7$ operation without state bloat?
  - [ ] Does the Reconciliation Layer compare aggregated tick volume against official exchange Kline summary volume to detect dropped ticks during extreme spikes?
* **Academic/Industry Reference for Thesis Logging:** *arXiv:2609.01176 (2026), "Investigating Cryptocurrency Pump-and-Dump Schemes"*; *Makarov & Schoar (2020), Journal of Financial Economics*.

### Problem 5: State Rollbacks / Reorganizations & Serving Layer Truth Override
* **Domain Reality:** When discrepancies occur (either from on-chain block reorganizations where `parent_hash` diverges, or CEX WebSocket state corruption), the Serving Layer must seamlessly replace provisional real-time estimates with verified Batch truth.
* **Code Audit Checklist:**
  - [ ] Does the Serving View/Table implement a deterministic priority rule where reconciled Batch records (`is_reconciled = true`) override provisional Speed Layer records for the same `(symbol, window_start)` key?
* **Academic/Industry Reference for Thesis Logging:** *QuickNode Engineering (2026), "Blockchain Reorg Handling in Data Streams"*; *Jia et al., arXiv:2604.20421 (2026)*.

---

## 3. Mathematical Reconciliation Metrics & Verification Queries

Ensure the reconciliation module computes and logs the following metrics into a dedicated Lakehouse audit table (`reconciliation_audit_metrics`) for thesis evaluation:

### 3.1 Core Mathematical Definitions

1. **Sequence Gap Rate ($\text{R}_{\text{gap}}$):**
   $$\text{R}_{\text{gap}} = \frac{\sum_{i=1}^{N} \max\left(0, \text{ID}_{i} - \text{ID}_{i-1} - 1\right)}{\text{ID}_{\max} - \text{ID}_{\min} + 1} \times 100\%$$

2. **Relative Volume Drift ($\Delta V_{\%}$):**
   $$\Delta V_{\%} = \frac{\left| \sum V_{\text{Speed}} - \sum V_{\text{Batch}} \right|}{\sum V_{\text{Batch}}} \times 100\%$$

3. **VWAP Reconciliation Error ($\varepsilon_{\text{VWAP}}$):**
   $$\text{VWAP} = \frac{\sum_{j} \left(P_j \times Q_j\right)}{\sum_{j} Q_j}, \quad \varepsilon_{\text{VWAP}} = \left| \text{VWAP}_{\text{Speed}} - \text{VWAP}_{\text{Batch}} \right|$$

4. **Duplication Ratio ($\text{R}_{\text{dup}}$):**
   $$\text{R}_{\text{dup}} = \frac{\text{COUNT}_{\text{raw}}(*) - \text{COUNT}(\text{DISTINCT trade\_id})}{\text{COUNT}_{\text{raw}}(*)} \times 100\%$$

5. **Reconciliation Convergence Latency ($T_{\text{conv}}$):**
   $$T_{\text{conv}} = t_{\text{Batch\_Merged}} - t_{\text{Window\_Close}}$$

### 3.2 Reference SQL Verification Queries (Delta Lake / Iceberg / Spark SQL)

Verify that the pipeline can execute equivalent logic to the following two queries:

```sql
-- Query A: Detect Sequence Gaps & Duplicate Rate per Symbol and 1-Minute Window
WITH ordered_trades AS (
    SELECT
        symbol,
        DATE_TRUNC('minute', event_time) AS window_start,
        trade_id,
        LAG(trade_id) OVER (PARTITION BY symbol ORDER BY trade_id) AS prev_trade_id
    FROM lakehouse.silver_crypto_trades
)
SELECT
    symbol,
    window_start,
    COUNT(*) AS total_rows,
    COUNT(DISTINCT trade_id) AS unique_trades,
    (COUNT(*) - COUNT(DISTINCT trade_id)) AS duplicate_count,
    SUM(CASE WHEN trade_id - prev_trade_id > 1 THEN (trade_id - prev_trade_id - 1) ELSE 0 END) AS missing_sequence_count
FROM ordered_trades
GROUP BY symbol, window_start
HAVING duplicate_count > 0 OR missing_sequence_count > 0;
```

```sql
-- Query B: Cross-Layer Reconciliation (Speed Layer vs Batch Layer OHLCV & VWAP)
SELECT
    b.symbol,
    b.window_start,
    s.trade_count AS speed_count,
    b.trade_count AS batch_count,
    s.volume AS speed_volume,
    b.volume AS batch_volume,
    ABS(s.volume - b.volume) / NULLIF(b.volume, 0) * 100.0 AS volume_drift_pct,
    ABS(s.close_price - b.close_price) AS close_price_error,
    ABS(s.vwap - b.vwap) AS vwap_error,
    CASE
        WHEN ABS(s.volume - b.volume) < 1e-8 AND s.close_price = b.close_price THEN 'MATCHED'
        ELSE 'RECONCILED_AND_HEALED'
    END AS reconciliation_status
FROM lakehouse.gold_ohlcv_batch b
LEFT JOIN lakehouse.gold_ohlcv_speed s
    ON b.symbol = s.symbol AND b.window_start = s.window_start;
```

---

## 4. Pre-24/7 Fault Injection & Smoke Test Protocol

Before approving the pipeline for $24/7$ operation, execute the following **4 automated chaos tests** in a staging/test run and record the results:

### Test Case 1: WebSocket Disconnect & Sequence Gap Healing
1. **Inject Fault:** Pause or kill the WebSocket ingestion collector for $20\text{ seconds}$ during active market trading, or drop $50$ consecutive `trade_id` records before writing to Kafka.
2. **Expected Speed Behavior:** Speed Layer processes remaining stream with low latency and flags `missing_sequence_count > 0`.
3. **Expected Batch/Reconciliation Behavior:** Batch/backfill job detects the gap, fetches missing trades via REST/historical source, merges into Lakehouse, and reduces post-reconciliation $\text{R}_{\text{gap}}$ to $0\%$.

### Test Case 2: Late-Arriving Out-of-Order Event Injection
1. **Inject Fault:** Publish synthetic trade events to Kafka whose `event_time` is delayed by $2\times$ the Speed Layer watermark (e.g., $30\text{ seconds}$ late) after the $1\text{-minute}$ candle has already closed in the Speed Layer.
2. **Expected Speed Behavior:** Speed Layer drops the late events to preserve latency and memory bounds ($\Delta V_{\%} > 0\%$ initially).
3. **Expected Batch/Reconciliation Behavior:** Next Batch micro-batch/scheduled run includes the late events based on `event_time`, updates the Gold table via `MERGE INTO`, and reduces final Serving Layer $\Delta V_{\%}$ to $0\%$.

### Test Case 3: Protocol Seam Duplicate Flood
1. **Inject Fault:** Replay the last $1,000$ Kafka messages twice into the ingestion topic (simulating a $10\%$ duplicate burst during WebSocket reconnect + REST overlap).
2. **Expected Behavior:** Neither the Silver trade table nor the Gold OHLCV volume double-counts the replayed trades. Post-merge `COUNT(*) == COUNT(DISTINCT trade_id)`.

### Test Case 4: $10\times$ Flash Crash Replay Burst (Backpressure & Resource Check)
1. **Inject Fault:** Replay a high-volatility historical batch at $10\times$ normal ingestion rate for $3\text{ minutes}$.
2. **Expected Behavior:**
   - Zero Out-Of-Memory (OOM) exceptions or corrupted Spark/Flink checkpoints.
   - Speed Layer latency recovers smoothly once the spike subsides.
   - Small files on Lakehouse storage are compacted or bounded (`OPTIMIZE` / compaction job verified so $24/7$ streaming does not degrade read latency).

---

## 5. 24/7 Burn-In & Benchmark Readiness Gate Checklist

Run through this final operational checklist before starting the $24/7$ benchmark:

- [ ] **Checkpointing & State Cleanup:** Streaming checkpoints are persisted on reliable storage, and state TTL/Watermarks are active so RAM usage plateaus over $24\text{ hours}$.
- [ ] **Small-File Compaction Strategy:** Delta Lake / Iceberg table maintenance (`OPTIMIZE` / `VACUUM` or auto-compaction) is scheduled so continuous micro-batches do not create millions of tiny Parquet files during a $24\text{-hour}$ run.
- [ ] **Observability & Telemetry Logging:** Every reconciliation run appends a summary row to `reconciliation_audit_metrics` with timestamps, CPU/Memory usage, execution duration, $\text{R}_{\text{gap}}$, $\Delta V_{\%}$, and $\varepsilon_{\text{VWAP}}$ so thesis charts can be generated directly from the Lakehouse.
- [ ] **Rate Limit Guardrails:** REST backfill client includes exponential backoff and respects HTTP $429$ `Retry-After` headers.

---

## 6. Required Agent Output Format (Audit Report)

After inspecting the codebase and executing the tests above, generate a concise **Pre-Benchmark Audit Report** structured as follows:

1. **Architecture & Codebase Inspection Summary:** Pass/Warn/Fail status for each of the 5 Domain Problems, citing exact file paths and line numbers in the repository.
2. **Fixes Applied (if any):** Any missing deduplication keys, watermark adjustments, `MERGE INTO` conditions, or compaction configs you patched during the audit.
3. **Fault Injection Test Results Table:**
   | Test Case | Injected Anomaly | Pre-Reconciliation Drift ($\Delta V_{\%}$ / Gaps) | Post-Reconciliation Drift | Convergence Time ($T_{\text{conv}}$) | Status |
   | :--- | :--- | :--- | :--- | :--- | :--- |
4. **Go / No-Go Recommendation for 24/7 Streaming:** Clear confirmation that the pipeline is stable and ready for the $24/7$ benchmark run.