# Distributed ETL Pipeline for IoT Temperature Sensor Data

## Overview

This project builds a distributed ETL (Extract, Transform, Load) pipeline for IoT temperature sensor data using Apache Spark on Databricks Free Edition. The domain simulates a real-world IoT fleet of 1,000 sensors spread across four geographic regions (North, South, East, West), each reporting temperature, humidity, battery level, and event type readings at high frequency.

The core motivation mirrors the problem statement directly: traditional single-machine tools like Pandas cannot handle gigabytes of event data without running out of memory. This pipeline demonstrates how Spark distributes that same workload across partitions, processes it in parallel, and stores the output in a format optimised for downstream analytical queries.

**Total data processed:** 12,000,000 rows (~975 MB as CSV, ~1.7 KB per region as aggregated Parquet)
**Corruption rate caught and measured:** 4.43% (531,356 rows filtered)
**Platform:** Databricks Free Edition, Serverless Compute, Unity Catalog

---

## Architecture

```
01_data_generation notebook
         │
         ▼
[Spark range(12M) + distributed column generation]
         │
         ▼
/Volumes/workspace/default/iot_pipeline/raw/
  part-00000.csv   (~244 MB)     ← 1 file per Spark partition
  part-00001.csv   (~244 MB)
  part-00002.csv   (~244 MB)
  part-00003.csv   (~244 MB)
         │
         ▼
02_ingestion notebook
         │
         ├── Explicit StructType schema (8 columns, no inference)
         ├── try_to_timestamp() — safe cast, nulls on failure
         ├── Filter: null device_id, invalid timestamp, extreme temperature
         │
         ▼
df_clean  (11,468,644 rows — 4.43% dropped)
         │
         ├── withColumn: event_hour, event_day, event_month
         │
         ├── groupBy("region")          → df_region_summary  (4 rows)
         ├── groupBy("region","event_hour") → df_hourly_summary (96 rows)
         └── Window rank by avg_temp    → df_top3_per_region  (12 rows)
         │
         ▼
/Volumes/workspace/default/iot_pipeline/output/
  region_summary/
    region=East/part-00000.snappy.parquet
    region=North/part-00000.snappy.parquet
    region=South/part-00000.snappy.parquet
    region=West/part-00000.snappy.parquet
  hourly_summary/
    region=East/part-00000.snappy.parquet
    region=North/part-00000.snappy.parquet
    region=South/part-00000.snappy.parquet
    region=West/part-00000.snappy.parquet
```

---

## Project Structure

```
01_data_generation.ipynb   — Synthetic dataset generator (PySpark, no Pandas)
02_ingestion.ipynb         — Schema enforcement, cleaning, transforms, Parquet write

/Volumes/workspace/default/iot_pipeline/
  raw/                     — 4 CSV input files (~975 MB total)
  output/
    region_summary/        — Partitioned Parquet, one row per region
    hourly_summary/        — Partitioned Parquet, 24 rows per region
```

---

## Schema

| Column | Type | Nullable | Notes |
|--------|------|----------|-------|
| `device_id` | StringType | Yes | 0.5% null injected — unregistered sensor |
| `timestamp` | StringType → TimestampType | Yes | 1% "INVALID_DATE" injected — firmware bug |
| `temperature_c` | FloatType | Yes | 2% null + 1% extreme (-999/9999) injected |
| `humidity_pct` | FloatType | Yes | 2% null injected |
| `region` | StringType | No | North / South / East / West |
| `status` | StringType | No | OK / WARNING / CRITICAL / OFFLINE |
| `battery_level` | FloatType | Yes | 1% null injected |
| `event_type` | StringType | No | TEMP_READING / BATTERY_LOW / THRESHOLD_BREACH / HEARTBEAT |

**Note:** `timestamp` is read as `StringType` intentionally. Declaring `TimestampType` at read time in PERMISSIVE mode silently nulls valid timestamps alongside invalid ones. We read as `String`, then cast explicitly using `F.try_to_timestamp()` — which returns `null` on failure without throwing — and filter afterward.

---

## How to Run

### Prerequisites
- Databricks Free Edition account (community.cloud.databricks.com)
- Serverless compute attached to notebook (Default Interactive Compute)
- Unity Catalog Volume created:

```sql
CREATE VOLUME IF NOT EXISTS workspace.default.iot_pipeline
```

### Step 1 — Generate the dataset

Open `01_data_generation` notebook and run all cells in order:

```
Cell 1 → Imports and path constants
Cell 2 → Build distributed DataFrame (lazy — no data generated yet)
Cell 3 → Write to CSV (triggers execution — ~20 seconds)
Cell 4 → Verify 4 files, ~975 MB total
```

**Expected output:**
```
CSV files   : 4
Total size  : ~0.95 GB
```

### Step 2 — Run the ETL pipeline

Open `02_ingestion` notebook and run all cells in order:

```
Cell 1  → Imports and path constants
Cell 2  → Define StructType schema
Cell 3  → Read CSVs with explicit schema (lazy)
Cell 4  → Cast timestamp, filter malformed rows (lazy)
Cell 5  → Count dirty vs clean rows (triggers execution)
Cell 6  → Display cleaned sample + null check
Cell 7  → Extract event_hour, event_day, event_month
Cell 8  → groupBy region aggregation
Cell 9  → groupBy region + hour aggregation
Cell 10 → Window function — top 3 hottest devices per region
Cell 11 → Write region_summary as partitioned Parquet
Cell 12 → Write hourly_summary as partitioned Parquet
Cell 13 → Read back and verify partition pruning
```

**Expected output from Cell 5:**
```
Raw row count   : 12,000,000
Clean row count : 11,468,644
Rows dropped    : 531,356  (4.43%)
```

**Expected output from Cell 13:**
```
Total rows across all regions: 96
East rows: 24
PartitionFilters: [isnotnull(region), (region = East)]
```

---

## What I Learned

### Lazy Evaluation
Spark does zero computation when you define a DataFrame — it builds a plan. The proof: Cell 2 took 0.058 seconds to "define" 12 million rows. Cell 3 took 19.1 seconds to actually generate and write them. The entire transformation chain (Column 7 through Cell 10) built in under a second. `.write` and `.count()` are actions — they are the trigger that executes the plan. Every `.withColumn()`, `.filter()`, `.groupBy()` before them is just adding to a recipe that hasn't been cooked yet.

### Schema Enforcement
Never rely on schema inference in production. Spark samples only the first few rows and guesses column types — a malformed row at position 500,000 is invisible during that sampling. An explicit `StructType` schema is a shared contract that every partition and every worker node agrees on before reading a single byte. We also learned that `F.to_timestamp()` throws in ANSI SQL mode (Databricks serverless default) on unparseable strings, while `F.try_to_timestamp()` returns `null` safely — matching PERMISSIVE intent without requiring mode changes.

### The Shuffle
`groupBy()` and `Window` functions both require a shuffle — Spark physically redistributing rows so all records sharing the same key end up in the same partition. This is the most expensive operation in distributed computing. Contrast: `F.hour(timestamp)` in Cell 7 needed zero shuffle because each row's hour can be computed from that row alone. `groupBy("region")` in Cell 8 needed a shuffle because "North" rows were scattered randomly across all 4 partitions and had to be physically co-located before any correct average could be computed.

### Parquet vs CSV
Parquet stores data column-by-column, not row-by-row. A query for `avg(temperature_c)` across 12 million rows reads only the `temperature_c` column bytes — never touching `device_id`, `humidity_pct`, or any other column. CSV reads every column on every row regardless of what you need. Parquet also embeds the schema in the file footer, eliminating the need for schema inference on every read.

---

## Trade-offs

### Why `region` Was Chosen as the Partition Key

`region` was chosen for three reasons:

**1. Cardinality is right.** Four distinct values (North, South, East, West) means four partition folders. Partitioning by `device_id` would create 1,000 folders — each holding only ~11,500 rows and ~1.7 KB of aggregated data. The overhead of opening, reading the footer of, and closing 1,000 files individually would cost more than the data itself. That's the small file problem. Four folders with balanced data is the right partition structure for this dataset.

**2. Query patterns align.** Real IoT analytics almost always filter or compare by region first — "show me East region readings", "compare North vs South temperatures." Partition pruning eliminates irrelevant regions before Spark opens a single file.

**3. Distribution is balanced.** Each region holds approximately 25% of all rows (~2.87M each), with less than 0.2% variance between the largest and smallest region. Skewed partitions — where one key holds 90% of the rows — would create a hot partition that processes in 10x longer than the others, making parallelism worthless.

### Impact of Partition Size on Performance

| Partition key | Folders | Rows per folder | File size | Assessment |
|---|---|---|---|---|
| `region` (our choice) | 4 | ~2.87M | ~244 MB raw | ✅ Balanced, prunable |
| `device_id` | 1,000 | ~11,500 | ~1.7 KB agg | ❌ Small file problem |
| `event_hour` | 24 | ~477K | ~10 MB raw | ⚠️ Acceptable, but hour alone isn't a useful query boundary |
| `event_month` | 12 | ~955K | ~20 MB raw | ✅ Good for time-series pipelines with monthly reporting |

The general rule: partition key cardinality should keep each output file between **128 MB and 1 GB** for typical Spark workloads. Below 128 MB, the small file problem degrades read performance. Above 1 GB, a single task has too much work and becomes a bottleneck.

### `coalesce()` vs `repartition()` Before Writing

`coalesce(1)` was used before both Parquet writes to ensure one file per partition folder. `coalesce()` merges existing partitions locally — no shuffle, low cost. `repartition(1)` achieves the same output file count but forces a full shuffle first. For reducing partition count before a write, `coalesce()` is always the right choice.

---

## Environment Notes

| Component | Value |
|---|---|
| Platform | Databricks Free Edition |
| Compute | Serverless CPU (Default Interactive Compute) |
| Spark Session | `pyspark.sql.connect.session.SparkSession` (Spark Connect) |
| Execution Engine | Photon (Databricks vectorized C++ engine, always-on in serverless) |
| Storage | Unity Catalog Volume — `workspace.default.iot_pipeline` |
| DBFS | Disabled (Unity Catalog workspace — public DBFS root is off by design) |
| Schema inference | Never used — all schemas declared explicitly via `StructType` |
