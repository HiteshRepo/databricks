# Databricks Certified Data Engineer Professional — Study Plan

## Exam Facts

| Item | Detail |
|---|---|
| Questions | 59 multiple-choice (scenario-based) |
| Duration | 120 minutes (~2 min/question) |
| Passing score | ~70% |
| Cost | $200 USD |
| Renewal | Every 2 years |
| Format | Scenario-based — tests *best approach*, not trivia |

---

## Exam Domains

| Domain | Weight | Priority |
|---|---|---|
| Python & SQL for Data Processing | 22% | Critical |
| Cost & Performance Optimization | 13% | Critical |
| Data Transformation, Cleansing & Quality | 10% | High |
| Monitoring & Alerting | 10% | High |
| Debugging & Deploying | 10% | High |
| Ensuring Data Security & Compliance | 10% | High |
| Data Ingestion & Acquisition | 7% | Medium |
| Data Governance | 7% | Medium |
| Data Modelling | 6% | Medium |
| Data Sharing & Federation | 5% | Lower |

---

## Project: RetailFlow — End-to-End Retail Analytics Platform

A realistic e-commerce data platform: raw events land in ADLS, flow through a medallion pipeline,
get governed via Unity Catalog, and served to downstream consumers — with full observability and
deployment automation.

### Architecture

```
ADLS Gen2 (Landing Zone)
  /raw/customers/    <- batch JSON drops
  /raw/products/     <- batch JSON drops
  /raw/orders/       <- streaming JSON drops

        | Auto Loader (Day 3)

DLT Pipeline (Days 4-5)
  Bronze: raw ingestion with schema enforcement
  Silver: validated, deduplicated, expectations enforced
  Gold:   SCD2 customer dim, order aggregations, product metrics

        |

Advanced Patterns (Days 6-7)
  Streaming orders with watermarks + tumbling windows
  Spark UI / EXPLAIN / AQE tuning

        |

Governance & Security (Day 8)
  Unity Catalog grants, column masking (email/phone)
  GDPR hard delete via REORG PURGE

        |

Operations (Days 9-10)
  DABs deployment, job orchestration
  Monitoring, alerts, Delta Sharing
```

### Data Model

| Entity | Characteristics | Patterns exercised |
|---|---|---|
| `customers` | PII (name, email, phone), profiles change over time | SCD2, CDF, column masking, GDPR delete |
| `products` | Catalog updates, prices change | MERGE, OPTIMIZE, ZORDER |
| `orders` | High-volume, late-arriving events | Streaming, watermarks, window functions |
| `order_events` | Status changes (CREATED/SHIPPED/DELIVERED/CANCELLED) | CDC via DLT APPLY CHANGES |

### Unity Catalog Layout

```
retailflow (catalog)
├── raw     (schema)  — Auto Loader landing, raw Delta tables
├── silver  (schema)  — validated customers, products, orders
└── gold    (schema)  — customer_dim (SCD2), daily_sales, product_metrics
```

---

## 10-Day Curriculum (2 hours/day)

### Day 1 — Delta Lake Deep Dive + Change Data Feed

**Concepts:** MERGE edge cases, schema evolution, time travel, CDF

**What to build:**
- Create `retailflow.raw.customers` and `retailflow.raw.products` Delta tables manually
- Run MERGE with all clauses: `whenMatched`, `whenNotMatched`, `whenNotMatchedBySource`
- Test schema evolution with `mergeSchema` option
- Query time travel: `SELECT * FROM customers VERSION AS OF 3`
- Enable CDF on the customers table, insert/update some rows, query `table_changes()`

**Key exam topics:**
- `whenNotMatchedBySource` DELETE semantics
- Schema evolution modes (`mergeSchema` vs `overwriteSchema`)
- CDF read options: `readChangeFeed`, `startingVersion`, `startingTimestamp`
- Transaction log and optimistic concurrency control

---

### Day 2 — Table Maintenance: VACUUM, OPTIMIZE, ZORDER, Liquid Clustering

**Concepts:** Table maintenance commands, file compaction, query optimization strategies

**What to build:**
- Run `OPTIMIZE retailflow.raw.customers` — observe file compaction
- Add `ZORDER BY (customer_id)` — understand when to use vs partitioning
- Run `VACUUM` with and without retention override — understand the 7-day default
- Convert a table to use liquid clustering: `CLUSTER BY (customer_id, region)`
- Run `REORG TABLE customers APPLY (PURGE)` to understand GDPR hard delete prep

**Key exam topics:**
- ZORDER vs partitioning vs liquid clustering — trade-offs
- VACUUM retention period and `spark.databricks.delta.retentionDurationCheck.enabled`
- When OPTIMIZE runs automatically vs manually
- `DESCRIBE HISTORY` to inspect table operations

---

### Day 3 — Auto Loader

**Concepts:** Incremental file ingestion with `cloudFiles`

**What to build:**
- Write Auto Loader notebooks for all 3 sources (customers, products, orders) reading from ADLS Gen2
- Configure schema inference with `cloudFiles.schemaLocation`
- Test `maxFilesPerTrigger` and `maxBytesPerTrigger`
- Observe the rescue data column (`_rescued_data`) with a malformed record
- Compare `trigger(once=True)` vs `trigger(availableNow=True)`

**Key exam topics:**
- `cloudFiles` source format options
- Schema inference vs schema hints vs schema enforcement
- Exactly-once guarantees via checkpointing
- `trigger(availableNow=True)` vs `trigger(processingTime=...)` vs continuous

---

### Day 4 — DLT Pipelines: Foundations

**Concepts:** Declarative pipeline syntax, expectations, bronze→silver

**What to build:**
- Create a DLT pipeline in your workspace
- Bronze layer: `@dlt.table` reading from Auto Loader sources
- Silver layer: add data quality expectations
  - `@dlt.expect` — log violations but continue
  - `@dlt.expect_or_drop` — drop invalid rows
  - `@dlt.expect_or_fail` — fail pipeline on violation
- Run the pipeline in triggered mode, inspect the event log

**Key exam topics:**
- Difference between `dlt.table` (materialized view) and `dlt.read_stream` (streaming live table)
- Expectation severity levels and when to use each
- Pipeline event log: `event_log()` table function
- Triggered vs continuous pipeline execution mode

---

### Day 5 — DLT Pipelines: Advanced

**Concepts:** CDC via APPLY CHANGES, streaming live tables, gold layer, pipeline metrics

**What to build:**
- Add gold layer to the Day 4 pipeline:
  - `customer_dim` with SCD2 using `APPLY CHANGES INTO` (sequence by updated_at, stored as SCD Type 2)
  - `daily_sales` as a materialized view with aggregations
- Test `APPLY CHANGES INTO` with out-of-order records
- Query the pipeline quality metrics table: `retailflow.silver.quality_metrics`
- Inspect lineage in Unity Catalog UI

**Key exam topics:**
- `APPLY CHANGES INTO` vs manual MERGE — when to use each
- SCD Type 1 vs Type 2 in DLT (`STORED AS SCD TYPE 2`)
- Streaming live tables vs materialized views — refresh semantics
- DLT pipeline graph and dependency resolution

---

### Day 6 — Structured Streaming: Watermarks & Window Functions

**Concepts:** Output modes, trigger types, time-based aggregations, late data handling

**What to build:**
- Write a streaming job separate from DLT reading the orders stream
- Tumbling window: order count per 1-hour window grouped by region
- Sliding window: 1-hour window sliding every 15 minutes
- Add a watermark (`withWatermark("order_time", "30 minutes")`) to handle late orders
- Test all output modes: append, complete, update — observe which works with which operation
- Write to a Delta table using `foreachBatch` vs direct `writeStream`

**Key exam topics:**
- Watermark semantics: when state is dropped, late data handling
- Output mode restrictions (complete requires aggregation, append requires watermark with aggregation)
- `trigger(availableNow=True)` for incremental batch processing
- Stateful vs stateless streaming operations

---

### Day 7 — Spark Internals & Performance Optimization

**Concepts:** Spark execution model, AQE, skew handling, query planning

**What to build:**
- Run `EXPLAIN (cost, formatted)` on the gold aggregation queries — read the plan
- Open Spark UI: identify stages, tasks, shuffle read/write, stragglers
- Simulate skew: create a skewed orders table, observe task duration imbalance in Spark UI
- Enable AQE: `spark.sql.adaptive.enabled = true` — observe skew join optimization
- Test broadcast join threshold: `spark.sql.autoBroadcastJoinThreshold`
- Use `ANALYZE TABLE` to update statistics, compare plans before/after

**Key exam topics:**
- DAG, stages, tasks — how shuffles create stage boundaries
- AQE features: dynamic coalescing, skew join optimization, dynamic partition pruning
- When broadcast join is chosen automatically vs forced with `broadcast()` hint
- Reading `EXPLAIN` output: `BroadcastHashJoin` vs `SortMergeJoin`

---

### Day 8 — Unity Catalog Governance + Security + GDPR

**Concepts:** Grants, row filters, column masks, audit logs, GDPR hard deletes

**What to build:**
- Create two UC roles: `analyst` (read-only) and `engineer` (read-write)
- `GRANT SELECT ON TABLE retailflow.gold.daily_sales TO analyst`
- Create a column mask on `customers.email` — analysts see `***@***.com`, engineers see full value
- Create a row filter on `customers` — each analyst only sees their region
- GDPR delete workflow: soft delete flag → `REORG TABLE APPLY (PURGE)` → `VACUUM`
- Query system audit log: `SELECT * FROM system.access.audit WHERE user_identity LIKE '%analyst%'`

**Key exam topics:**
- `GRANT`/`REVOKE` syntax on catalog, schema, table, column level
- Column masks vs dynamic views — when to use each
- Row-level security with row filter functions
- `system.access.audit` for compliance reporting
- GDPR deletion: why VACUUM alone is insufficient, why PURGE is needed first

---

### Day 9 — Databricks Asset Bundles + Job Orchestration

**Concepts:** DABs deployment, multi-task jobs, cluster policies, repair runs

**What to build:**
- Install Databricks CLI, authenticate to your DEV workspace
- Write `databricks.yml` to define the RetailFlow pipeline as a bundle:
  - DLT pipeline resource
  - Multi-task job: Auto Loader → DLT pipeline → gold aggregation notebook
  - Task dependencies with `depends_on`
- `databricks bundle validate` → `databricks bundle deploy` → `databricks bundle run`
- Simulate a task failure, use `databricks jobs run repair` to resume from failed task
- Define a job cluster in the bundle (vs all-purpose cluster) — understand cost implications

**Key exam topics:**
- `databricks.yml` structure: resources, targets (dev/staging/prod)
- `databricks bundle deploy` vs `databricks bundle run`
- Job cluster lifecycle — spins up per run, cheaper than all-purpose
- Task retry policies, timeout settings, on-failure email alerts
- `dbutils.notebook.run` vs multi-task job dependencies

---

### Day 10 — Monitoring, Alerting & Delta Sharing

**Concepts:** Job alerts, streaming query listeners, Ganglia UI, Delta Sharing

**What to build:**
- Add failure/success email alert to the RetailFlow job
- Add a streaming query listener to the orders streaming job — log batch processing time
- Inspect Ganglia metrics on a running cluster (CPU, memory, network)
- Create a Delta Share:
  - Create a share object: `CREATE SHARE retailflow_share`
  - Add gold tables to it: `ALTER SHARE retailflow_share ADD TABLE retailflow.gold.daily_sales`
  - Create a recipient and generate activation link
- Review job run history and cluster event logs in the UI

**Key exam topics:**
- Alert types: job failure, streaming backlog, query duration
- `StreamingQueryListener` — `onQueryProgress`, `onQueryTerminated`
- Ganglia vs Spark UI — what each shows
- Delta Sharing protocol: provider/share/recipient model
- Cluster event log vs driver logs vs Spark event log — which to use for what

---

## Practice Exams (Separate from the 10-day curriculum)

Schedule these after completing the 10-day build:

- [SkillCertPro Practice Tests](https://skillcertpro.com/product/databricks-data-engineer-professional-practice-tests/)
- [Udemy Databricks DE Professional Prep](https://www.udemy.com/course/databricks-certified-data-engineer-professional/)
- [ExamTopics Free Questions](https://www.examtopics.com/exams/databricks/certified-data-engineer-professional/)

**Strategy:** Take timed full mocks. Review every wrong answer against the official docs — understand *why*, not just what.

---

## Key Resources

| Resource | Use for |
|---|---|
| [Databricks Documentation](https://docs.databricks.com) | Authoritative reference for all topics |
| [Advanced Data Engineering with Databricks](https://www.databricks.com/training/catalog/advanced-data-engineering-with-databricks-971) | Official course — covers DLT, Auto Loader, governance |
| [Data Engineering with Databricks](https://www.databricks.com/training/catalog/data-engineering-with-databricks-911) | Foundation course if needed |
| [Delta Lake Documentation](https://docs.delta.io/latest/index.html) | Deep dive on Delta internals |
| [Unity Catalog Documentation](https://docs.databricks.com/en/data-governance/unity-catalog/index.html) | Governance, grants, masking |
| [Databricks Asset Bundles](https://docs.databricks.com/en/dev-tools/bundles/index.html) | DABs reference |

---

## Exam Tips

- **Scenario questions:** Always ask "which is the *best* approach given the constraints?" — not just "does this work?"
- **DLT vs Structured Streaming:** DLT is preferred for production ETL in the exam; raw Structured Streaming is for custom low-level control
- **Auto Loader vs COPY INTO:** Auto Loader scales better for large file counts; COPY INTO is simpler for small batches
- **ZORDER vs Liquid Clustering:** Liquid clustering is the modern replacement; prefer it for new tables
- **Job cluster vs all-purpose cluster:** Job cluster = cheaper, spins up per run; all-purpose = interactive development
- **Time management:** ~2 min/question — skip and return if stuck, don't lose time on one question
