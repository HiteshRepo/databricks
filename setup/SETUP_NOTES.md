# Setup Notes

## What we did

### 1. Created the catalog, schemas and volumes

The notebook `setup/00_catalog_setup.py` was imported into the workspace and run manually.
It creates the Unity Catalog hierarchy:

```
retailflow (catalog)
├── raw     (schema)
├── silver  (schema)
└── gold    (schema)

retailflow.raw.landing     (managed volume)
retailflow.raw.checkpoints (managed volume)
```

Because the metastore has no default storage root, catalog and schema creation was done
via CLI rather than SQL `CREATE CATALOG`. See `KNOWN_ISSUES.md` for details.

CLI commands used:

```bash
# Catalog
databricks catalogs create retailflow \
  --storage-root "$CATALOG_STORAGE_ROOT" \
  --comment "RetailFlow study project — Databricks DE Professional exam prep" \
  --profile DEV

# Schemas
databricks schemas create raw    retailflow --comment "Landing zone: raw Delta tables and Auto Loader targets" --profile DEV
databricks schemas create silver retailflow --comment "Validated and deduplicated data" --profile DEV
databricks schemas create gold   retailflow --comment "Aggregated, business-ready tables" --profile DEV

# Volumes — done via SQL in the notebook (CLI does not support managed volume creation)
# CREATE VOLUME IF NOT EXISTS retailflow.raw.landing    COMMENT '...';
# CREATE VOLUME IF NOT EXISTS retailflow.raw.checkpoints COMMENT '...';
```

Notebook imported with:

```bash
databricks workspace import \
  --file setup/00_catalog_setup.py \
  --language PYTHON \
  --overwrite \
  --profile DEV \
  /Users/hitesh.pattanayak@veeam.com/retailflow/setup/00_catalog_setup
```

---

### 2. Generated sample data locally and uploaded to volumes

Sample data was generated locally using `setup/01_generate_data.py`:

```bash
.venv/bin/python setup/01_generate_data.py
```

Outputs to `data/`:
- `customers/` — 3 batches (1,000 records total) + `batch_002_updates.json` (50 loyalty-tier updates for CDF demo)
- `products/` — 2 batches (200 records)
- `orders/` — 5 batches (5,000 records, ~10% late-arriving for watermark demos)

Uploaded to UC volumes:

```bash
databricks fs cp -r data/customers dbfs:/Volumes/retailflow/raw/landing/customers --profile DEV
databricks fs cp -r data/products  dbfs:/Volumes/retailflow/raw/landing/products  --profile DEV
databricks fs cp -r data/orders    dbfs:/Volumes/retailflow/raw/landing/orders    --profile DEV
```

---

## Why upload raw JSON files to volumes instead of creating tables with SQL INSERT?

The files in `landing/` simulate **external data arriving from upstream systems** — an
e-commerce platform dropping order events, a CRM exporting customer profiles, etc. In
production this is exactly how data lands: as files in ADLS/S3, not as SQL inserts.

Uploading raw files to volumes lets us practise the full ingestion pipeline:

| Day | What reads from landing/ |
|-----|--------------------------|
| 3   | Auto Loader (`cloudFiles`) — incrementally detects and ingests new files |
| 4-5 | DLT Bronze layer — reads from Auto Loader source |
| 6   | Structured Streaming orders job |

### Alternative pattern: Event Hubs → Structured Streaming → MERGE

In production systems, a common alternative is to skip the file landing zone entirely:
events stream directly from **Azure Event Hubs** into a Structured Streaming job that
reads with `readStream.format("eventhubs")` and MERGEs into Delta tables. This is used
in our internal pipelines.

### Pattern comparison: production trade-offs

**File landing zone (ADLS → Auto Loader → Delta)**

Pros:
- **Decoupled** — the source system doesn't need to know Databricks exists. It just writes files. You can swap the ingestion engine without touching upstream.
- **Replayable** — files sit in ADLS until you delete them. If your pipeline breaks, you reprocess from the same files. No data loss.
- **Auditability** — the raw file is the source of truth. You can always trace a record back to the exact file it came from.
- **Handles bursty drops** — Auto Loader queues files and processes them incrementally. A vendor drops 10,000 files at 2am, Auto Loader works through them at its own pace.
- **Schema drift friendly** — `_rescued_data` catches fields that don't fit the schema without failing the pipeline.

Cons:
- **Latency** — inherently batch or micro-batch. You're always behind by at least one file drop cycle. Not suitable for sub-minute freshness requirements.
- **File management overhead** — you need a landing zone cleanup strategy, else ADLS fills up with raw files indefinitely.
- **Small file problem** — if sources drop many tiny files frequently, you accumulate small files that hurt query performance downstream (OPTIMIZE/compaction needed).

---

**Event Hubs → Structured Streaming → MERGE**

Pros:
- **Low latency** — events land in seconds. Near real-time freshness.
- **No file management** — EH handles retention, partitioning, and consumer offsets. No landing zone to clean up.
- **Natural for CDC** — status changes, updates, deletes flow as discrete events. Fits the MERGE pattern cleanly.
- **Backpressure handled** — EH buffers spikes. Streaming job catches up at its own rate.

Cons:
- **Retention window** — EH retains data for 1–7 days (up to 90 days on premium). If your pipeline is down longer than the retention window, you lose events permanently. No reprocessing from scratch.
- **Tightly coupled** — your pipeline is coupled to EH's partition count, consumer group limits, and connection strings. Migrating to a different source requires pipeline changes.
- **Harder to backfill** — if you need to reload 6 months of history, EH won't have it. You'd need a separate backfill path (usually files).
- **Exactly-once is harder** — requires careful offset management and idempotent MERGEs to avoid duplicates on restart.
- **Cost at scale** — EH pricing is based on throughput units. High-volume streams get expensive.

---

**When to choose which**

| Signal | Pattern |
|---|---|
| Source is a third-party system that exports files | File landing zone |
| Freshness requirement is hours or days | File landing zone |
| You need indefinite reprocessability | File landing zone |
| Source is an internal microservice emitting events | EH → Streaming |
| Freshness requirement is seconds to minutes | EH → Streaming |
| Data model is event-driven (status changes, CDC) | EH → Streaming |
| You need both history and real-time | Both — EH for live, files for backfill |

The last row is what most mature pipelines end up doing — EH handles the live stream,
a separate historical file dump handles backfill and recovery. Two ingestion paths
merging into the same Bronze table.

### What we lose by skipping the file landing zone

If we had inserted directly into Delta tables with SQL, we would:
- Skip the Auto Loader pattern entirely (a major exam topic)
- Lose the ability to simulate incremental file arrivals (`maxFilesPerTrigger`, checkpoint behaviour)
- Lose the `_rescued_data` rescue column demo (requires files with schema drift)
- Lose the late-arriving orders in the raw files (the `ingested_at` delay in `make_order`)

The batched files are also intentional — `batch_001` arrives first, `batch_002` later —
so that Auto Loader's exactly-once guarantee and checkpoint state can be observed across
multiple trigger runs.
