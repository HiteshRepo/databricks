# Day 1 Takeaways — Delta Lake Deep Dive + CDF

## Key Concepts
<!-- What did you learn? Bullet points, your own words. -->
- `createOrReplaceTempView("name")` registers a DataFrame as a named temp view in the Spark session catalog — no data is copied, it's a logical pointer to the execution plan
- Temp views exist only for the duration of the Spark session (not persisted to the metastore)
- This is the bridge between Python DataFrames and SQL cells — `%sql` can't access Python variables directly, so temp views are how you pass data across
- `WHEN NOT MATCHED BY SOURCE` is Delta-only — ANSI SQL has no clause for "target row exists but source row doesn't". Standard SQL requires a separate DELETE statement for this case, which breaks atomicity. Delta handles all three cases (matched, not matched, not matched by source) in a single atomic transaction
- `mergeSchema` has two ways to enable: `MERGE WITH SCHEMA EVOLUTION INTO ...` (SQL MERGE, DBR 15.2+) or `.option("mergeSchema", "true")` on the DataFrame writer. Both are statement-scoped — nothing leaks globally
- `spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")` is **blocked on serverless** with `CONFIG_NOT_AVAILABLE.SERVERLESS_DELTA_SCHEMA_AUTO_MERGE_ENABLED` — only works on classic clusters. On serverless, always use `WITH SCHEMA EVOLUTION` or `.option("mergeSchema", "true")`
- Neither `mergeSchema` nor `overwriteSchema` converts existing column types — Delta files are immutable Parquet and can't be edited in place. `mergeSchema` only handles additive changes (new columns); type mismatches on existing columns raise an `AnalysisException`
- `overwriteSchema` does NOT migrate existing rows to the new type — it deletes all existing data and replaces it with whatever you write. You are responsible for casting values in your DataFrame before the write
- `ALTER TABLE ... ALTER COLUMN price TYPE DOUBLE` is the only option that rewrites existing rows with type casting applied — but only allows safe widening casts (`INT → LONG`, `FLOAT → DOUBLE`, `INT → DOUBLE`). Incompatible casts like `STRING → INT` fail with `AnalysisException: Cannot change column 'price' from STRING to INT`
- For incompatible type changes where you want to preserve data, the only option is to read the old data, cast it yourself, then overwrite with `overwriteSchema`:
  ```python
  migrated_df = spark.table("retailflow.raw.products") \
      .withColumn("price", col("price").cast("INT"))  # you own the cast
  migrated_df.write.format("delta").option("overwriteSchema", "true").mode("overwrite").saveAsTable(...)
  ```
- `WHEN NOT MATCHED BY SOURCE` can DELETE or UPDATE (e.g. soft-delete with `is_active = false`) — it's not restricted to DELETE

## Exam Gotchas
<!-- Things the exam specifically tests that differ from what you'd intuitively expect. -->
- ANSI SQL MERGE only has two clauses; `WHEN NOT MATCHED BY SOURCE` is Delta-specific — if a question mentions deleting target rows absent from source in one atomic step, the answer is Delta MERGE not a separate DELETE
- `WHEN NOT MATCHED BY SOURCE` can UPDATE, not just DELETE — a soft-delete pattern (setting a flag) is a valid and common use of this clause
- Multiple `WHEN MATCHED` clauses are allowed — but only the **first matching clause** fires per row; order matters. If two clauses could both apply to the same row, the second is silently skipped — no error, just wrong behaviour. Put the most specific condition first.

## Syntax to Remember
<!-- Commands or code snippets you'll forget without writing them down. -->

```python
# Bridge a Python DataFrame into a SQL cell
source_df.createOrReplaceTempView("customers_source")
# Now usable in %sql as: USING customers_source AS s
```

```python
# Schema evolution — SQL MERGE (serverless-compatible, DBR 15.2+, statement-scoped)
# MERGE WITH SCHEMA EVOLUTION INTO target USING source ON ... WHEN ...

# Schema evolution — DataFrame write (serverless-compatible, scoped to one write only)
source_df.write \
    .format("delta") \
    .option("mergeSchema", "true") \
    .mode("append") \
    .saveAsTable("retailflow.raw.customers")

# Schema evolution — session config (classic clusters only; BLOCKED on serverless)
# spark.conf.set("spark.databricks.delta.schema.autoMerge.enabled", "true")

# Full schema replacement — deletes all existing rows, writes new DataFrame from scratch
df.write \
    .format("delta") \
    .option("overwriteSchema", "true") \
    .mode("overwrite") \
    .saveAsTable("retailflow.raw.products")
```

```sql
-- Change column type in-place — rewrites existing rows with cast applied (DBR 11.3+ / Unity Catalog)
ALTER TABLE retailflow.raw.products ALTER COLUMN price TYPE DOUBLE
```

## CDF Output Explained (real example)

```
customer_id  name          email                    _change_type      _commit_version
1            Alice Nguyen  alice.new@...            update_preimage   11
1            Alice Nguyen  alice.v2@...             update_postimage  11
3            Carol Smith   carol@...                delete            13
8            Henry Wu      henry@...                insert            15
```

**Reading this:**

- **Alice (v11)** — one UPDATE produces two rows at the same `_commit_version`. The preimage shows her state *before* the update (`alice.new@...` — set by the section 2 MERGE). The postimage shows her state *after* (`alice.v2@...` — set by the explicit UPDATE in section 5). To get current state, take `update_postimage` only.
- **Carol (v13)** — DELETE produces one row with her last known state. Useful for audit: you can see exactly what was deleted.
- **Henry (v15)** — INSERT produces one row. Straightforward — this is the row as it was written.
- **Commit versions are not consecutive** (11, 13, 15) — other operations happened between them (e.g. the DELETE on Carol was v13, Henry's INSERT was v15 because there was another operation at v14).
- **`loyalty_tier` is NULL for all rows** — this column was added after these rows were written; CDF records the row state at commit time, not the current schema.

**Downstream consumer pattern** — filter to only the rows you need:
```python
# Current state only (what the table looks like now)
current = changes_df.filter(col("_change_type").isin("insert", "update_postimage"))

# Full audit trail (what changed and from what)
audit = changes_df  # keep all rows including preimage and delete
```

## Aha Moments
<!-- Something that clicked, surprised you, or changed how you think about this topic. -->
- `VERSION AS OF 0` on a freshly created table returns no rows — version 0 is the `CREATE TABLE` operation, which only writes the schema. The first actual data lands in version 1 (the `INSERT`). Always check `DESCRIBE HISTORY` to know which version has the data you want
- RESTORE is a pure transaction log operation — it adds/removes file references in the log without rewriting any Parquet files. This makes it fast regardless of table size. It also restores the schema, since schema is stored in the log entry not in the files
- RESTORE and VACUUM are in tension: VACUUM permanently deletes the Parquet files that RESTORE needs to re-activate. If you VACUUM aggressively, you lose the ability to restore to older versions — the 7-day default retention exists precisely as a safety window
- Unity Catalog tables get **auto-OPTIMIZE** triggered automatically after writes (`"auto":"true"` in DESCRIBE HISTORY) — you may not need to run OPTIMIZE manually on UC tables at all
- MERGE uses **deletion vectors** for deletes/updates instead of rewriting the data file — a small bitmap file marks rows as logically deleted, the original Parquet stays untouched. The file is physically cleaned up on the next OPTIMIZE run
- MERGE isolation level is **WriteSerializable** (detects read + write conflicts); OPTIMIZE uses **SnapshotIsolation** (write-write conflicts only) — OPTIMIZE is safe with weaker isolation because it only rewrites files without changing row data
- **Rule of thumb for isolation levels:** if the transaction's write decisions depend on what it read, you need WriteSerializable. If the write is independent of what was read, SnapshotIsolation is sufficient
  - WriteSerializable: MERGE, UPDATE, DELETE — reads inform the write decision; a concurrent modification to those rows would invalidate the decision
  - SnapshotIsolation: OPTIMIZE, INSERT, COPY INTO — writes don't depend on reads; concurrent changes to the same rows don't affect correctness
  - Concrete risk: two concurrent MERGEs both read customer_id=1 with `updated_at=2024-01-01`, both decide to UPDATE. Under SnapshotIsolation both silently succeed and the second overwrites based on stale data. Under WriteSerializable the second detects a conflict and throws `ConcurrentModificationException`, forcing a retry with fresh data

## DLT (Delta Live Tables) — context only, covered Day 4–5

DLT is Databricks' declarative ETL framework — you declare what your tables should look like, Databricks handles execution, ordering, retries, and data quality. Contrast with imperative Spark code where you explicitly read, transform, and write.

Came up in Day 1 only because `APPLY CHANGES INTO` (a DLT feature) uses CDF under the hood — relevant for understanding why CDF matters beyond manual change reads. No DLT code until Day 4.

## Streaming CDF

**Regular Delta streaming vs CDF streaming:**

Without CDF, Delta streaming only handles appends cleanly. Updates/deletes cause:
```
AnalysisException: Detected a data update in the source table.
Use .option("ignoreChanges", "true") to ignore updates.
```
`ignoreChanges=true` suppresses the error but re-emits the **entire Parquet file** containing the updated row — if a file has 10,000 rows and 1 was updated, all 10,000 are re-processed. You can't tell which one changed.

CDF streaming gives you row-level change records instead:
```python
# Regular streaming — file-level, updates cause full-file re-emission
spark.readStream.format("delta") \
    .option("ignoreChanges", "true") \
    .table("retailflow.raw.customers")

# CDF streaming — row-level, exact change records only
spark.readStream.format("delta") \
    .option("readChangeFeed", "true") \
    .option("startingVersion", 0) \
    .table("retailflow.raw.customers")
```

**How micro-batches work (from the real CDF output):**
```
Micro-batch 1 (v11–v13):
  → Alice update_preimage   (v11)
  → Alice update_postimage  (v11)
  → Carol delete            (v13)
  checkpoint saved: last processed = v13

Micro-batch 2 (v14–v15):
  → Henry insert            (v15)
  checkpoint saved: last processed = v15
```
If the job crashes mid-batch, it restarts from the last saved checkpoint and reprocesses — this is the exactly-once guarantee.

**Why `readChangeFeed` and `ignoreChanges` are mutually exclusive:**
Both answer the same question ("what do I do when the source has updates?") differently:
- `ignoreChanges` — re-emit the whole file, handle duplicates downstream
- `readChangeFeed` — emit only the precise change records

Combining them throws an error — pick one.

**How DLT `APPLY CHANGES INTO` uses CDF under the hood:**
```python
# What you write in DLT:
dlt.apply_changes(
    target   = "customer_dim",
    source   = "retailflow.raw.customers",
    keys     = ["customer_id"],
    sequence_by = "updated_at"
)

# What DLT does internally:
# 1. Reads source as a CDF stream (readChangeFeed=true)
# 2. For each micro-batch:
#    - insert / update_postimage → MERGE into target by keys
#    - delete                   → DELETE from target
# 3. sequence_by handles out-of-order: if a DELETE (v15) arrives before an INSERT (v16)
#    for the same key, the INSERT wins because updated_at is later
```
The `sequence_by` column is what makes `APPLY CHANGES INTO` safe for late-arriving records — raw CDF streaming doesn't handle ordering for you.

## Common Traps
<!-- Mistakes easy to make in the exam or in practice. -->
- Incompatible column type changes (e.g. `STRING → INT`) fail for all three methods except `overwriteSchema` — but `overwriteSchema` deletes existing data, it doesn't migrate it. You must cast the old data yourself before overwriting
- `ALTER TABLE ALTER COLUMN` only allows safe widening casts — don't assume it handles arbitrary type changes
- `mergeSchema` is additive only — it will never resolve a type conflict on an existing column, it just adds missing columns
- RESTORE fails if VACUUM has already deleted the files for the target version — `DeltaAnalysisException: The underlying files have been deleted by VACUUM`. Aggressive VACUUM (`RETAIN 0 HOURS`) permanently destroys restore capability
- MERGE `operationMetrics` in DESCRIBE HISTORY tells you exactly what each clause did: `numTargetRowsMatchedUpdated`, `numTargetRowsInserted`, `numTargetRowsNotMatchedBySourceDeleted` — use these to verify MERGE behaviour, not just a `SELECT *`
- MERGE condition applies to **all** matched rows — if your `WHEN MATCHED` condition is `t.updated_at < s.updated_at` and the source has a newer timestamp for every row, every matched row gets updated, not just the ones with changed values
