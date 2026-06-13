# Unity Catalog Primer

## What is a metastore?

Think of it as a three-layer hierarchy:

```
Account (Databricks account — one per organisation)
└── Metastore (one per region — the governance registry)
    ├── Workspace A  ──attached──┐
    ├── Workspace B  ──attached──┤  all share the same metastore
    └── Workspace C  ──attached──┘
        └── Catalog
            └── Schema
                └── Table / Volume / Function
```

**The metastore is not a database.** It stores only metadata — table definitions,
ownership, access policies, audit logs. The actual data lives in ADLS/S3.

To store that metadata, the metastore itself needs a storage account (its "storage
root"). This is a one-time config done when the metastore is first created at the
account level by an admin.

A workspace is **attached** to a metastore. Multiple workspaces in the same region
can share one metastore — meaning they see the same catalogs, tables, and access
policies. This is how enterprises share governed data across teams without copying it.

---

## The object hierarchy

| Level | What it is | Analogy |
|---|---|---|
| Metastore | The governance registry for a region | The database server |
| Catalog | Top-level namespace, has its own storage root | A database instance |
| Schema | Groups related tables together | A schema / namespace in SQL |
| Table | Delta table (managed or external) | A table |
| Volume | Managed file storage path inside UC | A folder with access control |

---

## Managed vs external storage

Both types store real files in ADLS. The difference is who controls the path and
what happens when you drop the table.

| | Managed | External |
|---|---|---|
| Files in ADLS? | Yes | Yes |
| Path chosen by | Databricks (under storage root) | You |
| Drop table → files | Deleted by Databricks | Remain untouched |


**Managed table/volume** — Files are written to ADLS under a path Databricks chooses
(beneath the catalog's `storage_location`). Drop the table → Databricks deletes the
files too. You don't manage the path.

**External table** — You point Databricks at a path in ADLS that you own and control.
Drop the table → only the metadata is removed, files remain in ADLS. Requires an
external location + storage credential to be registered in Unity Catalog first.

---

## Storage root and MANAGED LOCATION

Every level of the hierarchy (metastore → catalog → schema) can have its own storage
root. When you create a managed table, Databricks walks up the hierarchy to find the
nearest configured storage root:

```
Schema storage root       ← used first if set
  ↑ else
Catalog storage root      ← used if schema has none
  ↑ else
Metastore storage root    ← used if catalog has none
  ↑ else
Error                     ← nothing configured → CREATE CATALOG needs MANAGED LOCATION
```

### How the walk-up works in practice

When you run `CREATE TABLE retailflow.silver.customers`, Databricks needs to know
where in ADLS to write the files. It resolves this by walking up:

```
Step 1: Does retailflow.silver have a storage root?
        → No → go up

Step 2: Does retailflow (catalog) have a storage root?
        → Yes → use it. Files land at:
          abfss://...$WORKSPACE_ID/__unitystorage/catalogs/<id>/schemas/<id>/tables/<id>

Done. Metastore is never checked.
```

Databricks constructs the full nested path automatically — you never set it manually.

If the catalog also had no storage root, it would check the metastore. If nothing is
configured at any level → error. This is exactly what we hit: the metastore had no
root and the catalog didn't exist yet, so there was nowhere to resolve to.

### Overriding at any level

You can pin storage at schema or table level to override the inherited root:

```sql
-- All managed tables in silver go to a dedicated container
CREATE SCHEMA retailflow.silver
  MANAGED LOCATION 'abfss://silver-container@storageaccount.dfs.core.windows.net/';

-- Just this one table goes somewhere specific
CREATE TABLE retailflow.silver.customers
  MANAGED LOCATION 'abfss://another-container@storageaccount.dfs.core.windows.net/customers';
```

The most specific level always wins. This is useful when different schemas have
different data classification requirements (e.g. PII in a separate, more restricted
storage account).

In our workspace the metastore has no storage root (common in enterprise setups where
infra tooling provisions the metastore without a default). So `CREATE CATALOG` must
include `MANAGED LOCATION` explicitly — see `KNOWN_ISSUES.md`.

---

## Our workspace layout

```
metastore_azure_swedencentral  (shared across the DEV workspace)
└── retailflow  (catalog — owned by hitesh.pattanayak@veeam.com)
    ├── raw     (schema)
    │   ├── landing     (managed volume — JSON files land here)
    │   └── checkpoints (managed volume — streaming state)
    ├── silver  (schema)
    └── gold    (schema)
```

Storage root for the `retailflow` catalog:
```
$CATALOG_STORAGE_ROOT
```
