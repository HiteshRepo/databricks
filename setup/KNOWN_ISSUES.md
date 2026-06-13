# Known Issues & Learnings

## Issue: CREATE CATALOG fails with missing metastore storage root

### Error
```
AnalysisException: [INVALID_STATE] Metastore storage root URL does not exist.
Default Storage is enabled in your account. You can use the UI to create a new
catalog using Default Storage, or please provide a storage location for the
catalog (for example 'CREATE CATALOG myCatalog MANAGED LOCATION '<location-path>').
```

### Root cause
The Unity Catalog metastore (`metastore_azure_swedencentral`) was created without a
default storage root URL. This means any `CREATE CATALOG` statement must provide an
explicit `MANAGED LOCATION` — the metastore cannot fall back to a default.

This is common in shared/enterprise workspaces where the metastore was bootstrapped
by infra tooling (Pulumi in this case) without a global default.

### Fix
Provide `MANAGED LOCATION` explicitly, pointing to an external location the user has
access to. We used the workspace-level ADLS container that backs the `dbw_dev_sdc_qbcu`
catalog:

```sql
CREATE CATALOG IF NOT EXISTS retailflow
  MANAGED LOCATION '$CATALOG_STORAGE_ROOT'
  COMMENT 'RetailFlow study project — Databricks DE Professional exam prep';
```

Or via Databricks CLI (bypasses the SQL limitation entirely):
```bash
databricks catalogs create retailflow \
  --storage-root "$CATALOG_STORAGE_ROOT" \
  --comment "RetailFlow study project — Databricks DE Professional exam prep" \
  --profile DEV
```

### How we found the storage root
Listed existing managed catalogs to find a valid storage root:
```bash
databricks catalogs list --profile DEV
databricks catalogs get dbw_dev_sdc_qbcu --profile DEV
# storage_root: $CATALOG_STORAGE_ROOT
```

### Exam relevance
On the exam, this surfaces as: *"When would you need MANAGED LOCATION in CREATE CATALOG?"*
— Answer: when the metastore has no default storage root, or when you want the catalog's
managed tables stored in a specific ADLS path rather than the metastore default.

---

## Issue: Databricks CLI workspace import syntax changed

### Error
```
Error: accepts 1 arg(s), received 2
```

### Root cause
Older CLI versions accepted positional `SOURCE TARGET` syntax. The current CLI (v0.200+)
uses flags — the target path is the only positional arg, source is `--file`.

### Fix
```bash
# Old syntax (broken):
databricks workspace import setup/00_catalog_setup.py /path/in/workspace --language PYTHON

# New syntax (correct):
databricks workspace import \
  --file setup/00_catalog_setup.py \
  --language PYTHON \
  --overwrite \
  --profile DEV \
  /Users/hitesh.pattanayak@veeam.com/retailflow/setup/00_catalog_setup
```

Also note: `databricks workspace mkdirs` on `/Users` fails with `Folder Users is protected`
— the parent dirs are created automatically by the import command itself.

---

## Issue: databricks schemas create flag name

### Error
```
Error: unknown flag: --catalog-name
```

### Fix
Catalog name is a positional argument, not a flag:
```bash
databricks schemas create <schema-name> <catalog-name> --profile DEV
```
