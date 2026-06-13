# Syncing Notebooks to Your Databricks Workspace

Each day's `.py` file is formatted as a Databricks notebook source file.
You have two options to get them into your workspace.

## Option A: Databricks CLI (recommended)

Import a single notebook:
```bash
databricks workspace import day01/delta_lake_and_cdf.py \
  /Users/<your-email>/retailflow/day01/delta_lake_and_cdf \
  --language PYTHON --profile DEV
```

Import all notebooks at once:
```bash
for f in day*/**.py; do
  dir=$(dirname $f)
  name=$(basename $f .py)
  databricks workspace mkdirs /Users/<your-email>/retailflow/$dir --profile DEV
  databricks workspace import $f /Users/<your-email>/retailflow/$dir/$name \
    --language PYTHON --overwrite --profile DEV
done
```

## Option B: Git Repos in Databricks UI

1. In your workspace: Workspace > Repos > Add Repo
2. Point it at this repo's remote URL
3. Pull — all files appear as notebooks automatically
4. Files with `# Databricks notebook source` header render as multi-cell notebooks

## Option C: Copy-paste (quick for single cells)

For one-off exploration during a day's session, just paste cells directly
into a new notebook in the UI. No sync needed.

---

## Re-authenticating the CLI

All profiles show as invalid. Re-authenticate the DEV profile:

```bash
databricks auth login \
  --host https://$DATABRICKS_HOST \
  --profile DEV
```

This opens a browser OAuth flow. After completing it, verify:

```bash
databricks auth env --profile DEV
databricks clusters list --profile DEV
```
