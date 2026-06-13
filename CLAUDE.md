# RetailFlow — Databricks DE Professional Exam Prep

## Goal
Build the RetailFlow retail analytics platform incrementally across 10 sessions to cover
every topic on the Databricks Certified Data Engineer Professional exam.

## Key files
- `STUDY_PLAN.md` — full curriculum and exam domain breakdown
- `REVISION.md`   — aggregated cheat sheet (updated after each day)
- `databricks.yml` — DABs config (used from Day 9)
- `setup/`        — one-time workspace bootstrap scripts

## Workspace
- Profile: `DEV`
- Host: `$DATABRICKS_HOST` (set in `.env`)
- UC Catalog: `retailflow` (schemas: `raw`, `silver`, `gold`)
- Landing volume: `/Volumes/retailflow/raw/landing`
- Checkpoint volume: `/Volumes/retailflow/raw/checkpoints`

## Progress tracker
Update the status column at the end of each session.

| Day | Topic | Status |
|-----|-------|--------|
| Setup | Catalog, volumes, sample data, CLI auth | ✅ Done (2026-06-12) |
| 1 | Delta Lake deep dive + CDF | ⬜ Not started |
| 2 | VACUUM, OPTIMIZE, ZORDER, Liquid Clustering | ⬜ Not started |
| 3 | Auto Loader | ⬜ Not started |
| 4 | DLT Foundations | ⬜ Not started |
| 5 | DLT Advanced | ⬜ Not started |
| 6 | Streaming watermarks & windows | ⬜ Not started |
| 7 | Spark internals & performance | ⬜ Not started |
| 8 | Unity Catalog, governance & GDPR | ⬜ Not started |
| 9 | Databricks Asset Bundles & job orchestration | ⬜ Not started |
| 10 | Monitoring, alerting & Delta Sharing | ⬜ Not started |

## Current session
<!-- Update this block when starting a new session. -->
**Active day:** —
**Notebook:** —
**Resuming from:** —

## What Claude should know
- User is a senior data engineer with production Databricks experience (SCD2, MERGE,
  Structured Streaming, Unity Catalog, broadcast joins, DQX). Do not over-explain these.
- Gaps to fill: Auto Loader, DLT pipelines, CDF, VACUUM/ZORDER, DABs, watermarks,
  Spark internals deep dive, column masking, GDPR patterns, Delta Sharing.
- Each session = 2 hours. Stay focused on the active day's notebook. Do not spill into
  adjacent days unless explicitly asked.
- After building each section, prompt the user to fill in `dayXX/TAKEAWAYS.md`.
- Preferred style: build and explain as we go — no long theory dumps before coding.
