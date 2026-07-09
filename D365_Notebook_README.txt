================================================================================
D365 CUSTOM INGESTION NOTEBOOK — v5
Full-parity replacement for the Azure Databricks D365 Connector
================================================================================

FILES IN THIS PACKAGE
--------------------------------------------------------------------------------
  D365_Connector_Notebook_v5.py    The Databricks notebook (import this)
  D365_Notebook_Documentation.html Full documentation (open in browser)
  D365_Notebook_README.txt         This file — quick reference

--------------------------------------------------------------------------------
WHAT THIS NOTEBOOK DOES
--------------------------------------------------------------------------------
Ingests Microsoft Dynamics 365 (Dataverse) data into Databricks Delta tables.
Reads CSV files that Azure Synapse Link writes to ADLS Gen2, joins them with
the model.json header/schema file, applies inserts/updates/deletes, and writes
Delta tables to Unity Catalog — exactly what the managed D365 connector does.

Handles 200-600 tables with resume-from-failure. If a run fails at table #50,
the next run picks up from #50 — not from #1.

--------------------------------------------------------------------------------
QUICK START (5 STEPS)
--------------------------------------------------------------------------------
1. Import D365_Connector_Notebook_v5.py into your Databricks workspace
2. Edit Section 0 CONFIG:
     - adls_base_path        (your ADLS Gen2 path)
     - catalog               (Unity Catalog name)
     - raw_schema            (destination schema for raw tables)
     - tables_to_ingest      ([] = all tables, or specific list)
     - scd_type              (1 = overwrite, 2 = history)
3. Uncomment ONE authentication option in Section 0 and set your secrets
4. Attach the notebook to a cluster (Databricks Runtime 13.3+ recommended)
5. Run all cells

--------------------------------------------------------------------------------
COMMON COMMANDS (after loading all cells)
--------------------------------------------------------------------------------
  preview_ingestion()           See what will happen without running
  run_ingestion()               Run the full ingestion
  show_ingestion_status()       See current status of all tables
  retry_failed_only()           Retry ONLY failed tables
  reset_stuck_running()         Force-recover stuck tables (no wait)

--------------------------------------------------------------------------------
FAILURE RECOVERY — WHAT TO DO WHEN A RUN FAILS
--------------------------------------------------------------------------------

Scenario A: Cell failed, cluster still running
  Action:  Just re-run Section 16 (results = run_ingestion())
  Result:  Completed tables are skipped, failed tables retry from cursor

Scenario B: Cluster crashed / restarted
  Action:  Re-run all cells top-to-bottom (functions need reloading)
  Result:  Cursor state in Delta is preserved — resume works correctly

Scenario C: Want to retry ONLY failed tables
  Action:  retry_failed_only()
  Result:  Skips SUCCESS and never-run tables, retries only FAILED

Scenario D: Scheduled Job — Do Nothing
  Action:  Wait for next scheduled run
  Result:  Auto-resumes from where it stopped

--------------------------------------------------------------------------------
CONFIGURATION CHEAT SHEET
--------------------------------------------------------------------------------

Small run (test with 1 table):
    tables_to_ingest    = ["account"]
    scd_type            = 1
    batch_size          = 50
    resume_mode         = "resume"

Production (600 tables, hourly):
    tables_to_ingest    = []                    # all tables
    scd_type            = 1                     # or 2 for history
    batch_size          = 50                    # 12 batches of 50
    resume_mode         = "resume"              # skip completed
    resume_skip_minutes = 5                     # window
    stop_on_failure     = False                 # don't halt on 1 bad table
    stuck_running_hours = 2                     # auto-recover stuck

After schema change in D365:
    force_full_refresh  = True                  # one time
    # Reset to False after successful run

Retry-only mode:
    resume_mode         = "failed_only"
    # Or just call retry_failed_only()

--------------------------------------------------------------------------------
FEATURES IMPLEMENTED (95 verified against docs)
--------------------------------------------------------------------------------

CORE INGESTION
  * Reads model.json for schema auto-discovery
  * Reads headerless CSVs and applies column names from model.json
  * Per-table versionnumber cursor for incremental ingestion
  * Chronological folder processing
  * Snapshot folder handling (first run / full refresh)
  * Changelog folder handling (incremental)

SCD SUPPORT
  * SCD Type 1: overwrite in place
  * SCD Type 2: __START_AT / __END_AT / __CURRENT history columns
  * Hard delete handling (SCD1 removes rows)
  * Soft delete handling (SCD2 marks __CURRENT=false)

DATA TYPES (verified against Azure Databricks docs)
  * String, Integer, BigInt, Decimal, Double, Boolean → native
  * Money → DECIMAL(19,4)
  * DateTime, Date, Time → TIMESTAMP / DATE
  * GUID, Lookup → STRING
  * Picklist, State, Status → INTEGER
  * Multi-select Picklist → STRING (comma-separated)
  * Image, File → STRING (metadata only, no binary)

RELIABILITY & RESUME (v4 + v5)
  * Per-table cursor preserved on failure
  * mark_running status prevents double-processing
  * Exponential backoff retry (2s/4s/8s)
  * Stuck-RUNNING auto-recovery
  * Batching for 200-600 tables
  * Progress display [N/M]
  * Priority order: FAILED → never-run → stale → skip
  * Continue-on-failure (default)
  * Run history audit log

RERUN UX (v5)
  * preview_ingestion() dry-run
  * show_ingestion_status() dashboard
  * retry_failed_only() one-liner
  * reset_stuck_running() force recovery

UNITY CATALOG & DELTA
  * Auto-creates catalog schemas
  * Delta Change Data Feed enabled
  * Delta table properties tagged for traceability

DATA PATTERNS (utilities in Section 18)
  * OptionSetMetadata join (Picklist labels)
  * Lookup join pattern
  * Multi-select LATERAL VIEW EXPLODE
  * Attachment metadata query (annotation table)
  * SCD2 current-records query
  * SCD2 full-history query

F&O SUPPORT
  * Virtual entities (mserp_ prefix)
  * Direct tables
  * S2S auth guidance
  * Track Changes enablement note

LIMITATIONS DOCUMENTED
  * 250 tables/pipeline (this notebook uses batches)
  * Source table deletion != destination deletion
  * No auto column backfill
  * 5-15 min Synapse Link latency
  * F&O 15 min sync delay
  * No backfill on Synapse Link downtime

--------------------------------------------------------------------------------
MONITORING TABLES CREATED
--------------------------------------------------------------------------------

  <catalog>.<control_schema>.d365_ingestion_cursor
    One row per table. Tracks: last_versionnumber, last_folder_ts,
    last_run_ts, row_count, status (SUCCESS/FAILED/RUNNING),
    error_message, scd_type, is_full_refresh.

  <catalog>.<control_schema>.d365_run_history
    One row per run_ingestion() invocation. Tracks: run_id, timing,
    counts (in_scope, processed, succeeded, failed, skipped),
    total_rows, duration, status, notes.

  <catalog>.<raw_schema>.<table_name>
    One Delta table per ingested D365 table.

--------------------------------------------------------------------------------
TROUBLESHOOTING
--------------------------------------------------------------------------------

Error: "Cannot read model.json"
  Cause:  Synapse Link hasn't exported yet, or ADLS auth is wrong.
  Fix:    In Power Apps → Azure Synapse Link, verify status is Active.
          Verify service principal has Storage Blob Data Contributor role.

Error: "versionnumber missing from changelog"
  Cause:  Synapse Link not configured to export changelogs with versionnumber.
  Fix:    In Power Apps, reconfigure Synapse Link. Then set
          CONFIG["force_full_refresh"] = True and re-run once.

Error: "Type mismatch / Cannot cast"
  Cause:  D365 changed a column's data type.
  Fix:    Set CONFIG["force_full_refresh"] = True and re-run for the
          affected table only (tables_to_ingest = ["that_table"]).

Error: Table stuck in RUNNING for hours
  Cause:  Cluster crashed mid-processing.
  Fix:    Call reset_stuck_running() or wait stuck_running_hours (default 2h).

Error: 250-table limit warning
  Note:   Just a warning — this notebook handles it via batching.
          Adjust batch_size if runs are too long or too short.

--------------------------------------------------------------------------------
SCHEDULING AS A DATABRICKS JOB
--------------------------------------------------------------------------------

For production, schedule the notebook via Databricks Workflows:
  1. Workflows → Create Job
  2. Task type: Notebook
  3. Select D365_Connector_Notebook_v5
  4. Schedule: e.g. every 1 hour (matches Synapse Link export frequency)
  5. Cluster: use a job cluster for cost efficiency
  6. Notifications: set email on failure

Each scheduled run automatically resumes from where prior runs stopped.
No manual intervention needed for typical failure scenarios.

--------------------------------------------------------------------------------
SUPPORT / QUESTIONS
--------------------------------------------------------------------------------
See D365_Notebook_Documentation.html for detailed explanations,
architecture diagrams, and complete feature list.
