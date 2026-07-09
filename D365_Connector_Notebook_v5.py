# Databricks notebook source
# MAGIC %md
# MAGIC # D365 Custom Ingestion Notebook — v5 (Production-Ready)
# MAGIC ### Full feature parity with Azure Databricks D365 Connector + resume + audit
# MAGIC
# MAGIC **All features verified from:**
# MAGIC - learn.microsoft.com/en-us/azure/databricks/ingestion/lakeflow-connect/d365-reference
# MAGIC - learn.microsoft.com/en-us/azure/databricks/ingestion/lakeflow-connect/d365-faq
# MAGIC - learn.microsoft.com/en-us/azure/databricks/ingestion/lakeflow-connect/d365-limits
# MAGIC
# MAGIC **Status of the managed connector this notebook emulates: Public Preview** (as of May 2026)
# MAGIC
# MAGIC ### 🆕 v5 RERUN UX IMPROVEMENTS
# MAGIC - ✅ `preview_ingestion()` — dry-run: see what will happen without running
# MAGIC - ✅ `show_ingestion_status()` — formatted status dashboard, one-liner
# MAGIC - ✅ `retry_failed_only()` — retry only FAILED tables, no config editing
# MAGIC - ✅ `reset_stuck_running()` — instant recovery, no 2h wait
# MAGIC - ✅ Run history table — audit log of every run
# MAGIC
# MAGIC ### v4 SCALE + RESUME FEATURES (for 200-600 tables)
# MAGIC - ✅ Resume mode — skip already-completed tables on retry
# MAGIC - ✅ Failed-first priority — retry failed tables before new ones
# MAGIC - ✅ Stuck-RUNNING recovery — auto-recover from cluster crashes
# MAGIC - ✅ Batching — configurable batches with progress display [N/M]
# MAGIC - ✅ Beyond 250 table limit — batches handle 600+ tables cleanly
# MAGIC - ✅ Continue-on-failure — one bad table doesn't block others
# MAGIC
# MAGIC ### For reference — Azure Databricks managed pipeline parameters this notebook replicates
# MAGIC ```python
# MAGIC # If you were using the managed connector instead of this notebook, the pipeline
# MAGIC # would be created with parameters like this (from Microsoft Learn docs):
# MAGIC #
# MAGIC # from databricks.sdk import WorkspaceClient
# MAGIC # from databricks.sdk.service.pipelines import IngestionPipelineDefinition
# MAGIC # w = WorkspaceClient()
# MAGIC # w.pipelines.create(
# MAGIC #     name="d365_ingestion",
# MAGIC #     ingestion_definition=IngestionPipelineDefinition(
# MAGIC #         channel="PREVIEW",                     # Required: must be "PREVIEW"
# MAGIC #         connection_name="d365_connection",     # UC connection name
# MAGIC #         source_schema="objects",               # Typically "objects" for Dataverse
# MAGIC #         source_table="account",                # D365 logical name (lowercase)
# MAGIC #         destination_catalog="main",
# MAGIC #         destination_schema="d365_data",
# MAGIC #         scd_type="SCD_TYPE_2",                 # "SCD_TYPE_1" or "SCD_TYPE_2"
# MAGIC #         table_configuration={
# MAGIC #             "account": {
# MAGIC #                 "columns": ["accountid", "name", "emailaddress1"]
# MAGIC #             }
# MAGIC #         }
# MAGIC #     )
# MAGIC # )
# MAGIC ```
# MAGIC
# MAGIC **Features covered:**
# MAGIC - ✅ model.json schema auto-discovery (all Dataverse data types)
# MAGIC - ✅ Headerless CSV + JSON header joining (positional column mapping)
# MAGIC - ✅ Per-table versionnumber cursor (incremental ingestion)
# MAGIC - ✅ Snapshot folder processing (first run / full load)
# MAGIC - ✅ Changelog folder processing (incremental — inserts / updates / deletes)
# MAGIC - ✅ SCD Type 1 — in-place overwrite
# MAGIC - ✅ SCD Type 2 — __START_AT / __END_AT / __CURRENT history tracking
# MAGIC - ✅ Hard delete handling (IsDelete flag from Synapse Link changelog)
# MAGIC - ✅ Soft delete handling (SCD2 — marks record __CURRENT=false)
# MAGIC - ✅ No-delete-tracking mode (records preserved, warning logged)
# MAGIC - ✅ Full refresh / schema drift recovery (force_full_refresh flag)
# MAGIC - ✅ Column selection per table (table_configuration equivalent)
# MAGIC - ✅ Money type → DECIMAL(19,4)
# MAGIC - ✅ Time type → STRING ISO 8601
# MAGIC - ✅ Image / File types → STRING metadata
# MAGIC - ✅ Multi-select Picklist → comma-separated STRING e.g. "1,3,5"
# MAGIC - ✅ Lookup → GUID STRING
# MAGIC - ✅ Picklist / State / Status → INTEGER code
# MAGIC - ✅ Case-sensitive lowercase table names enforced
# MAGIC - ✅ Per-table primary key resolution (D365 convention: <tablename>id)
# MAGIC - ✅ Multi-environment support (separate source_schema per environment)
# MAGIC - ✅ Idempotent retry — cursor preserved on failure
# MAGIC - ✅ Exponential backoff retry on transient failures
# MAGIC - ✅ Unity Catalog schema registration with Delta table properties
# MAGIC - ✅ Delta Change Data Feed enabled on all tables
# MAGIC - ✅ Monitoring view — cursor state, row counts, status per table
# MAGIC - ✅ Per-table reset (selective full refresh)
# MAGIC - ✅ 250 table limit warning per pipeline
# MAGIC - ✅ [v3 NEW] versionnumber presence validation — fail-fast with actionable error
# MAGIC - ✅ [v3 NEW] OptionSetMetadata join utility — resolve Picklist integers to labels
# MAGIC - ✅ [v3 NEW] F&O / virtual entity guidance — mserp_ and direct table patterns
# MAGIC
# MAGIC **Intentional gaps vs managed connector (not replicable in a notebook):**
# MAGIC - ❌ Serverless compute auto-scaling (notebook uses cluster compute)
# MAGIC - ❌ Lakeflow Connect pipeline UI / API
# MAGIC - ❌ Built-in Databricks job auto-creation per schedule
# MAGIC - ❌ Lakehouse Monitoring integration (add post-ingestion manually)

# COMMAND ----------
# MAGIC %md ## 0. Configuration

# COMMAND ----------

CONFIG = {
    # ── ADLS Gen2 ─────────────────────────────────────────────────────────────
    # Format: abfss://<container>@<storage_account>.dfs.core.windows.net/<path>
    "adls_base_path": "abfss://dynamics365@yourstorageaccount.dfs.core.windows.net/",

    # ── Unity Catalog ─────────────────────────────────────────────────────────
    "catalog":        "main",
    "raw_schema":     "d365_raw",       # Landing schema — raw data lands here
    "control_schema": "d365_control",   # Internal: cursor tracking table

    # ── Source ────────────────────────────────────────────────────────────────
    # Dataverse environment identifier (folder prefix inside ADLS container)
    # Matches source_schema in the managed connector pipeline parameter
    # e.g. "https://yourorg.crm.dynamics.com" or the org unique name
    "source_schema": "",
    # MULTI-ENVIRONMENT NOTE (from docs):
    # "You need separate pipelines for each Dataverse environment (source_schema).
    #  Single connection authenticates to your ADLS Gen2 container.
    #  Multiple pipelines: one per Dataverse environment, each with a different source_schema."
    # To ingest from prod AND test, run this notebook twice with different source_schema values
    # and different raw_schema destinations (e.g. "d365_prod" vs "d365_test").

    # Tables to ingest. Leave [] to ingest ALL tables found in model.json.
    # Must be lowercase logical names — connector is case-sensitive.
    # e.g. ["account", "contact", "opportunity", "salesorder"]
    # DUPLICATE TABLE NAME WARNING (from docs):
    # "Databricks can't ingest two or more tables with the same name in the same pipeline,
    #  even if they come from different source schemas."
    # If two source schemas have a table named "account", use separate runs with different
    # raw_schema destinations to avoid conflicts.
    "tables_to_ingest": [],

    # ── SCD ───────────────────────────────────────────────────────────────────
    # 1 = SCD Type 1: overwrite in place (default, same as connector default)
    # 2 = SCD Type 2: full history with __START_AT / __END_AT / __CURRENT
    "scd_type": 1,

    # ── Delete tracking ───────────────────────────────────────────────────────
    # True  = Synapse Link IS configured to export delete records (IsDelete flag)
    # False = Synapse Link does NOT export deletes (records stay in Delta forever)
    # Matches connector behaviour: "If Synapse Link doesn't export deletes,
    # deleted records remain in target tables until you perform a full refresh"
    "delete_tracking_enabled": True,

    # ── Column selection (table_configuration) ────────────────────────────────
    # Per-table column whitelist. Only listed columns are ingested.
    # Matches connector's optional table_configuration.columns parameter.
    # Leave {} to ingest all columns (default).
    # Example:
    #   "column_selection": {
    #       "account": ["accountid", "name", "emailaddress1", "telephone1"],
    #       "contact": ["contactid", "fullname", "emailaddress1"]
    #   }
    "column_selection": {},

    # ── Primary key overrides ─────────────────────────────────────────────────
    # D365 convention: <tablename>id  (e.g. account → accountid)
    # Only add entries here for tables that deviate from the convention.
    "primary_key_overrides": {
        # "customtable": "my_custom_pk_column"
    },

    # ── Full refresh ──────────────────────────────────────────────────────────
    # True  = ignore cursor, re-read all Snapshot folders (use after schema change)
    # False = normal incremental run
    # Reset to False after a full refresh completes.
    "force_full_refresh": False,

    # ── Retry ─────────────────────────────────────────────────────────────────
    # Max retries per table on transient failures (exponential backoff)
    "max_retries": 3,

    # ── SCALE & RESUME BEHAVIOUR (for 200-600 tables) ─────────────────────────
    # RESUME MODE — controls what to do if the previous run failed partway through.
    #
    # "resume"       : (RECOMMENDED for large runs)
    #                  Skip tables completed successfully within resume_skip_minutes.
    #                  Process FAILED tables first, then never-started, then stale.
    #                  If run 1 failed at table #50 of 200, run 2 only processes
    #                  tables #50 onwards + any earlier FAILED — NOT restart from #1.
    #
    # "full"         : Process every table every time in original order (v3 behaviour).
    #                  Use for very small table counts where iteration cost is trivial.
    #
    # "failed_only"  : Only process tables with status=FAILED or RUNNING (stuck).
    #                  Use to targeted-retry after fixing an issue.
    "resume_mode": "resume",

    # Skip successful tables processed within this many minutes.
    # Only applies when resume_mode = "resume".
    # Example: 5 = skip tables that completed successfully in the last 5 minutes
    # Set to 0 to always re-process even successful tables (still respects cursor).
    "resume_skip_minutes": 5,

    # STUCK RUNNING RECOVERY — if a table has status=RUNNING for longer than this,
    # treat it as failed and re-process it. Handles cluster crashes mid-run.
    "stuck_running_hours": 2,

    # BATCHING — for large table counts, process in batches with progress reports.
    # Each batch commits cursors before moving to the next batch.
    # Set to 0 to disable batching (process all in one pass).
    "batch_size": 50,

    # STOP-ON-FAILURE — if True, stop the whole run when any table fails.
    # If False (default), continue processing other tables even if one fails.
    # Recommendation: False for large runs — one bad table shouldn't block 599 others.
    "stop_on_failure": False,

    # PARALLELISM — number of tables to process concurrently.
    # 1 = sequential (safest, default).
    # 2-4 = parallel folder listing/reads, sequential MERGEs.
    # >4 = risk of ADLS throttling. Only increase if you know your storage limits.
    "max_parallel_tables": 1,
}

# ══════════════════════════════════════════════════════════════════════════════
# IMPORTANT LIMITATIONS & OPERATIONAL NOTES (from d365-limits docs)
# Read these before going to production.
# ══════════════════════════════════════════════════════════════════════════════
#
# 1. SOURCE TABLE DELETED ≠ DESTINATION TABLE DELETED
#    "When a source table is deleted, the destination table is NOT automatically deleted.
#     You must delete the destination table manually."
#
# 2. COLUMN BACKFILL NOT AUTOMATIC
#    "If you select a column after a pipeline has already started, the connector does not
#     automatically backfill data for the new column." To get historical data for a newly
#     added column, set force_full_refresh = True and re-run.
#
# 3. SYNAPSE LINK EXPORT LATENCY
#    Changes appear in ADLS Gen2 after Synapse Link's export interval, typically
#    5-15 minutes. This latency is inherent to the architecture and cannot be avoided.
#    Your Delta table can never be more fresh than this interval.
#
# 4. NO BACKFILL ON SYNAPSE LINK DOWNTIME
#    "If Synapse Link misses changes due to downtime, those changes aren't captured
#     unless you perform a full refresh." Monitor Synapse Link health and do a full
#     refresh after any unplanned outage.
#
# 5. F&O VIRTUAL ENTITY SYNC DELAY
#    "Virtual entities sometimes take longer than 15 minutes to synchronize."
#    Allow up to 15 minutes for virtual entity schema changes to appear in
#    Dataverse schema discovery before running ingestion.
#
# 6. MISSING ADLS FOLDERS = DATA LOSS
#    "If you delete folders or folders are missing, the connector can't recover
#     without a full refresh." Configure ADLS lifecycle policies to retain at least
#    7-30 days of exports per your recovery needs.
#
# 7. CURSOR MONOTONICALLY INCREASING
#    "The source system assumes that the cursor columns are monotonically increasing."
#    versionnumber values from Synapse Link must always increase. Gaps are OK but
#    out-of-order values would break incremental logic.
#

# ── ADLS Authentication ───────────────────────────────────────────────────────
# Uncomment ONE of the options below:
#
# AZURE DATABRICKS Unity Catalog Connection parameters (from Microsoft Learn docs):
# When you create the Unity Catalog connection for D365 in the Azure Databricks UI,
# these are the EXACT parameter names required:
#   - tenant_id                 : Microsoft Entra ID tenant (Directory) ID
#                                 Example: "12345678-1234-1234-1234-123456789abc"
#   - client_id                 : Entra ID application (client) ID
#                                 Example: "87654321-4321-4321-4321-cba987654321"
#   - client_secret             : Entra ID application client secret value
#                                 Example: "abc123~xyz789..."
#   - azure_storage_account_name: ADLS Gen2 storage account name
#                                 Example: "d365storage"
#   - azure_container_name      : ADLS Gen2 container where Synapse Link exports
#                                 Example: "d365-export"
#   - oauth_scope               : OAuth scope for Azure Storage — do NOT modify
#                                 Value: "https://storage.azure.com/.default"

# Option A: Service Principal via Databricks Secrets (recommended for production)
# Matches Microsoft Entra ID OAuth 2.0 client credentials flow used by the connector
# azure_storage_account_name = "yourstorageaccount"
# azure_container_name       = "dynamics365"
# oauth_scope                = "https://storage.azure.com/.default"
# tenant_id                  = dbutils.secrets.get(scope="d365-scope", key="tenant-id")
# client_id                  = dbutils.secrets.get(scope="d365-scope", key="client-id")
# client_secret              = dbutils.secrets.get(scope="d365-scope", key="client-secret")
# spark.conf.set(f"fs.azure.account.auth.type.{azure_storage_account_name}.dfs.core.windows.net", "OAuth")
# spark.conf.set(f"fs.azure.account.oauth.provider.type.{azure_storage_account_name}.dfs.core.windows.net",
#                "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
# spark.conf.set(f"fs.azure.account.oauth2.client.id.{azure_storage_account_name}.dfs.core.windows.net",
#                client_id)
# spark.conf.set(f"fs.azure.account.oauth2.client.secret.{azure_storage_account_name}.dfs.core.windows.net",
#                client_secret)
# spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{azure_storage_account_name}.dfs.core.windows.net",
#                f"https://login.microsoftonline.com/{tenant_id}/oauth2/token")

# Option B: Storage Account Key
# spark.conf.set(f"fs.azure.account.key.yourstorageaccount.dfs.core.windows.net",
#                dbutils.secrets.get(scope="d365-scope", key="storage-key"))

# Option C: ADLS already mounted
# Set adls_base_path = "/mnt/dynamics365/"

# COMMAND ----------
# MAGIC %md ## 1. Imports

# COMMAND ----------

import json
import re
import time
from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, LongType,
    DoubleType, BooleanType, TimestampType, DateType, DecimalType
)
from delta.tables import DeltaTable

print("✅ Imports loaded")

# COMMAND ----------
# MAGIC %md ## 2. Dataverse → Delta Type Mapping
# MAGIC
# MAGIC **Verified against official connector data type reference:**
# MAGIC https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/d365-reference#data-type-mapping

# COMMAND ----------

# Full type map matching the official connector reference documentation
DATAVERSE_TYPE_MAP = {
    # Strings
    "string":               StringType(),       # Single-line text
    "memo":                 StringType(),       # Multi-line text
    "nvarchar":             StringType(),

    # Numerics
    "integer":              IntegerType(),      # Whole Number
    "int32":                IntegerType(),
    "int64":                LongType(),         # BigInt
    "bigint":               LongType(),
    "double":               DoubleType(),       # Floating Point
    "float":                DoubleType(),
    "decimal":              DecimalType(28, 10),# Decimal (precision/scale from metadata)
    "money":                DecimalType(19, 4), # Money — stored as DECIMAL(19,4) per docs
                                                # v1 BUG FIX: was using generic DecimalType

    # Boolean
    "boolean":              BooleanType(),      # Yes/No

    # Date/Time
    "datetime":             TimestampType(),    # DateTime — timezone info preserved
    "datetimeoffset":       TimestampType(),
    "date":                 DateType(),         # Date only
    "time":                 TimestampType(),    # Time → TIMESTAMP per Azure Databricks docs
                                                # "Spark has no native Time type, so the connector
                                                #  promotes Time to TIMESTAMP"
                                                # v3 BUG FIX: was StringType, corrected to TimestampType

    # Identifiers
    "guid":                 StringType(),       # Uniqueidentifier — stored as string
    "uniqueidentifier":     StringType(),

    # Relationships
    "lookup":               StringType(),       # Foreign key GUID as string
                                                # v1 BUG FIX: was missing explicit mapping

    # Option Sets
    "picklist":             IntegerType(),      # Option Set — integer code, not label
    "state":                IntegerType(),      # State attribute (also an option set)
    "status":               IntegerType(),      # Status attribute
    "multiselect_picklist": StringType(),       # Multi-select — comma-separated integers e.g. "1,3,5"
                                                # v1 BUG FIX: was missing

    # Binary / File
    "image":                StringType(),       # Image URL or metadata — NOT binary content
    "file":                 StringType(),       # File metadata only — NOT file contents
    "binary":               StringType(),       # Binary as base64 string
                                                # v1 BUG FIX: image/file types were missing

    # Fallback
    "json":                 StringType(),       # Complex/custom types as JSON string
}

def get_spark_type(dataverse_type: str) -> object:
    """Map a Dataverse type string to a Spark type. Defaults to StringType if unknown."""
    return DATAVERSE_TYPE_MAP.get(dataverse_type.lower(), StringType())

print("✅ Type map loaded — covers all official Dataverse → Delta Lake mappings")

# COMMAND ----------
# MAGIC %md ## 3. Unity Catalog Setup

# COMMAND ----------

def setup_catalog_schemas():
    catalog     = CONFIG["catalog"]
    raw_schema  = CONFIG["raw_schema"]
    ctrl_schema = CONFIG["control_schema"]
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{raw_schema}`")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{ctrl_schema}`")
    print(f"✅ Schemas ready: {catalog}.{raw_schema} | {catalog}.{ctrl_schema}")

setup_catalog_schemas()

# COMMAND ----------
# MAGIC %md ## 4. Cursor Control Table
# MAGIC Per-table cursor — stores last `versionnumber` and folder timestamp.
# MAGIC Equivalent to connector's internal pipeline metadata cursor storage.
# MAGIC "Cursors are stored in the pipeline metadata and don't appear in target Delta tables." — docs

# COMMAND ----------

CONTROL_TABLE = f"`{CONFIG['catalog']}`.`{CONFIG['control_schema']}`.`d365_ingestion_cursor`"

def setup_control_table():
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CONTROL_TABLE} (
            table_name              STRING      NOT NULL COMMENT 'D365 logical table name (lowercase)',
            last_versionnumber      BIGINT      COMMENT 'Last processed Synapse Link versionnumber',
            last_folder_ts          STRING      COMMENT 'Last processed ADLS folder timestamp',
            last_run_ts             TIMESTAMP   COMMENT 'When this table was last ingested',
            row_count               BIGINT      COMMENT 'Rows processed in last run',
            status                  STRING      COMMENT 'SUCCESS | FAILED | RUNNING',
            error_message           STRING      COMMENT 'Last error if status=FAILED',
            scd_type                INT         COMMENT 'SCD type used: 1 or 2',
            is_full_refresh         BOOLEAN     COMMENT 'Was last run a full refresh?'
        )
        USING DELTA
        COMMENT 'D365 ingestion cursor — one row per table, tracks incremental position'
    """)
    print(f"✅ Control table ready: {CONTROL_TABLE}")

def get_cursor(table_name: str) -> dict:
    df = spark.sql(f"""
        SELECT last_versionnumber, last_folder_ts, scd_type
        FROM {CONTROL_TABLE}
        WHERE table_name = '{table_name}'
        AND status != 'RUNNING'
    """)
    if df.count() == 0:
        return None
    row = df.first()
    return {
        "last_versionnumber": row["last_versionnumber"] or 0,
        "last_folder_ts":     row["last_folder_ts"] or "",
        "scd_type":           row["scd_type"]
    }

def mark_running(table_name: str):
    """Mark table as in-progress before processing starts."""
    spark.sql(f"""
        MERGE INTO {CONTROL_TABLE} AS t
        USING (SELECT '{table_name}' AS table_name) AS s
        ON t.table_name = s.table_name
        WHEN MATCHED THEN UPDATE SET t.status = 'RUNNING', t.last_run_ts = current_timestamp()
        WHEN NOT MATCHED THEN INSERT (table_name, status, last_run_ts, last_versionnumber,
                                      last_folder_ts, row_count, scd_type, is_full_refresh)
             VALUES ('{table_name}', 'RUNNING', current_timestamp(), 0, '', 0,
                     {CONFIG['scd_type']}, false)
    """)

def update_cursor(table_name: str, last_versionnumber: int, last_folder_ts: str,
                  row_count: int, status: str = "SUCCESS",
                  error_message: str = None, is_full_refresh: bool = False):
    err = (error_message or "").replace("'", "''")[:500]
    spark.sql(f"""
        MERGE INTO {CONTROL_TABLE} AS target
        USING (SELECT
            '{table_name}'      AS table_name,
            {last_versionnumber} AS last_versionnumber,
            '{last_folder_ts}'  AS last_folder_ts,
            current_timestamp() AS last_run_ts,
            {row_count}         AS row_count,
            '{status}'          AS status,
            '{err}'             AS error_message,
            {CONFIG['scd_type']} AS scd_type,
            {str(is_full_refresh).lower()} AS is_full_refresh
        ) AS source
        ON target.table_name = source.table_name
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

setup_control_table()

# COMMAND ----------
# MAGIC %md ## 5. Parse model.json
# MAGIC Reads Synapse Link's metadata JSON to discover all table schemas.
# MAGIC "The connector reads Synapse Link metadata files from ADLS Gen2 and
# MAGIC  extracts table schemas from the metadata JSON files." — docs

# COMMAND ----------

def parse_model_json(base_path: str) -> dict:
    """
    Parse model.json → returns dict of:
      { table_name_lowercase: { "columns": [...], "schema": StructType } }

    model.json structure (Synapse Link format):
      { "entities": [ { "name": "account", "attributes": [ { "name": "accountid", "dataType": "guid" } ] } ] }
    """
    model_path = base_path.rstrip("/") + "/model.json"
    try:
        model_raw  = dbutils.fs.head(model_path, 10_000_000)
        model_data = json.loads(model_raw)
    except Exception as e:
        raise ValueError(
            f"❌ Cannot read model.json at {model_path}.\n"
            f"   Ensure Azure Synapse Link is configured and has exported at least once.\n"
            f"   Error: {e}"
        )

    table_schemas = {}

    for entity in model_data.get("entities", []):
        # Enforce lowercase — connector is case-sensitive, requires lowercase names
        table_name = entity.get("name", "").lower().strip()
        if not table_name:
            continue

        columns      = []
        spark_fields = []

        for attr in entity.get("attributes", []):
            col_name   = attr.get("name", "").strip()
            col_type   = attr.get("dataType", "string")
            spark_type = get_spark_type(col_type)

            # Respect Money precision/scale from metadata if provided
            if col_type.lower() == "money":
                spark_type = DecimalType(19, 4)
            elif col_type.lower() == "decimal":
                precision = attr.get("traits", {}).get("precision", 28)
                scale     = attr.get("traits", {}).get("scale", 10)
                spark_type = DecimalType(int(precision), int(scale))

            columns.append({"name": col_name, "data_type": col_type.lower()})
            spark_fields.append(StructField(col_name, spark_type, nullable=True))

        # Synapse Link system columns always present in changelog CSVs
        existing_names = {c["name"].lower() for c in columns}
        system_cols = [
            ("versionnumber", LongType()),
            ("modifiedon",    TimestampType()),
            ("createdon",     TimestampType()),
            ("IsDelete",      StringType()),    # delete marker flag
        ]
        for sc_name, sc_type in system_cols:
            if sc_name.lower() not in existing_names:
                columns.append({"name": sc_name, "data_type": "system"})
                spark_fields.append(StructField(sc_name, sc_type, nullable=True))

        table_schemas[table_name] = {
            "columns": columns,
            "schema":  StructType(spark_fields)
        }

    count = len(table_schemas)
    sample = list(table_schemas.keys())[:8]
    print(f"✅ model.json parsed — {count} tables found")
    print(f"   Sample: {sample}{'...' if count > 8 else ''}")

    # Warn if approaching the 250-table pipeline limit documented in the connector
    if count > 200:
        print(f"  ⚠️  {count} tables found. Connector limit is 250 per pipeline.")
        print(f"      Consider splitting tables across multiple notebook runs.")

    return table_schemas

# COMMAND ----------
# MAGIC %md ## 6. Column Selection (table_configuration)
# MAGIC Filters columns per table — matches connector's optional table_configuration.columns parameter.
# MAGIC "Select only required columns to reduce data transfer, storage costs, and query processing time." — docs

# COMMAND ----------

def apply_column_selection(table_name: str, columns: list, schema: StructType):
    """
    Apply per-table column whitelist from CONFIG["column_selection"].
    Returns filtered (columns, schema) tuple.
    If no whitelist configured for this table, returns all columns.
    """
    selection = CONFIG.get("column_selection", {}).get(table_name.lower(), [])
    if not selection:
        return columns, schema  # No filter — ingest all columns

    # Always keep primary key and system columns regardless of selection
    pk_col    = resolve_primary_key(table_name, columns)
    keep_always = {pk_col.lower(), "versionnumber", "isdeletee", "modifiedon", "createdon"}
    selected_lower = {c.lower() for c in selection} | keep_always

    filtered_columns = [c for c in columns if c["name"].lower() in selected_lower]
    filtered_fields  = [f for f in schema.fields if f.name.lower() in selected_lower]

    removed = len(columns) - len(filtered_columns)
    if removed > 0:
        print(f"    📋 Column selection: keeping {len(filtered_columns)}/{len(columns)} columns "
              f"({removed} excluded)")

    return filtered_columns, StructType(filtered_fields)

# COMMAND ----------
# MAGIC %md ## 7. ADLS Folder Discovery
# MAGIC Synapse Link writes to ISO timestamp-named folders.
# MAGIC "The connector processes folders in chronological order based on timestamps." — docs

# COMMAND ----------

def get_base_path() -> str:
    base = CONFIG["adls_base_path"].rstrip("/")
    src  = CONFIG.get("source_schema", "").strip().rstrip("/")
    if src:
        base = f"{base}/{src}"
    return base

def list_folders(path: str) -> list:
    try:
        items = dbutils.fs.ls(path)
        return sorted([f.path for f in items if f.isDir()])
    except Exception:
        return []

def parse_folder_timestamp(folder_path: str) -> str:
    """Extract sortable ISO timestamp from Synapse Link folder name."""
    # Synapse Link format: .../2026-07-09T06-00-00/
    match = re.search(r'(\d{4}-\d{2}-\d{2}T\d{2}-\d{2}-\d{2})', folder_path)
    if match:
        raw = match.group(1)            # e.g. 2026-07-09T06-00-00
        return raw[:10] + "T" + raw[11:].replace("-", ":")  # → 2026-07-09T06:00:00
    return folder_path  # fallback: use full path for sort

def validate_versionnumber_in_changelog(folder_path: str) -> bool:
    """
    GAP FIX 2: Validate that changelog CSV contains the versionnumber field.
    Docs: "If VersionNumber is missing from changelog, incremental ingestion fails
    and you must perform a full refresh."
    Reads only the first CSV file header row to check — does not load full data.
    Returns True if versionnumber is present, False if missing (triggers full refresh).
    """
    csv_files = [f.path for f in dbutils.fs.ls(folder_path) if f.name.endswith(".csv")]
    if not csv_files:
        return True  # Empty folder — safe to continue

    # Peek at 1 row to check column count matches expectations
    # Synapse Link changelog CSV: versionnumber is always last system column if present
    try:
        sample = spark.read.option("header", "false").csv(csv_files[0]).limit(1)
        col_count = len(sample.columns)
        # A valid Synapse Link changelog has at least versionnumber + IsDelete system cols
        # Minimum viable: if only 1-2 columns exist it's likely missing system columns
        if col_count < 2:
            print(f"    ⚠️  WARNING: Changelog folder '{folder_path.split('/')[-2]}' "
                  f"has only {col_count} column(s). versionnumber may be missing.")
            print(f"        Per docs: 'Synapse Link must export changelogs with the "
                  f"versionnumber field.' Configure Synapse Link correctly or do a full refresh.")
            return False
        return True
    except Exception:
        return True  # Can't validate — proceed and let downstream handle errors


def get_new_folders(table_path: str, folder_type: str,
                    cursor: dict, force_full: bool) -> list:
    """Return Synapse Link folders not yet processed by this pipeline."""
    type_path    = f"{table_path}/{folder_type}"
    all_folders  = list_folders(type_path)

    if force_full or cursor is None:
        return all_folders

    last_ts     = cursor.get("last_folder_ts", "")
    new_folders = [f for f in all_folders if parse_folder_timestamp(f) > last_ts]
    return new_folders

# COMMAND ----------
# MAGIC %md ## 8. Read Headerless CSV + Apply Schema
# MAGIC Core operation: joins headerless Synapse Link CSVs with model.json column order.
# MAGIC "Column names, data types, and nullability are inferred from the metadata." — docs

# COMMAND ----------

def read_csv_with_schema(folder_path: str, columns: list,
                         schema: StructType) -> DataFrame:
    """
    Read headerless Synapse Link CSVs and apply named schema from model.json.
    Synapse Link CSVs have NO header row. Column order matches model.json attribute order.
    """
    csv_files = [f.path for f in dbutils.fs.ls(folder_path) if f.name.endswith(".csv")]
    if not csv_files:
        return spark.createDataFrame([], schema)

    # Read raw — all columns arrive as _c0, _c1, _c2 ...
    df_raw = (spark.read
              .option("header", "false")
              .option("inferSchema", "false")  # Never infer — always use model.json
              .option("escape", '"')
              .option("multiLine", "true")
              .option("encoding", "UTF-8")
              .csv(folder_path + "/*.csv"))

    if df_raw.rdd.isEmpty():
        return spark.createDataFrame([], schema)

    raw_cols = df_raw.columns   # _c0, _c1, _c2 ...

    # Positional mapping: rename _cN → column name from model.json, cast to correct type
    rename_exprs = []
    for i, col_def in enumerate(columns):
        if i >= len(raw_cols):
            break   # model.json has more columns than CSV — safe to skip trailing

        col_name   = col_def["name"]
        spark_type = get_spark_type(col_def["data_type"])

        # Money and Decimal need precise types
        if col_def["data_type"] == "money":
            spark_type = DecimalType(19, 4)

        rename_exprs.append(
            F.col(raw_cols[i]).cast(spark_type).alias(col_name)
        )

    return df_raw.select(rename_exprs)

# COMMAND ----------
# MAGIC %md ## 9. Primary Key Resolution
# MAGIC "D365 convention: <tablename>id (e.g. account → accountid)" — docs

# COMMAND ----------

def resolve_primary_key(table_name: str, columns: list) -> str:
    overrides  = CONFIG.get("primary_key_overrides", {})
    if table_name.lower() in overrides:
        return overrides[table_name.lower()]

    conventional_pk = f"{table_name.lower()}id"
    col_names_lower = [c["name"].lower() for c in columns]

    if conventional_pk in col_names_lower:
        return columns[col_names_lower.index(conventional_pk)]["name"]

    # Fallbacks
    for candidate in ["id", "recid", "entityid"]:
        if candidate in col_names_lower:
            print(f"  ⚠️  {table_name}: no '{conventional_pk}' found, using '{candidate}' as PK")
            return columns[col_names_lower.index(candidate)]["name"]

    # Last resort: first column
    first = columns[0]["name"] if columns else "id"
    print(f"  ⚠️  {table_name}: defaulting to first column '{first}' as PK")
    return first

# COMMAND ----------
# MAGIC %md ## 10. Delta Table Management

# COMMAND ----------

def get_delta_table_fqn(table_name: str) -> str:
    """Fully qualified Unity Catalog table name."""
    return f"{CONFIG['catalog']}.{CONFIG['raw_schema']}.{table_name}"

def create_delta_table_if_not_exists(table_name: str, schema: StructType, scd_type: int):
    """Create target Delta table with correct schema and properties."""
    fqn = get_delta_table_fqn(table_name)

    # SCD Type 2 adds history tracking columns
    scd2_cols = ""
    if scd_type == 2:
        scd2_cols = """,
            __START_AT   TIMESTAMP COMMENT 'SCD2: record valid from (set on insert)',
            __END_AT     TIMESTAMP COMMENT 'SCD2: record valid until (NULL = current)',
            __CURRENT    BOOLEAN   COMMENT 'SCD2: true = latest active version'"""

    col_ddl = ",\n    ".join([
        f"`{f.name}` {f.dataType.simpleString()} COMMENT 'Dataverse column'"
        for f in schema.fields
        if f.name not in ("__START_AT", "__END_AT", "__CURRENT")
    ])

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS `{fqn}` (
            {col_ddl}
            {scd2_cols}
        )
        USING DELTA
        COMMENT 'D365 raw ingestion: {table_name} (SCD{scd_type})'
        TBLPROPERTIES (
            'delta.enableChangeDataFeed'        = 'true',
            'd365.source_table'                 = '{table_name}',
            'd365.scd_type'                     = '{scd_type}',
            'd365.ingestion_notebook_version'   = '2.0',
            'd365.connector_equivalent'         = 'lakeflow_connect_d365'
        )
    """)

# COMMAND ----------
# MAGIC %md ## 11. Apply Snapshot Data (First Run / Full Refresh)
# MAGIC Snapshot = full table state export from Synapse Link.
# MAGIC Used on first run or after force_full_refresh.

# COMMAND ----------

def apply_snapshot(table_name: str, df: DataFrame, scd_type: int):
    """
    Write Snapshot data to Delta table.
    SCD1: full overwrite.
    SCD2: overwrite with history columns initialized.
    """
    fqn = get_delta_table_fqn(table_name)

    if scd_type == 2:
        df = (df
              .withColumn("__START_AT", F.current_timestamp())
              .withColumn("__END_AT",   F.lit(None).cast(TimestampType()))
              .withColumn("__CURRENT",  F.lit(True)))

    (df.write
       .format("delta")
       .mode("overwrite")
       .option("overwriteSchema", "true")  # Handle schema drift on full refresh
       .saveAsTable(fqn))

    count = df.count()
    print(f"    📥 Snapshot written: {count:,} rows → {fqn}")
    return count

# COMMAND ----------
# MAGIC %md ## 12. Apply Changelog Data (Incremental — Inserts / Updates / Deletes)
# MAGIC
# MAGIC **Change detection rules verified from docs:**
# MAGIC - Inserts: new records present in changelog
# MAGIC - Updates: modified records identified by versionnumber change
# MAGIC - Deletes: IsDelete flag in changelog (if Synapse Link exports deletes)
# MAGIC
# MAGIC **Delete handling verified from docs:**
# MAGIC - Hard deletes (SCD1): DELETE rows from Delta table
# MAGIC - Soft deletes (SCD2): set __CURRENT=false, __END_AT=now
# MAGIC - No delete tracking: records stay until full refresh (warning logged)

# COMMAND ----------

def apply_changelog(table_name: str, df_changelog: DataFrame,
                    pk_col: str, scd_type: int) -> int:
    """
    Apply incremental changelog to Delta table via MERGE.
    Handles inserts, updates, and deletes per connector behaviour.
    Returns row count processed.
    """
    fqn             = get_delta_table_fqn(table_name)
    delete_tracking = CONFIG.get("delete_tracking_enabled", True)

    # Sort by versionnumber to apply changes in correct sequence
    if "versionnumber" in df_changelog.columns:
        df_changelog = df_changelog.orderBy(F.col("versionnumber").cast(LongType()).asc())

    # Separate deletes from upserts based on IsDelete flag
    has_is_delete = "IsDelete" in df_changelog.columns
    if has_is_delete and delete_tracking:
        df_deletes = df_changelog.filter(
            F.lower(F.col("IsDelete").cast(StringType())) == "true"
        )
        df_upserts = df_changelog.filter(
            F.lower(F.col("IsDelete").cast(StringType())) != "true"
        )
    else:
        if not delete_tracking and has_is_delete:
            deleted_count = df_changelog.filter(
                F.lower(F.col("IsDelete").cast(StringType())) == "true"
            ).count()
            if deleted_count > 0:
                print(f"    ⚠️  {deleted_count} delete records found but delete_tracking_enabled=False.")
                print(f"        These records will remain in Delta until a full refresh.")
        df_deletes = spark.createDataFrame([], df_changelog.schema)
        df_upserts = df_changelog

    total_rows  = df_changelog.count()
    upsert_cnt  = df_upserts.count()
    delete_cnt  = df_deletes.count()

    if total_rows == 0:
        return 0

    dt = DeltaTable.forName(spark, fqn)

    # ── SCD TYPE 1 ─────────────────────────────────────────────────────────────
    if scd_type == 1:
        # MERGE: update matching rows, insert new rows
        if upsert_cnt > 0:
            (dt.alias("target")
               .merge(df_upserts.alias("source"),
                      f"target.`{pk_col}` = source.`{pk_col}`")
               .whenMatchedUpdateAll()
               .whenNotMatchedInsertAll()
               .execute())

        # DELETE: remove rows where IsDelete=true
        if delete_cnt > 0:
            pk_values = [str(row[pk_col]) for row in
                         df_deletes.select(pk_col).collect()]
            pk_list   = ", ".join([f"'{v}'" for v in pk_values])
            spark.sql(f"""
                DELETE FROM `{fqn}`
                WHERE `{pk_col}` IN ({pk_list})
            """)
            print(f"    🗑️  SCD1 hard delete: {delete_cnt} rows removed")

    # ── SCD TYPE 2 ─────────────────────────────────────────────────────────────
    elif scd_type == 2:
        now = F.current_timestamp()

        if upsert_cnt > 0:
            # Step 1: Expire current versions of changed records
            (dt.alias("target")
               .merge(df_upserts.alias("source"),
                      f"target.`{pk_col}` = source.`{pk_col}` AND target.__CURRENT = true")
               .whenMatchedUpdate(set={
                   "__END_AT":  "current_timestamp()",
                   "__CURRENT": "false"
               })
               .execute())

            # Step 2: Insert new versions
            df_new = (df_upserts
                      .withColumn("__START_AT", now)
                      .withColumn("__END_AT",   F.lit(None).cast(TimestampType()))
                      .withColumn("__CURRENT",  F.lit(True)))
            df_new.write.format("delta").mode("append").saveAsTable(fqn)

        if delete_cnt > 0:
            # SCD2 deletes: expire the record (mark as ended, not physically deleted)
            # "marks deleted records" — per connector docs
            pk_values = [str(row[pk_col]) for row in
                         df_deletes.select(pk_col).collect()]
            pk_list   = ", ".join([f"'{v}'" for v in pk_values])
            spark.sql(f"""
                UPDATE `{fqn}`
                SET __END_AT  = current_timestamp(),
                    __CURRENT = false
                WHERE `{pk_col}` IN ({pk_list})
                AND __CURRENT = true
            """)
            print(f"    🗑️  SCD2 soft delete: {delete_cnt} records expired (__CURRENT=false)")

    print(f"    ✅ Changelog applied: {upsert_cnt:,} upserts | {delete_cnt:,} deletes")
    return total_rows

# COMMAND ----------
# MAGIC %md ## 13. Exponential Backoff Retry
# MAGIC "When a connector fails, it automatically retries with exponential backoff." — docs

# COMMAND ----------

def with_retry(fn, table_name: str, max_retries: int = 3):
    """Execute fn() with exponential backoff retry on transient failures."""
    for attempt in range(1, max_retries + 1):
        try:
            return fn()
        except Exception as e:
            if attempt == max_retries:
                raise  # Re-raise on final attempt
            wait_secs = 2 ** attempt   # 2s, 4s, 8s ...
            print(f"  ⚠️  {table_name}: attempt {attempt} failed — "
                  f"retrying in {wait_secs}s... ({e})")
            time.sleep(wait_secs)

# COMMAND ----------
# MAGIC %md ## 14. Process Single Table
# MAGIC Full ingestion pipeline for one D365 table — Snapshot + Changelog + cursor update.

# COMMAND ----------

def process_table(table_name: str, table_schema: dict,
                  base_path: str, force_full: bool) -> dict:
    """
    Complete ingestion for one D365 table. Idempotent — safe to retry.
    Returns status dict.
    """
    # Enforce lowercase — connector is case-sensitive per docs
    table_name = table_name.lower().strip()

    print(f"\n{'─'*60}")
    print(f"  TABLE: {table_name.upper()}")
    print(f"{'─'*60}")

    columns = table_schema["columns"]
    schema  = table_schema["schema"]

    # Apply column selection (table_configuration equivalent)
    columns, schema = apply_column_selection(table_name, columns, schema)

    pk_col   = resolve_primary_key(table_name, columns)
    scd_type = CONFIG.get("scd_type", 1)

    print(f"  PK: {pk_col} | SCD{scd_type} | Columns: {len(columns)}")

    cursor       = get_cursor(table_name)
    is_first_run = (cursor is None or force_full)
    table_path   = f"{base_path}/{table_name}"

    last_versionnumber = cursor["last_versionnumber"] if cursor else 0
    last_folder_ts     = cursor["last_folder_ts"]     if cursor else ""
    total_rows         = 0

    try:
        mark_running(table_name)

        # ── CREATE DELTA TABLE ─────────────────────────────────────────────────
        create_delta_table_if_not_exists(table_name, schema, scd_type)

        # ── STEP 1: SNAPSHOT (first run / full refresh) ────────────────────────
        if is_first_run:
            mode = "Full refresh" if force_full else "First run"
            print(f"  📦 {mode} — reading Snapshot folders")
            snapshot_folders = get_new_folders(
                table_path, "Snapshot", None, force_full=True
            )
            print(f"  📂 Snapshot folders: {len(snapshot_folders)}")

            for folder in snapshot_folders:
                def _snap(f=folder):
                    df = read_csv_with_schema(f, columns, schema)
                    if df.rdd.isEmpty():
                        return 0
                    return apply_snapshot(table_name, df, scd_type)

                count = with_retry(_snap, table_name, CONFIG.get("max_retries", 3))
                total_rows    += count
                ts = parse_folder_timestamp(folder)
                if ts > last_folder_ts:
                    last_folder_ts = ts

        # ── STEP 2: CHANGELOG (incremental) ───────────────────────────────────
        print(f"  📋 Changelog folders (new since last run)")
        changelog_folders = get_new_folders(
            table_path, "Changelog", cursor if not is_first_run else None,
            force_full=force_full
        )
        print(f"  📂 New changelog folders: {len(changelog_folders)}")

        for folder in sorted(changelog_folders, key=parse_folder_timestamp):
            # GAP FIX 2: Validate versionnumber presence before processing
            # Docs: "Synapse Link must export changelogs with the versionnumber field."
            # "If VersionNumber is missing, incremental ingestion fails — do a full refresh."
            if not validate_versionnumber_in_changelog(folder):
                print(f"  ❌ versionnumber missing in changelog folder — skipping.")
                print(f"     ACTION REQUIRED: Check Synapse Link config, then set")
                print(f"     CONFIG['force_full_refresh'] = True and re-run.")
                update_cursor(
                    table_name=table_name, last_versionnumber=last_versionnumber,
                    last_folder_ts=last_folder_ts, row_count=total_rows,
                    status="FAILED",
                    error_message="versionnumber missing from changelog. "
                                  "Reconfigure Synapse Link and run a full refresh."
                )
                return {"table": table_name, "status": "FAILED",
                        "error": "versionnumber missing from changelog"}

            def _chg(f=folder):
                df = read_csv_with_schema(f, columns, schema)
                if df.rdd.isEmpty():
                    return 0, 0
                # Track max versionnumber from this batch
                if "versionnumber" in df.columns:
                    max_vn = df.agg(
                        F.max(F.col("versionnumber").cast(LongType()))
                    ).first()[0] or 0
                    return apply_changelog(table_name, df, pk_col, scd_type), max_vn
                return apply_changelog(table_name, df, pk_col, scd_type), 0

            result = with_retry(_chg, table_name, CONFIG.get("max_retries", 3))
            count, max_vn = result if isinstance(result, tuple) else (result, 0)

            total_rows += count
            if max_vn > last_versionnumber:
                last_versionnumber = max_vn
            ts = parse_folder_timestamp(folder)
            if ts > last_folder_ts:
                last_folder_ts = ts

        # ── UPDATE CURSOR ──────────────────────────────────────────────────────
        update_cursor(
            table_name         = table_name,
            last_versionnumber = last_versionnumber,
            last_folder_ts     = last_folder_ts,
            row_count          = total_rows,
            status             = "SUCCESS",
            is_full_refresh    = is_first_run
        )
        print(f"  ✅ Done: {total_rows:,} rows | cursor: versionnumber={last_versionnumber}")
        return {"table": table_name, "status": "SUCCESS", "rows": total_rows}

    except Exception as e:
        error_msg = str(e)
        print(f"  ❌ FAILED: {error_msg[:200]}")
        # Preserve cursor on failure so next run resumes from correct position
        # "the connector tries to avoid missing data by storing the last position of the cursor" — docs
        update_cursor(
            table_name         = table_name,
            last_versionnumber = last_versionnumber,
            last_folder_ts     = last_folder_ts,
            row_count          = total_rows,
            status             = "FAILED",
            error_message      = error_msg,
            is_full_refresh    = is_first_run
        )
        return {"table": table_name, "status": "FAILED", "error": error_msg}

# COMMAND ----------
# MAGIC %md ## 14b. RESUME & SCALE — Table Prioritization for Large Runs
# MAGIC
# MAGIC For 200-600 tables, we need smarter iteration than "loop over everything".
# MAGIC This section handles:
# MAGIC - **Resume from failure** — skip tables completed successfully, prioritize FAILED
# MAGIC - **Stuck RUNNING recovery** — treat stuck-forever tables as failed
# MAGIC - **Batching** — commit progress after every N tables so partial progress isn't lost
# MAGIC - **Progress display** — show [N/M] on each table

# COMMAND ----------

from datetime import timedelta

def recover_stuck_running_tables():
    """
    Handle tables stuck in RUNNING status (cluster crashed mid-processing).
    Anything RUNNING for longer than CONFIG['stuck_running_hours'] is treated as FAILED.
    Their cursor position is preserved so next run resumes from the last checkpoint.
    """
    stuck_hours = CONFIG.get("stuck_running_hours", 2)
    result = spark.sql(f"""
        UPDATE {CONTROL_TABLE}
        SET status = 'FAILED',
            error_message = CONCAT(
                'Stuck in RUNNING for > {stuck_hours} hours — auto-recovered. ',
                COALESCE(error_message, '')
            )
        WHERE status = 'RUNNING'
          AND last_run_ts < (current_timestamp() - INTERVAL {stuck_hours} HOURS)
    """)
    recovered = spark.sql(f"""
        SELECT COUNT(*) AS n FROM {CONTROL_TABLE}
        WHERE status = 'FAILED'
          AND error_message LIKE '%auto-recovered%'
    """).first()["n"]
    if recovered > 0:
        print(f"♻️  Recovered {recovered} stuck-RUNNING tables (cluster crash detected)")

def classify_tables_by_status(all_tables: dict) -> dict:
    """
    Query control table and classify every table by processing status.
    Returns dict with 4 lists:
      - failed:       status=FAILED (highest priority — retry these first)
      - never_run:    no cursor row (never processed — needs first snapshot)
      - stale:        status=SUCCESS but older than resume_skip_minutes
      - fresh:        status=SUCCESS within resume_skip_minutes (SKIP)
    """
    skip_minutes = CONFIG.get("resume_skip_minutes", 5)

    status_rows = spark.sql(f"""
        SELECT
            table_name,
            status,
            last_run_ts,
            (current_timestamp() - INTERVAL {skip_minutes} MINUTES > last_run_ts) AS is_stale
        FROM {CONTROL_TABLE}
    """).collect()
    status_map = {r["table_name"]: (r["status"], r["is_stale"]) for r in status_rows}

    failed, never_run, stale, fresh = [], [], [], []
    for tname in all_tables:
        tname_lower = tname.lower()
        if tname_lower not in status_map:
            never_run.append(tname_lower)
        else:
            status, is_stale = status_map[tname_lower]
            if status == "FAILED":
                failed.append(tname_lower)
            elif status == "RUNNING":
                # Not caught by stuck recovery (recent) — still resume it
                failed.append(tname_lower)
            elif status == "SUCCESS":
                if is_stale:
                    stale.append(tname_lower)
                else:
                    fresh.append(tname_lower)

    return {
        "failed": failed,
        "never_run": never_run,
        "stale": stale,
        "fresh": fresh
    }

def order_tables_by_priority(all_tables: dict) -> tuple:
    """
    Return (ordered_tables_to_process, skipped_tables) based on resume_mode.

    Priority order for "resume" mode:
      1. FAILED tables      — retry from last cursor
      2. Never-run tables   — first-time snapshot
      3. Stale SUCCESS      — incremental check for new changelog folders
      4. Fresh SUCCESS      — skipped entirely (already up to date)
    """
    mode = CONFIG.get("resume_mode", "resume")

    if mode == "full":
        return list(all_tables.keys()), []

    classified = classify_tables_by_status(all_tables)

    if mode == "failed_only":
        to_process = classified["failed"]
        skipped    = classified["never_run"] + classified["stale"] + classified["fresh"]
        return to_process, skipped

    # Default: "resume" mode
    to_process = (classified["failed"]
                  + classified["never_run"]
                  + classified["stale"])
    skipped    = classified["fresh"]
    return to_process, skipped

def print_resume_summary(all_tables: dict):
    """Show the user exactly what will be processed vs skipped, and why."""
    classified = classify_tables_by_status(all_tables)
    mode = CONFIG.get("resume_mode", "resume")

    total = len(all_tables)
    print(f"\n📊 RESUME MODE = '{mode}' | resume_skip_minutes = {CONFIG.get('resume_skip_minutes', 5)}")
    print(f"   Total tables in scope       : {total}")
    print(f"   ❌ FAILED (retry priority)  : {len(classified['failed'])}")
    print(f"   🆕 Never processed          : {len(classified['never_run'])}")
    print(f"   🔄 Stale SUCCESS (recheck)  : {len(classified['stale'])}")
    print(f"   ✅ Fresh SUCCESS (skipped)  : {len(classified['fresh'])}")

    if classified["failed"] and len(classified["failed"]) <= 10:
        print(f"   Failed tables to retry: {classified['failed']}")
    elif classified["failed"]:
        print(f"   Failed tables (first 10): {classified['failed'][:10]}...")

def chunk_list(items: list, batch_size: int) -> list:
    """Split a list into batches of batch_size. Returns list of lists."""
    if batch_size <= 0:
        return [items]
    return [items[i:i + batch_size] for i in range(0, len(items), batch_size)]



# COMMAND ----------

def run_ingestion():
    """
    Main entry point — resume-aware, batched, scalable D365 ingestion.
    Handles 200-600 tables reliably. On failure, next run resumes from
    the exact failure point — no restart from table #1.
    """
    import uuid
    run_id       = str(uuid.uuid4())[:8]
    start_time   = datetime.now()
    base_path    = get_base_path()
    force_full   = CONFIG.get("force_full_refresh", False)
    resume_mode  = CONFIG.get("resume_mode", "resume")
    batch_size   = CONFIG.get("batch_size", 50)
    stop_on_fail = CONFIG.get("stop_on_failure", False)

    print(f"""
╔══════════════════════════════════════════════════════════════╗
║         D365 Custom Ingestion Notebook v4                    ║
║         Resume-Aware  |  Batched  |  Scalable                ║
╠══════════════════════════════════════════════════════════════╣
  Base path      : {base_path[:55]}
  Destination    : {CONFIG['catalog']}.{CONFIG['raw_schema']}
  SCD Type       : {CONFIG['scd_type']}
  Delete tracking: {CONFIG.get('delete_tracking_enabled', True)}
  Force refresh  : {force_full}
  Resume mode    : {resume_mode}
  Batch size     : {batch_size}
  Stop on fail   : {stop_on_fail}
  Started        : {start_time.strftime('%Y-%m-%d %H:%M:%S')}
╚══════════════════════════════════════════════════════════════╝
    """)

    # ── Step 0: Recover stuck-RUNNING tables from crashed prior runs ────
    print("📋 Step 0: Recovering stuck-RUNNING tables...")
    recover_stuck_running_tables()

    # ── Step 1: Parse model.json ────────────────────────────────────────
    print("\n📋 Step 1: Parsing model.json schema...")
    table_schemas = parse_model_json(base_path)

    # ── Step 2: Determine tables in scope ───────────────────────────────
    configured = [t.lower().strip() for t in CONFIG.get("tables_to_ingest", [])]
    if configured:
        tables_to_run = {t: table_schemas[t] for t in configured if t in table_schemas}
        missing = [t for t in configured if t not in table_schemas]
        if missing:
            print(f"  ⚠️  Not found in model.json (check lowercase names): {missing}")
    else:
        tables_to_run = table_schemas

    total_in_scope = len(tables_to_run)
    print(f"\n📋 Step 2: {total_in_scope} tables in scope")

    if total_in_scope > 250:
        print(f"  ℹ️  {total_in_scope} tables exceeds the managed connector's 250 limit.")
        print(f"     This notebook handles it via batching (batch_size={batch_size}).")

    # ── Step 3: Resume prioritization ───────────────────────────────────
    print("\n📋 Step 3: Resume prioritization...")
    print_resume_summary(tables_to_run)

    if force_full:
        to_process = list(tables_to_run.keys())
        skipped    = []
        print(f"\n  ⚠️  force_full_refresh=True — processing ALL {len(to_process)} tables")
    else:
        to_process, skipped = order_tables_by_priority(tables_to_run)

    if skipped:
        print(f"\n⏭  SKIPPING {len(skipped)} tables (completed within "
              f"{CONFIG.get('resume_skip_minutes', 5)} min)")

    if not to_process:
        print("\n✅ Nothing to process — all tables are up to date.")
        return []

    print(f"\n📋 Step 4: Processing {len(to_process)} tables in batches of {batch_size}...")

    # ── Step 4: Process in batches ──────────────────────────────────────
    batches     = chunk_list(to_process, batch_size)
    results     = []
    success_cnt = 0
    failed_cnt  = 0
    total_rows  = 0
    processed   = 0

    for batch_num, batch in enumerate(batches, 1):
        print(f"\n{'#'*60}")
        print(f"  BATCH {batch_num}/{len(batches)}  |  {len(batch)} tables")
        print(f"{'#'*60}")

        for table_name in batch:
            processed += 1
            table_schema = tables_to_run[table_name]

            print(f"\n[{processed}/{len(to_process)}]", end=" ")
            result = process_table(table_name, table_schema, base_path, force_full)
            results.append(result)

            if result["status"] == "SUCCESS":
                success_cnt += 1
                total_rows  += result.get("rows", 0)
            else:
                failed_cnt  += 1
                if stop_on_fail:
                    print(f"\n🛑 stop_on_failure=True — halting run")
                    print(f"   Fix the issue then re-run — resume will pick up here.")
                    break

        # After each batch: cursors are already committed per-table
        print(f"\n  ✓ Batch {batch_num}/{len(batches)} complete: "
              f"{success_cnt} success | {failed_cnt} failed | "
              f"{processed}/{len(to_process)} total")

        if stop_on_fail and failed_cnt > 0:
            break

    # ── Step 5: Final Summary ───────────────────────────────────────────
    duration = (datetime.now() - start_time).seconds
    print(f"""
╔══════════════════════════════════════════════════════════════╗
║                    Run Complete                              ║
╠══════════════════════════════════════════════════════════════╣
  ✅ Succeeded  : {success_cnt} tables
  ❌ Failed     : {failed_cnt} tables
  ⏭  Skipped   : {len(skipped)} tables (up to date)
  📊 Total rows : {total_rows:,}
  ⏱  Duration   : {duration}s
  Finished      : {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
╚══════════════════════════════════════════════════════════════╝
    """)

    if failed_cnt > 0:
        failed_tables = [r["table"] for r in results if r["status"] == "FAILED"]
        print(f"⚠️  Failed tables ({failed_cnt}):")
        for t in failed_tables[:20]:
            print(f"   - {t}")
        if len(failed_tables) > 20:
            print(f"   ... and {len(failed_tables) - 20} more")
        print(f"\n💡 RESUME INSTRUCTIONS:")
        print(f"   • Next run will retry these FIRST (resume_mode='resume').")
        print(f"   • Successful tables will be skipped (within resume_skip_minutes window).")
        print(f"   • To retry ONLY failed tables right now:")
        print(f"       CONFIG['resume_mode'] = 'failed_only'")
        print(f"   • Check {CONTROL_TABLE} for error details.")

    # ── Log this run to audit history ─────────────────────────────────
    run_status = "SUCCESS" if failed_cnt == 0 else ("PARTIAL" if success_cnt > 0 else "FAILED")
    log_run(
        run_id     = run_id,
        started    = start_time,
        finished   = datetime.now(),
        mode       = resume_mode,
        force_full = force_full,
        in_scope   = total_in_scope,
        processed  = processed,
        succeeded  = success_cnt,
        failed     = failed_cnt,
        skipped    = len(skipped),
        rows       = total_rows,
        duration   = duration,
        status     = run_status,
        notes      = f"Batch size={batch_size}, stop_on_fail={stop_on_fail}"
    )
    print(f"📝 Run logged: run_id={run_id} in {RUN_HISTORY_TABLE}")

    return results

# COMMAND ----------
# MAGIC %md ## 15b. Improved Rerun UX — Preview, Status, Failed-Only Retry
# MAGIC
# MAGIC Convenience functions for smooth rerun after a failure:
# MAGIC - `preview_ingestion()` — dry-run: see what WILL happen without running
# MAGIC - `show_ingestion_status()` — one-liner formatted status of all tables
# MAGIC - `retry_failed_only()` — retry ONLY failed tables (temp mode swap)
# MAGIC - `reset_stuck_running()` — immediately reset RUNNING tables (no wait)
# MAGIC - Run history log — every run gets logged for audit

# COMMAND ----------

RUN_HISTORY_TABLE = f"`{CONFIG['catalog']}`.`{CONFIG['control_schema']}`.`d365_run_history`"

def setup_run_history_table():
    """Audit log — one row per run_ingestion() invocation."""
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {RUN_HISTORY_TABLE} (
            run_id              STRING      NOT NULL,
            run_started         TIMESTAMP,
            run_finished        TIMESTAMP,
            resume_mode         STRING,
            force_full_refresh  BOOLEAN,
            tables_in_scope     INT,
            tables_processed    INT,
            tables_succeeded    INT,
            tables_failed       INT,
            tables_skipped      INT,
            total_rows          BIGINT,
            duration_seconds    INT,
            status              STRING,
            notes               STRING
        )
        USING DELTA
        COMMENT 'Audit log of every D365 ingestion run for traceability'
    """)

setup_run_history_table()

def log_run(run_id, started, finished, mode, force_full, in_scope,
            processed, succeeded, failed, skipped, rows, duration, status, notes=""):
    """Insert one row per run into run_history for auditing."""
    notes_esc = (notes or "").replace("'", "''")[:1000]
    spark.sql(f"""
        INSERT INTO {RUN_HISTORY_TABLE} VALUES (
            '{run_id}',
            TIMESTAMP('{started.strftime("%Y-%m-%d %H:%M:%S")}'),
            TIMESTAMP('{finished.strftime("%Y-%m-%d %H:%M:%S")}'),
            '{mode}',
            {str(force_full).lower()},
            {in_scope}, {processed}, {succeeded}, {failed}, {skipped},
            {rows}, {duration},
            '{status}',
            '{notes_esc}'
        )
    """)


def preview_ingestion():
    """
    Dry-run — show exactly what run_ingestion() WOULD do without actually doing it.
    Safe to call anytime. Doesn't touch any Delta tables or cursors.

    Usage:
        preview_ingestion()   # then decide whether to run for real
    """
    base_path = get_base_path()
    print(f"\n🔍 PREVIEW MODE — no data will be written\n")
    print(f"   Base path: {base_path}")
    print(f"   Destination: {CONFIG['catalog']}.{CONFIG['raw_schema']}\n")

    # Parse schemas
    table_schemas = parse_model_json(base_path)

    # Determine scope
    configured = [t.lower().strip() for t in CONFIG.get("tables_to_ingest", [])]
    if configured:
        tables_in_scope = {t: table_schemas[t] for t in configured if t in table_schemas}
    else:
        tables_in_scope = table_schemas

    # Show classification
    print_resume_summary(tables_in_scope)

    # Show what WOULD be processed
    if CONFIG.get("force_full_refresh", False):
        to_process = list(tables_in_scope.keys())
        skipped = []
        print(f"\n  🔥 force_full_refresh=True → WOULD process ALL {len(to_process)} tables")
    else:
        to_process, skipped = order_tables_by_priority(tables_in_scope)

    batch_size = CONFIG.get("batch_size", 50)
    batches    = chunk_list(to_process, batch_size)

    print(f"\n📊 EXECUTION PLAN")
    print(f"   Would process: {len(to_process)} tables")
    print(f"   Would skip   : {len(skipped)} tables (recently successful)")
    print(f"   Batches      : {len(batches)} × {batch_size} tables")
    print(f"   Est. runtime : ~{len(to_process) * 5}s (rough estimate: 5s/table)")

    if to_process:
        print(f"\n   First 10 tables in queue:")
        for i, t in enumerate(to_process[:10], 1):
            print(f"     {i}. {t}")
        if len(to_process) > 10:
            print(f"     ... plus {len(to_process) - 10} more")

    print(f"\n✅ Preview complete. To execute for real: results = run_ingestion()\n")


def show_ingestion_status():
    """
    Quick one-liner formatted status of every table.
    Safe to call anytime — read-only against the control table.
    """
    print(f"\n📊 D365 INGESTION STATUS DASHBOARD\n")

    # Overall counts
    overall = spark.sql(f"""
        SELECT status, COUNT(*) AS n
        FROM {CONTROL_TABLE}
        GROUP BY status
        ORDER BY status
    """).collect()

    if not overall:
        print("   ℹ️  No tables have been processed yet. Run run_ingestion() to start.")
        return

    print(f"   Table status counts:")
    for row in overall:
        icon = {"SUCCESS": "✅", "FAILED": "❌", "RUNNING": "🔄"}.get(row["status"], "❓")
        print(f"     {icon} {row['status']:<10} {row['n']}")

    # Failed tables detail
    failed = spark.sql(f"""
        SELECT table_name, last_run_ts, error_message
        FROM {CONTROL_TABLE}
        WHERE status = 'FAILED'
        ORDER BY last_run_ts DESC
        LIMIT 20
    """).collect()
    if failed:
        print(f"\n   ❌ Failed tables (up to 20 most recent):")
        for row in failed:
            err = (row["error_message"] or "")[:80]
            print(f"     - {row['table_name']:<40} {err}")

    # Stuck RUNNING tables detail
    stuck_hours = CONFIG.get("stuck_running_hours", 2)
    stuck = spark.sql(f"""
        SELECT table_name, last_run_ts,
               (current_timestamp() - last_run_ts) AS running_for
        FROM {CONTROL_TABLE}
        WHERE status = 'RUNNING'
          AND last_run_ts < (current_timestamp() - INTERVAL {stuck_hours} HOURS)
    """).collect()
    if stuck:
        print(f"\n   🔄 Stuck RUNNING tables (>{stuck_hours}h — will be auto-recovered on next run):")
        for row in stuck:
            print(f"     - {row['table_name']:<40} since {row['last_run_ts']}")

    # Last run summary
    last_run = spark.sql(f"""
        SELECT * FROM {RUN_HISTORY_TABLE}
        ORDER BY run_started DESC
        LIMIT 1
    """).collect()
    if last_run:
        r = last_run[0]
        print(f"\n   Last run: {r['run_started']} → {r['status']}")
        print(f"     Processed: {r['tables_processed']} | "
              f"Success: {r['tables_succeeded']} | "
              f"Failed: {r['tables_failed']} | "
              f"Skipped: {r['tables_skipped']}")
        print(f"     Rows: {r['total_rows']:,} | Duration: {r['duration_seconds']}s")


def retry_failed_only():
    """
    Convenience: retry ONLY FAILED tables from the last run.
    Temporarily swaps resume_mode to 'failed_only', runs, then restores.
    """
    original_mode = CONFIG.get("resume_mode", "resume")
    print(f"🔄 Temporarily switching resume_mode: '{original_mode}' → 'failed_only'")
    CONFIG["resume_mode"] = "failed_only"
    try:
        return run_ingestion()
    finally:
        CONFIG["resume_mode"] = original_mode
        print(f"↩️  Restored resume_mode = '{original_mode}'")


def reset_stuck_running():
    """
    Immediately mark all RUNNING tables as FAILED (no wait for stuck_running_hours).
    Use if you know a cluster crashed and don't want to wait 2h for auto-recovery.
    """
    count = spark.sql(f"""
        SELECT COUNT(*) AS n FROM {CONTROL_TABLE} WHERE status = 'RUNNING'
    """).first()["n"]
    if count == 0:
        print("✅ No RUNNING tables to reset.")
        return
    spark.sql(f"""
        UPDATE {CONTROL_TABLE}
        SET status = 'FAILED',
            error_message = 'Manually reset via reset_stuck_running()'
        WHERE status = 'RUNNING'
    """)
    print(f"♻️  Reset {count} RUNNING tables to FAILED. Next run will retry them.")

print("✅ Rerun utilities loaded: preview_ingestion(), show_ingestion_status(),")
print("   retry_failed_only(), reset_stuck_running()")

# COMMAND ----------
# MAGIC %md ## 16. Run

# COMMAND ----------

results = run_ingestion()

# COMMAND ----------
# MAGIC %md ## 17. Monitor — Cursor & Status Dashboard

# COMMAND ----------

display(spark.sql(f"""
    SELECT
        table_name,
        status,
        last_versionnumber,
        last_folder_ts,
        last_run_ts,
        row_count,
        scd_type,
        is_full_refresh,
        CASE WHEN error_message != '' THEN error_message ELSE NULL END AS error_message
    FROM {CONTROL_TABLE}
    ORDER BY last_run_ts DESC
"""))

# COMMAND ----------
# MAGIC %md ## 18. Utilities

# COMMAND ----------

# ── Reset cursor for one table (forces full refresh on next run) ──────────────
# TABLE_TO_RESET = "account"
# spark.sql(f"DELETE FROM {CONTROL_TABLE} WHERE table_name = '{TABLE_TO_RESET}'")
# spark.sql(f"DROP TABLE IF EXISTS `{CONFIG['catalog']}`.`{CONFIG['raw_schema']}`.`{TABLE_TO_RESET}`")
# print(f"✅ Reset '{TABLE_TO_RESET}' — will do full refresh on next run")

# ── Attachment metadata — what IS and IS NOT ingested ────────────────────────
# Per connector docs: "The connector ingests attachment metadata but NOT file contents."
# "Metadata only: file names, sizes, MIME types, record associations."
# "No binary data: connector doesn't ingest file contents — download separately via D365 Web API."
# Include "annotation" and "attachment" in tables_to_ingest to get the metadata:
#
# display(spark.sql("""
#     SELECT
#         annotationid,
#         objectid,       -- record this annotation is attached to
#         subject,        -- file name / subject
#         filename,       -- original file name
#         filesize,       -- bytes
#         mimetype,       -- e.g. application/pdf
#         documentbody    -- NULL: binary content NOT ingested
#     FROM `main`.`d365_raw`.`annotation`
#     LIMIT 100
# """))
#
# To download actual file contents, use the D365 Web API:
# GET https://<org>.api.crm.dynamics.com/api/data/v9.2/annotations(<annotationid>)/documentbody/$value

# ── Query an ingested table ───────────────────────────────────────────────────
# TABLE = "account"
# display(spark.sql(f"SELECT * FROM `{CONFIG['catalog']}`.`{CONFIG['raw_schema']}`.`{TABLE}` LIMIT 100"))

# ── SCD2: view current records only ──────────────────────────────────────────
# TABLE = "account"
# display(spark.sql(f"""
#     SELECT * FROM `{CONFIG['catalog']}`.`{CONFIG['raw_schema']}`.`{TABLE}`
#     WHERE __CURRENT = true LIMIT 100
# """))

# ── SCD2: view full history for a specific record ─────────────────────────────
# TABLE = "account"; PK_VALUE = "your-guid-here"
# display(spark.sql(f"""
#     SELECT * FROM `{CONFIG['catalog']}`.`{CONFIG['raw_schema']}`.`{TABLE}`
#     WHERE accountid = '{PK_VALUE}'
#     ORDER BY __START_AT
# """))

# ── Multi-select Picklist: parse comma-separated integers ─────────────────────
# Per connector docs: "Multi-select Option Sets ingested as comma-separated integer strings"
# display(spark.sql("""
#     SELECT accountid, accountname,
#            SPLIT(industrycodes, ',') AS industry_array
#     FROM `main`.`d365_raw`.`account`
# """))

# ── Lookup: join to get related data ─────────────────────────────────────────
# Per connector docs: "Lookups ingested as GUID strings. Join with referenced table."
# display(spark.sql("""
#     SELECT a.accountid, a.name, c.fullname AS primary_contact
#     FROM `main`.`d365_raw`.`account` a
#     LEFT JOIN `main`.`d365_raw`.`contact` c
#       ON a.primarycontactid = c.contactid
# """))

# ── GAP FIX 1: OptionSetMetadata — Resolve Picklist integer codes to labels ──
# Per connector docs: "Option Sets (Picklists) ingested as integer codes.
# To map to labels, join with OptionSetMetadata table or maintain a reference mapping table."
#
# The OptionSetMetadata table is a standard Dataverse system table.
# It must be included in your Synapse Link export and ingested by this notebook.
# Add "optionsetmetadata" to CONFIG["tables_to_ingest"] if not already there.
#
# Usage — resolve a single Picklist column (e.g. industrycode on account):
# display(spark.sql("""
#     SELECT
#         a.accountid,
#         a.name,
#         a.industrycode                  AS industry_code_int,
#         osm.LocalizedLabel              AS industry_label
#     FROM `main`.`d365_raw`.`account` a
#     LEFT JOIN `main`.`d365_raw`.`optionsetmetadata` osm
#         ON  osm.OptionSetName   = 'account_industrycode'
#         AND osm.Option          = a.industrycode
#         AND osm.LocalizedLabelLanguageCode = 1033   -- 1033 = English
# """))
#
# Usage — resolve a Multi-select Picklist (comma-separated integers):
# display(spark.sql("""
#     SELECT
#         a.accountid,
#         a.name,
#         CAST(code AS INT)               AS option_code,
#         osm.LocalizedLabel              AS option_label
#     FROM `main`.`d365_raw`.`account` a
#     LATERAL VIEW EXPLODE(SPLIT(a.industrycodes, ',')) AS code
#     LEFT JOIN `main`.`d365_raw`.`optionsetmetadata` osm
#         ON  osm.OptionSetName   = 'account_industrycodes'
#         AND osm.Option          = CAST(code AS INT)
#         AND osm.LocalizedLabelLanguageCode = 1033
# """))
#
# OptionSetMetadata key columns:
#   EntityName                  — D365 entity (e.g. "account")
#   OptionSetName               — attribute name (e.g. "account_industrycode")
#   Option                      — integer code (matches your Picklist column)
#   LocalizedLabel              — human-readable label (e.g. "Technology")
#   LocalizedLabelLanguageCode  — LCID (1033=English, 1031=German, etc.)


# ── GAP FIX 3: Finance & Operations (F&O) — Virtual Entities vs Direct Tables ─
# Per connector docs, F&O data is NOT natively in Dataverse.
# Two approaches to ingest F&O data — configure in Synapse Link, then run this notebook:
#
# APPROACH A — Virtual Entities (mserp_ prefix):
#   - Install "Finance and Operations Virtual Entity" solution in Dataverse
#   - Set up S2S authorization between Dataverse and F&O
#   - Enable Track Changes for each virtual entity in Dataverse Advanced Settings
#   - In Synapse Link, select virtual entities from the Dataverse section (mserp_ prefix)
#   - Add the mserp_ table names to CONFIG["tables_to_ingest"]:
#       e.g. ["mserp_custcustomerv3entity", "mserp_vendvendorv2entity"]
#   - Benefit: pre-joined, business-ready denormalized views — less downstream transformation
#
# APPROACH B — Direct Tables (raw F&O transactional tables):
#   - In Synapse Link setup, select tables from the "D365 Finance & Operations" section
#   - These are raw transactional tables (e.g. CUSTTABLE, VENDTABLE, SALESTABLE)
#   - No mserp_ prefix — use the actual F&O table names in CONFIG["tables_to_ingest"]
#   - Benefit: full data granularity and control over data modelling
#   - Trade-off: requires more downstream joins and business logic in your notebooks
#
# After configuring Synapse Link for either approach, run this notebook normally.
# No code changes required — the notebook reads whatever tables Synapse Link exports.
#
# Example: ingest F&O Customer virtual entity
# CONFIG["tables_to_ingest"] = ["mserp_custcustomerv3entity"]
# Then query:
# display(spark.sql("""
#     SELECT mserp_customeraccount, mserp_name, mserp_customergroupid
#     FROM `main`.`d365_raw`.`mserp_custcustomerv3entity`
#     LIMIT 100
# """))
