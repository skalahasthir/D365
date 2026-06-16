# Enterprise Data Engineering Guide

**Microsoft Dynamics 365 (Dataverse) → Azure Data Lake Storage Gen2 → Azure Databricks**

> _Pure Parquet/Delta Format · No CSV · No ADF · No Synapse Pipelines_
> _Synapse Link Spark Pool · Unity Catalog · Raw · Silver · Gold Medallion Architecture_

| Property | Value |
| --- | --- |
| Document Version | 1.0 |
| Date | June 2026 |
| Classification | Confidential — Internal Use |
| Architecture Pattern | Medallion (Raw / Silver / Gold) |
| Data Format | Pure Delta Lake (Parquet + `_delta_log`) |
| Source System | Microsoft Dynamics 365 via Dataverse |
| Landing Store | Azure Data Lake Storage Gen2 |
| Consumption Platform | Azure Databricks + Unity Catalog |

---

# Table of Contents

1.  [Executive Summary](#1-executive-summary)
2.  [Architecture Overview](#2-architecture-overview)
3.  [Prerequisites & Environment Setup](#3-prerequisites--environment-setup)
4.  [Synapse Link for Dataverse — Delta Lake Configuration](#4-synapse-link-for-dataverse--delta-lake-configuration)
5.  [Unity Catalog — Storage Credential & External Location](#5-unity-catalog--storage-credential--external-location)
6.  [Raw Layer — Unity Catalog External Tables](#6-raw-layer--unity-catalog-external-tables)
7.  [Silver Layer — Cleansed & Conformed Managed Delta Tables](#7-silver-layer--cleansed--conformed-managed-delta-tables)
8.  [Gold Layer — Star Schema Managed Delta Tables](#8-gold-layer--star-schema-managed-delta-tables)
9.  [Performance Optimisation](#9-performance-optimisation)
10. [Cost Optimisation](#10-cost-optimisation)
11. [Orchestration — Databricks Workflows](#11-orchestration--databricks-workflows)
12. [Monitoring & Observability](#12-monitoring--observability)
13. [Security & Compliance](#13-security--compliance)
14. [Disaster Recovery & Data Lineage](#14-disaster-recovery--data-lineage)
15. [Troubleshooting Guide](#15-troubleshooting-guide)
16. [Official Documentation & References](#16-official-documentation--references)
17. [Appendix A — Architecture Diagrams Reference](#appendix-a--architecture-diagrams-reference)

# 1 Executive Summary

This document provides a complete enterprise-grade blueprint for
extracting data from Microsoft Dynamics 365 (Dataverse) and making it
available for analytics and reporting in Azure Databricks using a
Medallion architecture. The solution eliminates all CSV-related data
quality risks by leveraging Synapse Link for Dataverse with Apache Spark
pool mode, which writes pure Delta Lake files (Parquet + transaction
log) to Azure Data Lake Storage Gen2.

Azure Databricks, governed by Unity Catalog, reads those Delta files as
External Tables (Raw layer), applies cleansing and standardisation
transformations to produce Silver Managed Tables, and finally shapes
data into a Star Schema model for the Gold layer — consumed by Power BI,
Azure Machine Learning, and downstream applications.

No Azure Data Factory pipelines and no Azure Synapse Pipelines are used
in the data movement or transformation phases. All orchestration is
handled natively by Databricks Workflows.

Beyond the core Raw / Silver / Gold pipeline, this guide covers
schema-evolution handling for Dataverse column changes (§6.3), an
optional Delta Live Tables overlay for declarative data-quality
enforcement (§7.7), cost-optimisation guidance with concrete
recommendations per billed component (§10), a documented
disaster-recovery procedure with per-layer RPO and Delta time-travel
examples (§14), Unity Catalog audit logging via system tables (§13.3),
and Dynamics 365 Finance & Operations integration covering both direct
tables and virtual entities (§4.3). Operational troubleshooting (§15)
and reference documentation (§16) round out the deliverable.

|                      |                                             |                                                                                                                                                                                                           |
|----------------------|---------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Principle**        | **Decision**                                | **Rationale**                                                                                                                                                                                             |
| Data Format          | Pure Delta Lake (Parquet only)              | Eliminates CSV special-character parsing issues; columnar format improves query performance by 3–10×                                                                                                      |
| Source Extract       | Synapse Link Spark Pool mode                | Internal Spark job converts CDC to Delta atomically; CSVs are auto-deleted before Databricks ever reads                                                                                                   |
| Orchestration        | Databricks Workflows                        | Native scheduling with task dependencies, retry logic, and alerting — no separate pipeline service required                                                                                               |
| Governance           | Unity Catalog                               | Single governance plane for lineage, RBAC, data masking, and audit across all three layers                                                                                                                |
| Incremental Strategy | SinkModifiedOn watermark + MERGE            | Low-latency incremental refresh; full history preserved in Raw; idempotent MERGE in Silver and Gold                                                                                                       |
| Silver Layer         | Cleansed Managed Delta Tables               | Canonical, reusable source for all Gold models; enforces schema, types, and referential integrity                                                                                                         |
| Gold Layer           | Star Schema Managed Delta Tables            | Optimised for DirectQuery (Power BI) and SQL analytics; partitioned and Z-ORDERed for performance                                                                                                         |
| Cost Posture         | Pay-per-use Spark Pool + Job Clusters       | Synapse Link service is free; pay only for Spark vCore-hours when jobs run and DBUs for Silver/Gold transformations. See §10 for per-component breakdown.                                                 |
| DR Strategy          | Raw authoritative; Silver/Gold recomputable | RPO per layer (§14.1): Raw = Synapse Link interval; Silver/Gold deterministic from stored watermarks. Delta time travel adds 7-day recovery window on managed tables.                                     |
| F&O Coverage         | Direct tables + virtual entities            | F&O integration via Synapse Link supports both direct tables (D365 F&O section) and virtual entities (Dataverse section, mserp_ prefix); both flow through the same Raw / Silver / Gold pipeline (§4.3). |

# 2 Architecture Overview

## 2.1 End-to-End Architecture

The diagram below illustrates the full pipeline from D365 through to
business consumers. The three Databricks layers (Raw, Silver, Gold) are
all governed by Unity Catalog, which provides a single control plane for
access, lineage, and audit.

![Figure 1](diagrams/figure-1.png)

*Figure 1 — End-to-End Architecture: D365 Dataverse → ADLS Gen2 (Pure
Delta) → Databricks Raw / Silver / Gold → Consumers*

## 2.2 Medallion Architecture — Data Transformation Flow

Each layer builds on the previous one with progressively higher data
quality. Raw preserves the exact output from Synapse Link; Silver
cleanses and conforms the data; Gold models it for business consumption.

![Figure 2](diagrams/figure-2.png)

*Figure 2 — Medallion Architecture: Raw (Bronze) → Silver → Gold,
table-level flow with transformations applied at each layer*

## 2.3 Unity Catalog Object Hierarchy

All three layers live within a single Unity Catalog (d365_catalog).
Access is controlled at schema and table level using standard GRANT /
REVOKE SQL. Raw External Tables point at the Synapse Link-managed ADLS
paths; Silver and Gold tables are Unity Catalog Managed Tables stored
separately.

![Figure 3](diagrams/figure-3.png)

*Figure 3 — Unity Catalog hierarchy: Storage Credential → External
Location → Catalog → Schemas (raw / silver / gold) → Tables*

# 3 Prerequisites & Environment Setup

## 3.1 Azure Infrastructure

|                                   |                                                                  |                                                                                         |
|-----------------------------------|------------------------------------------------------------------|-----------------------------------------------------------------------------------------|
| **Resource**                      | **SKU / Configuration**                                          | **Notes**                                                                               |
| ADLS Gen2 Storage Account         | Standard LRS or ZRS; Hierarchical Namespace enabled              | Minimum two containers: dataverse-link (Synapse Link), databricks-managed (Silver/Gold) |
| Azure Synapse Analytics Workspace | Standard; same region as ADLS Gen2                               | Required for Spark pool; no dedicated SQL pool needed                                   |
| Synapse Apache Spark Pool         | Small (4 vCores/32 GB), Autoscale enabled, 3–10 nodes, Spark 3.5 | Dedicated to Synapse Link only; do not share with other workloads                       |
| Azure Databricks Workspace        | Premium SKU (required for Unity Catalog)                         | Premium required for Unity Catalog, column-level security, and data lineage             |
| Unity Catalog Metastore           | One per Databricks account                                       | Must be attached to the Databricks workspace before catalog creation                    |
| Azure Key Vault                   | Standard                                                         | Store service principal secrets, PATs; linked to Databricks secret scope                |
| Azure Monitor / Log Analytics     | Standard                                                         | Pipeline failure alerts; Synapse Link monitoring                                        |

## 3.2 Dynamics 365 / Dataverse Permissions

- System Administrator security role in the target Dataverse environment

- Change Tracking enabled on every table to be exported (see Section 4)

- Dataverse API v9.2 or later

- Owner role on the ADLS Gen2 storage account (required for Synapse Link
  initial setup only)

- Storage Blob Data Contributor on the ADLS Gen2 storage account
  (ongoing)

- Synapse Administrator role within the Synapse Analytics workspace

## 3.3 Databricks Permissions

- Account admin or metastore admin to create Unity Catalog metastore and
  attach it to workspace

- Workspace admin to create clusters, secret scopes, and Workflows

- CREATE STORAGE CREDENTIAL privilege on the metastore

- CREATE EXTERNAL LOCATION privilege on the metastore

## 3.4 Synapse Spark Pool Configuration

|                    |                                                  |                                                                                  |
|--------------------|--------------------------------------------------|----------------------------------------------------------------------------------|
| **Parameter**      | **Recommended Value**                            | **Reason**                                                                       |
| Node size          | Small (4 vCores / 32 GB) with Autoscale enabled  | Delta merge operations are memory-intensive                                      |
| Min nodes          | 3                                                | Ensures parallel Spark tasks for large initial loads                             |
| Max nodes          | 10 (scale to 20 for high-volume tables)          | Synapse Link auto-scales within the pool bounds                                  |
| Spark version      | 3.5 (current GA — upgrade from 3.4 required)     | Official current version per Microsoft Learn (3.4 retired, 3.3 legacy)           |
| Delta Lake version | 3.0 (pre-installed in Synapse Spark 3.5 runtime) | Paired with Spark 3.5; no separate install required                              |
| Auto-pause         | Enabled, 5-minute idle timeout                   | Reduces cost when no CDC activity                                                |
| Nightly compaction | 11 PM – 6 AM local time (Dataverse env region)   | System-scheduled; cannot be modified; increase nodes to 20 if conflicts observed |
| Exclusive use      | This pool must not be shared                     | Shared usage disrupts scheduled Synapse Link Spark jobs                          |

# 4 Synapse Link for Dataverse — Delta Lake Configuration

## 4.1 Enable Change Tracking

Change Tracking must be enabled on each Dataverse table. Tables without
Change Tracking will not appear in the Synapse Link table selector and
cannot be exported.

### Via Power Apps UI (per table)

1.  Navigate to make.powerapps.com → select your environment.

2.  Left nav → Data → Tables → select the table.

3.  Click Settings → Advanced options → toggle Track changes → ON →
    Save.

4.  Repeat for every table to be exported.

### Via Dataverse Web API (bulk)

```powershell
# PowerShell – enable Change Tracking via REST API for multiple
tables
$org = 'https://<your-org>.api.crm.dynamics.com'
$tables =
@('account','contact','salesorder','opportunity','product','lead',
'salesorderdetail','systemuser','businessunit','pricelevel')
$headers = @{ Authorization="Bearer $accessToken";
'Content-Type'='application/json' }
foreach ($t in $tables) {
$uri = "$org/api/data/v9.2/EntityDefinitions(LogicalName='$t')"
Invoke-RestMethod -Method Patch -Uri $uri -Headers $headers
-Body '{"ChangeTrackingEnabled":true}'
Write-Host "Change Tracking enabled: $t"
}
```

## 4.2 Configure Synapse Link with Spark Pool (Delta Mode)

|                                                                                                                                                                                                                                                                                                                               |
|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **NOTE:** When Spark pool mode is enabled, Synapse Link writes all data as Delta Lake (Parquet + _delta_log) into the deltalake/ folder. The intermediate CSV files are written to private staging folders and are automatically deleted by the Spark job after Delta is committed. Your Databricks pipeline never sees CSV. |

### Step-by-Step Setup

5.  In Power Apps → select your environment → Azure Synapse Link → + New
    link.

6.  Select Subscription, Resource Group, ADLS Gen2 Storage Account,
    Container.

7.  Check 'Connect to your Azure Synapse Analytics workspace' → select
    workspace.

8.  Check 'Use Spark pool for processing' → select your dedicated Spark
    pool (must be Spark 3.5).

9.  Click Next → Add tables → select all required Dataverse tables.

10. For each table, click ••• → Advanced configuration:

```
Advanced configuration per table:
Time interval (minutes) : 15 (how often Spark job runs to merge CDC)
Append only mode : No (Delta handles upserts natively in-place)
Partition strategy : Year (auto-applied when Spark pool is active)
```

11. Click Save. Initial full sync begins automatically.

## 4.3 Finance & Operations Direct Tables (D365 F&O)

Microsoft Dynamics 365 Finance & Operations (F&O) exposes its data
through Synapse Link in two ways. The first is virtual entities: F&O
entities surfaced to Dataverse via the Finance and Operations Virtual
Entity solution. These show up in the Dataverse section of the Synapse
Link selector with the mserp_ prefix (e.g. mserp_custinvoicetrans) and
behave like Dataverse tables. The second is direct tables: F&O-native
tables surfaced under a separate "D365 Finance & Operations" section of
the selector, retaining their native F&O names (no mserp_ prefix).
Direct tables are a physical copy of the F&O application data exported
via Synapse Link, sized for scalable analytics and high-volume
transactional workloads such as ledger entries and inventory movements.

### Prerequisites

- Virtual entities or direct tables must be configured in D365 F&O
  BEFORE setting up the Synapse Link profile — the F&O side controls
  which tables are exposed.

- For direct tables: in D365 F&O, enable the relevant tables in the Data
  Management workspace under 'Data feed' settings. Each table must be
  individually enabled before it appears in the Synapse Link selector.

- Requires a D365 F&O version with Synapse Link support enabled — check
  the current compatibility matrix in the Microsoft Learn docs as the
  supported versions change with each F&O release.

- The Dataverse environment and the F&O environment must be linked (the
  F&O environment must be on the same tenant and registered with the
  Dataverse environment).

### Selecting F&O Tables in Power Apps Synapse Link Setup

When you create or edit a Synapse Link profile in Power Apps, the table
selector shows two top-level sections. The Dataverse section contains
standard Dataverse entities (account, contact, salesorder, etc.) plus
any F&O virtual entities — the latter all begin with the mserp_ prefix.
A separate section labelled "D365 Finance & Operations" lists the F&O
direct tables with their native names (no mserp_ prefix). Direct tables
are generally preferred for high-volume transactional workloads because
they avoid the virtual-entity projection layer.

**Example Virtual Entity Names (mserp_ prefix in Dataverse section)**

```
mserp_hcmworkerbaseentity (Workers / HR)
mserp_custinvoicetrans (Customer invoice transactions)
mserp_salesline (Sales order lines)
mserp_inventtable (Inventory items)
mserp_ledgerjournaltrans (Ledger journal transactions)
mserp_purchline (Purchase order lines)
mserp_vendinvoicejour (Vendor invoice journal)
mserp_inventsum (Inventory on-hand summary)
```

### Example Direct Table Names (D365 Finance & Operations section, no prefix)

```
HcmWorker (Workers / HR — native F&O table)
CustInvoiceTrans (Customer invoice transactions)
SalesLine (Sales order lines)
InventTable (Inventory items)
LedgerJournalTrans (Ledger journal transactions)
PurchLine (Purchase order lines)
VendInvoiceJour (Vendor invoice journal)
```

F&O tables follow the identical deltalake/<tablename>/ folder
structure in ADLS Gen2 and are registered as Raw External Tables in
d365_catalog.raw using the same bulk registration script described in
Section 6.1 — no separate process is required. From the Databricks side,
an F&O table and a Dataverse table are indistinguishable; both are pure
Delta Lake at rest.

|                                                                                                                                                                                                                                                                                                                                                                 |
|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **NOTE:** F&O virtual entities and direct tables may have different latency characteristics than Dataverse-native tables, because the F&O extract-and-publish path runs separately from Dataverse change tracking. Check the Synapse Link profile sync status per table in Power Apps and set monitoring alerts on tables where freshness is business-critical. |

|                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     |
|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **NOTE:** F&O system columns may differ from Dataverse system columns. Verify that SinkModifiedOn, Id, and IsDelete are present and populated on each F&O table — both direct tables (D365 Finance & Operations section) and virtual entities (mserp_ prefix in the Dataverse section) — before registering it as a Raw External Table. These columns drive the watermark and the Silver MERGE logic. Tables missing SinkModifiedOn cannot use the incremental Silver pattern and must be fully reloaded each run. |

## 4.4 ADLS Gen2 Folder Structure After Sync

|                                                                            |
|----------------------------------------------------------------------------|
| **Container: dataverse-link — folder layout**                              |
| <container>/                                                             |
| <environment-guid>/                                                      |
| deltalake/ ← DATABRICKS READ TARGET (pure Delta)                           |
| account/                                                                   |
| _delta_log/ ← Delta transaction log (JSON files)                          |
| 00000000000000000000.json                                                  |
| 00000000000000000001.json                                                  |
| part-00000-<uuid>.snappy.parquet                                         |
| part-00001-<uuid>.snappy.parquet                                         |
| contact/                                                                   |
| _delta_log/ + \*.parquet                                                  |
| salesorder/                                                                |
| opportunity/                                                               |
| product/ ...                                                               |
|                                                                            |
| <YYYY-MM-DDThh-mm-ssZ>/ ← Staging CSVs (auto-deleted after Delta commit) |
| Microsoft.Athena.TombstoneData/ ← System metadata (DO NOT MODIFY)          |

## 4.5 Key System Columns Added by Synapse Link

|                |               |                                                                                                                                                                                                                               |
|----------------|---------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Column**     | **Data Type** | **Purpose / Usage**                                                                                                                                                                                                           |
| Id             | GUID / string | Primary key of the Dataverse entity. Used as merge key in Silver/Gold.                                                                                                                                                        |
| versionnumber  | bigint        | Monotonically increasing CDC sequence per row. Databricks Lakeflow uses this as cursor.                                                                                                                                       |
| SinkModifiedOn | timestamp     | Timestamp when the row was last committed to the Delta table. Used as watermark for incremental processing.                                                                                                                   |
| IsDelete       | boolean       | Soft-delete flag. TRUE = record deleted in Dataverse. For Delta mode: Synapse Link performs soft delete on next sync cycle, then hard deletes the row permanently after 30 days. Filter IsDelete=TRUE out in Silver and Gold. |
| createdon      | timestamp     | Original Dataverse record creation timestamp (not lake-side).                                                                                                                                                                 |
| modifiedon     | timestamp     | Original Dataverse last-modified timestamp (not lake-side).                                                                                                                                                                   |
| statecode      | int           | Record status code (Active=0, Inactive=1). Always check alongside IsDelete.                                                                                                                                                   |

|                                                                                                                                                                                                                                                                                                                                                                                                                                                        |
|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Why no CSV?** Synapse Link with Spark pool enabled is the only way to guarantee that Databricks never reads CSV. Special characters (commas in text fields, embedded newlines, unicode, multi-byte characters) in Dataverse field values can corrupt CSV parsing, producing phantom extra columns or split rows. Delta Lake stores data in typed, schema-encoded Parquet — special characters are stored as binary data and never affect the schema. |

# 5 Unity Catalog — Storage Credential & External Location

Unity Catalog must be authorised to read from the Synapse Link
deltalake/ path in ADLS Gen2. The authorization chain is: Managed
Identity / Service Principal → Storage Credential → External Location →
External Table.

## 5.1 Create Access Connector for Azure Databricks (Azure Portal — Required First Step)

Before creating a Storage Credential in Unity Catalog, you must create
an Access Connector for Azure Databricks resource in the Azure Portal.
This is a first-party Azure resource that connects managed identities to
your Databricks account. The Storage Credential SQL references this
connector's Resource ID.

```bash
# Azure Portal steps (one-time setup):
# 1. Portal → + Create resource → search 'Access Connector for Azure
Databricks' → Create
# 2. Configure: Subscription, Resource Group, Region (same as ADLS
Gen2), Name
# 3. Managed Identity tab: Enable System-assigned → Review + create
# 4. After deployment: copy the Resource ID:
# /subscriptions/<sub>/resourceGroups/<rg>/providers
# /Microsoft.Databricks/accessConnectors/<connector-name>
# 5. Assign 'Storage Blob Data Contributor' role on your ADLS Gen2 to
this connector's identity
# 6. Optionally assign 'Storage Queue Data Contributor' for file
event notifications
```

## 5.2 Create Storage Credential (SQL — references Access Connector Resource ID)

```sql
-- Run in Databricks SQL (requires metastore admin or account admin)
-- OPTION A: Managed Identity (recommended — no secrets to rotate)
CREATE STORAGE CREDENTIAL dataverse_adls_cred
WITH AZURE_MANAGED_IDENTITY
(CREDENTIAL
'/subscriptions/<sub-id>/resourceGroups/<rg>/providers
/Microsoft.Databricks/accessConnectors/<connector-name>');
-- OPTION B: Service Principal
CREATE STORAGE CREDENTIAL dataverse_adls_cred
WITH AZURE_SERVICE_PRINCIPAL
(DIRECTORY_ID = '<tenant-id>',
APPLICATION_ID = secret('kv-scope','dbks-sp-client-id'),
CLIENT_SECRET = secret('kv-scope','dbks-sp-client-secret'));
-- Validate
DESCRIBE STORAGE CREDENTIAL dataverse_adls_cred;
```

## 5.3 Create External Location (Read-Only)

```sql
-- Point at the deltalake/ root — covers all Synapse Link tables
CREATE EXTERNAL LOCATION dataverse_delta_loc
URL
'abfss://dataverse-link@<adls>.dfs.core.windows.net/<env-guid>/deltalake/'
WITH (STORAGE CREDENTIAL dataverse_adls_cred);
-- Mark READ_ONLY to prevent accidental writes to Synapse-managed
Delta tables
ALTER EXTERNAL LOCATION dataverse_delta_loc SET READ_ONLY;
-- Validate connectivity
VALIDATE STORAGE CREDENTIAL dataverse_adls_cred
ON LOCATION
'abfss://dataverse-link@<adls>.dfs.core.windows.net/<env-guid>/deltalake/';
```

|                                                                                                                                                                                                                                                                                                                                      |
|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **NOTE:** CRITICAL: Never run OPTIMIZE or VACUUM on Raw External Tables. Synapse Link manages the Delta log compaction and VACUUM of the deltalake/ folder on a daily schedule. Running a second VACUUM from Databricks with a shorter retention window will remove log files that Synapse Link still needs, causing CDC corruption. |

## 5.4 Create Separate External Location for Silver & Gold Storage

```python
-- Databricks-managed container for Silver and Gold tables
-- This is a SEPARATE storage account/container from the Synapse Link
container
CREATE STORAGE CREDENTIAL databricks_managed_cred
WITH AZURE_MANAGED_IDENTITY
(CREDENTIAL '/subscriptions/<sub-id>/resourceGroups/<rg>/providers
/Microsoft.Databricks/accessConnectors/<connector-name>');
CREATE EXTERNAL LOCATION databricks_managed_loc
URL 'abfss://databricks-managed@<adls>.dfs.core.windows.net/d365/'
WITH (STORAGE CREDENTIAL databricks_managed_cred);
-- This location is NOT read-only — Databricks writes Silver and Gold
here
```

## 5.5 Create Catalog and Schemas

```sql
-- Create the top-level catalog (run once by metastore admin)
CREATE CATALOG IF NOT EXISTS d365_catalog
COMMENT 'Dynamics 365 / Dataverse analytics catalog';
USE CATALOG d365_catalog;
-- Raw schema: external tables pointing at Synapse Link Delta paths
CREATE SCHEMA IF NOT EXISTS raw
COMMENT 'Raw (Bronze): External Delta Tables — Synapse Link output, no
transformation'
LOCATION
'abfss://databricks-managed@<adls>.dfs.core.windows.net/d365/raw_meta/';
-- Note: LOCATION here is for managed table spillover only; External
Tables define their own paths
-- Silver schema: cleansed, managed Delta tables
CREATE SCHEMA IF NOT EXISTS silver
COMMENT 'Silver: Cleansed, typed, deduplicated Managed Delta Tables'
MANAGED LOCATION
'abfss://databricks-managed@<adls>.dfs.core.windows.net/d365/silver/';
-- Gold schema: business-ready Star Schema managed Delta tables
CREATE SCHEMA IF NOT EXISTS gold
COMMENT 'Gold: Star Schema Managed Delta Tables — BI-ready, Power BI
DirectQuery optimised'
MANAGED LOCATION
'abfss://databricks-managed@<adls>.dfs.core.windows.net/d365/gold/';
```

## 5.6 Grants & Access Control

```sql
-- Data Engineers: full access to all layers
GRANT USE CATALOG ON CATALOG d365_catalog TO
`data-engineers@company.com`;
GRANT USE SCHEMA, CREATE TABLE, SELECT, MODIFY
ON SCHEMA d365_catalog.raw TO `data-engineers@company.com`;
GRANT USE SCHEMA, CREATE TABLE, SELECT, MODIFY
ON SCHEMA d365_catalog.silver TO `data-engineers@company.com`;
GRANT USE SCHEMA, CREATE TABLE, SELECT, MODIFY
ON SCHEMA d365_catalog.gold TO `data-engineers@company.com`;
-- Analysts: Gold only (read)
GRANT USE CATALOG ON CATALOG d365_catalog TO `analysts@company.com`;
GRANT USE SCHEMA ON SCHEMA d365_catalog.gold TO
`analysts@company.com`;
GRANT SELECT ON SCHEMA d365_catalog.gold TO `analysts@company.com`;
-- ML team: Silver + Gold read
GRANT USE CATALOG ON CATALOG d365_catalog TO `ml-team@company.com`;
GRANT USE SCHEMA, SELECT ON SCHEMA d365_catalog.silver TO
`ml-team@company.com`;
GRANT USE SCHEMA, SELECT ON SCHEMA d365_catalog.gold TO
`ml-team@company.com`;
-- Service accounts (Power BI gateway, downstream APIs): Gold only
GRANT USE CATALOG ON CATALOG d365_catalog TO
`powerbi-svc@company.com`;
GRANT USE SCHEMA, SELECT ON SCHEMA d365_catalog.gold TO
`powerbi-svc@company.com`;
```

# 6 Raw Layer — Unity Catalog External Tables

The Raw layer registers the Synapse Link Delta files as External Tables
in Unity Catalog. There is zero data movement or transformation —
Databricks reads the Parquet files in place from the deltalake/ folder.
Unity Catalog governs access and tracks lineage from source to consumer.

## 6.1 Register Raw External Tables

```sql
-- Notebook: notebooks/raw/01_register_raw_tables.py (run once after
initial sync)
ADLS_ROOT =
'abfss://dataverse-link@<adls>.dfs.core.windows.net/<env-guid>/deltalake/'
UC_CATALOG = 'd365_catalog'
UC_SCHEMA = 'raw'
# All Dataverse tables to register
TABLES = {
'account' : 'accountid',
'contact' : 'contactid',
'lead' : 'leadid',
'opportunity' : 'opportunityid',
'salesorder' : 'salesorderid',
'salesorderdetail': 'salesorderdetailid',
'product' : 'productid',
'pricelevel' : 'pricelevelid',
'systemuser' : 'systemuserid',
'businessunit' : 'businessunitid',
}
for tbl, pk in TABLES.items():
path = f'{ADLS_ROOT}{tbl}/'
spark.sql(f'''
CREATE TABLE IF NOT EXISTS {UC_CATALOG}.{UC_SCHEMA}.{tbl}
USING DELTA
LOCATION '{path}'
COMMENT 'D365 {tbl} — External Delta table, Synapse Link Spark pool
output'
''')
cnt = spark.table(f'{UC_CATALOG}.{UC_SCHEMA}.{tbl}').count()
print(f'Registered {tbl}: {cnt:,} rows (PK: {pk})')
```

|                                                     |
|-----------------------------------------------------|
| **NOTE:** ADLS _partitioned folders — Do NOT read: |

|                                                                                                                                                                                                                                                                                                                                                  |
|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **NOTE:** External Table vs Managed Table: When a Raw External Table is DROPPED, Unity Catalog removes the metadata but leaves the Parquet files untouched in ADLS. When a Silver/Gold Managed Table is DROPPED, Unity Catalog also deletes the underlying data files. This asymmetry protects the Synapse Link output from accidental deletion. |

## 6.2 Validate Raw Tables

```python
# Notebook: notebooks/raw/02_validate_raw.py
from pyspark.sql import functions as F
TABLES = ['account','contact','salesorder','opportunity','product']
for tbl in TABLES:
df = spark.table(f'd365_catalog.raw.{tbl}')
total = df.count()
deleted = df.filter(F.col('IsDelete') == True).count()
max_sink = df.agg(F.max('SinkModifiedOn')).collect()[0][0]
schema = [(f.name, f.dataType.simpleString()) for f in
df.schema.fields]
print(f'[{tbl}] rows={total:,} soft_deletes={deleted:,}
latest_sync={max_sink}')
# Assert no null PKs
pk_col = f'{tbl}id'
nulls = df.filter(F.col(pk_col).isNull()).count()
assert nulls == 0, f'NULL PK in {tbl}: {nulls} rows'
print(f' PK check PASSED ({pk_col})')
```

## 6.3 Schema Evolution — Handling Dataverse Column Changes

![Figure 4](diagrams/figure-4.png)

*Figure 4 — Schema Evolution Decision Flow: change type → Synapse Link
behaviour → Databricks impact → required action*

Dataverse schemas evolve: business users add columns, deprecate fields,
and occasionally change data types. Synapse Link handles most of these
changes automatically by extending the Delta schema in ADLS, but the
Databricks side needs deliberate handling so the Silver MERGE keeps
working. This section maps each schema-change scenario to the action
required.

### Schema Change Scenarios

|                                       |                                                                                                     |                                                                                       |                                                                                                                             |
|---------------------------------------|-----------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------|
| **Change Type**                       | **Synapse Link Behaviour**                                                                          | **Databricks Impact**                                                                 | **Action Required**                                                                                                         |
| New column added in Dataverse         | Added to Delta schema automatically via _delta_log; old rows show NULL for new column              | Raw External Table auto-reflects new column; Silver MERGE needs mergeSchema=true      | Add .option('mergeSchema', 'true') to the Silver MERGE write; update Silver SELECT list to include the new column if needed |
| Column deleted in Dataverse           | Column NOT dropped from Delta — existing rows preserved; new and updated rows set the value to NULL | No impact on Raw table structure; Silver SELECT may need updating to handle NULLs     | Review the Silver SELECT projection; add NULL-safe handling (coalesce or default) where business logic expects a value      |
| Column data type changed in Dataverse | BREAKING CHANGE — Synapse Link profile must be unlinked and relinked; full re-export from scratch   | All data re-exported from epoch; Raw External Table must be dropped and re-registered | Unlink Synapse Link → relink → re-register Raw External Tables → reset _watermarks → re-run Silver from epoch              |
| Table added to Synapse Link           | New deltalake/<tablename>/ folder created after first sync completes                              | Register as a new Raw External Table in d365_catalog.raw                              | Run the bulk registration script (Section 6.1) targeting only the new table name                                            |

### Auto-Detecting Schema Changes in Databricks

The notebook below compares the current Delta schema for each Raw table
against a snapshot stored in raw._schema_snapshots. New columns and
removed columns are logged; downstream Silver pipelines can be paused or
alerted when a breaking change is detected.

```python
# Notebook: notebooks/raw/03_schema_change_detector.py
# Run daily before the Silver pipeline to surface schema drift early
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType,
TimestampType
from delta.tables import DeltaTable
from datetime import datetime
CATALOG = 'd365_catalog'
SNAP_TABLE = f'{CATALOG}.raw._schema_snapshots'
RAW_TABLES = [
'account', 'contact', 'salesorder', 'opportunity', 'product',
'systemuser', 'businessunit', 'team'
]
# Create the snapshots table if it does not exist
if not spark.catalog.tableExists(SNAP_TABLE):
schema = StructType([
StructField('table_name', StringType(), False),
StructField('column_name', StringType(), False),
StructField('data_type', StringType(), False),
StructField('captured_at', TimestampType(), False)
])
(spark.createDataFrame([], schema)
.write.format('delta').saveAsTable(SNAP_TABLE))
# Compare current schema vs stored snapshot
drift_rows = []
for tbl in RAW_TABLES:
full_name = f'{CATALOG}.raw.{tbl}'
if not spark.catalog.tableExists(full_name):
drift_rows.append((tbl, '*MISSING*', 'table_not_registered'))
continue
current = {f.name: f.dataType.simpleString()
for f in spark.table(full_name).schema.fields}
prior = {r['column_name']: r['data_type']
for r in spark.table(SNAP_TABLE)
.filter(F.col('table_name') == tbl)
.collect()}
added = set(current) - set(prior)
removed = set(prior) - set(current)
changed = {c for c in (set(current) & set(prior))
if current[c] != prior[c]}
for c in added: drift_rows.append((tbl, c, f'ADDED ({current[c]})'))
for c in removed: drift_rows.append((tbl, c, f'REMOVED (was
{prior[c]})'))
for c in changed: drift_rows.append((tbl, c, f'TYPE_CHANGED
{prior[c]} -> {current[c]}'))
if drift_rows:
print('Schema drift detected:')
for r in drift_rows: print(f' {r}')
# Optionally raise to fail the job and trigger an alert:
# raise RuntimeError(f'Schema drift detected: {drift_rows}')
else:
print('No schema drift since last snapshot.')
# Refresh snapshot to current state (run only after drift is
reviewed)
now = datetime.utcnow()
rows = [(tbl, f.name, f.dataType.simpleString(), now)
for tbl in RAW_TABLES
if spark.catalog.tableExists(f'{CATALOG}.raw.{tbl}')
for f in spark.table(f'{CATALOG}.raw.{tbl}').schema.fields]
df = spark.createDataFrame(rows,
['table_name', 'column_name', 'data_type', 'captured_at'])
(df.write.format('delta').mode('overwrite').saveAsTable(SNAP_TABLE))
```

|                                                                                                                                                                                                                                                                                                                                                                                   |
|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **NOTE:** Never manually modify files in the Synapse Link dataverse-link container. Per official Microsoft documentation, data files written by Synapse Link must not be edited, deleted, or augmented by customers — doing so corrupts the Delta transaction log and forces a full unlink/relink. The container is configured READ ONLY from Databricks for exactly this reason. |

# 7 Silver Layer — Cleansed & Conformed Managed Delta Tables

The Silver layer transforms Raw CDC data into a clean, typed, and
deduplicated canonical dataset. Silver tables are Unity Catalog Managed
Tables, meaning Databricks owns their storage, optimisation, and
lifecycle. Silver is the single reliable source of truth for all Gold
models — multiple Gold tables can read from the same Silver table.

## 7.1 Silver Layer Design Principles

|                              |              |                                                                                    |
|------------------------------|--------------|------------------------------------------------------------------------------------|
| **Transformation**           | **Applied?** | **Details**                                                                        |
| Deduplication                | Yes          | Remove IsDelete=true rows; ensure one row per entity key using versionnumber       |
| Data type enforcement        | Yes          | Cast all Dataverse option-set ints, strings, decimals, dates to proper Spark types |
| Null handling                | Yes          | Replace nulls with meaningful defaults; flag genuinely missing values separately   |
| Column renaming              | Yes          | Convert Dataverse snake_case internal names to business-readable names             |
| Lookup / option-set decoding | Yes          | Decode numeric status codes to human-readable strings using option-set maps        |
| PII column masking           | Yes          | Apply Unity Catalog column masks on email, phone, address fields                   |
| Referential integrity check  | Yes          | Validate FK columns (e.g. contact.accountid exists in silver.account)              |
| CDC watermark control        | Yes          | Only process rows where SinkModifiedOn > last watermark — no full reloads         |
| SCD strategy                 | Type 1       | In-place MERGE upsert; overwrite with latest values                                |
| No business aggregations     | No           | Silver is entity-level; no joins to create Fact tables (that is Gold's role)       |

## 7.2 Watermark Control Table

```sql
-- Create watermark table (run once)
CREATE TABLE IF NOT EXISTS d365_catalog.raw._watermarks (
table_name STRING NOT NULL,
layer STRING NOT NULL, -- 'silver' or 'gold'
last_run_ts TIMESTAMP NOT NULL,
rows_processed BIGINT,
run_status STRING, -- 'SUCCESS' or 'FAILED'
updated_at TIMESTAMP
)
USING DELTA
COMMENT 'Incremental watermark per table per layer';
-- Seed with epoch to force full first run
INSERT INTO d365_catalog.raw._watermarks VALUES
('account', 'silver', '1970-01-01', 0, 'INIT', current_timestamp()),
('contact', 'silver', '1970-01-01', 0, 'INIT', current_timestamp()),
('salesorder', 'silver', '1970-01-01', 0, 'INIT',
current_timestamp()),
('opportunity', 'silver', '1970-01-01', 0, 'INIT',
current_timestamp()),
('product', 'silver', '1970-01-01', 0, 'INIT', current_timestamp()),
('account', 'gold', '1970-01-01', 0, 'INIT', current_timestamp()),
('salesorder', 'gold', '1970-01-01', 0, 'INIT', current_timestamp());
```

## 7.3 Silver Helper Library

```python
# File: notebooks/silver/helpers.py
from pyspark.sql import functions as F, DataFrame
from delta.tables import DeltaTable
from pyspark.sql.window import Window
def get_watermark(table: str, layer: str) -> str:
'''Returns the last successful watermark timestamp for a
table/layer.'''
rows = (spark.table('d365_catalog.raw._watermarks')
.filter((F.col('table_name') == table) & (F.col('layer') == layer))
.select('last_run_ts').collect())
return rows[0][0] if rows else '1970-01-01'
def set_watermark(table: str, layer: str, ts, rows_processed: int,
status: str = 'SUCCESS'):
'''Updates the watermark after a successful run.'''
spark.sql(f'''
MERGE INTO d365_catalog.raw._watermarks AS t
USING (
SELECT '{table}' AS table_name, '{layer}' AS layer,
CAST('{ts}' AS TIMESTAMP) AS last_run_ts,
{rows_processed} AS rows_processed,
'{status}' AS run_status,
current_timestamp() AS updated_at
) AS s
ON t.table_name = s.table_name AND t.layer = s.layer
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
''')
def active_records(df: DataFrame) -> DataFrame:
'''Filters out soft-deleted records. Use for in-place update mode
(default).'''
return df.filter(
F.col('IsDelete').isNull() \| (F.col('IsDelete') == False)
)
def decode_statecode(col_name: str, mapping: dict) -> F.Column:
'''Decodes a numeric Dataverse option-set column to a human-readable
string.'''
expr = F.lit('Unknown')
for k, v in mapping.items():
expr = F.when(F.col(col_name) == k, v).otherwise(expr)
return expr
```

## 7.4 Silver Table: silver.account

```python
# Notebook: notebooks/silver/03_silver_account.py
from notebooks.silver.helpers import *
from pyspark.sql import functions as F
from delta.tables import DeltaTable
TABLE = 'account'
LAYER = 'silver'
GOLD_TBL = f'd365_catalog.silver.{TABLE}'
WM = get_watermark(TABLE, LAYER)
# ── 1. Read incremental from Raw
────────────────────────────────────────
raw = (spark.table('d365_catalog.raw.account')
.filter(F.col('SinkModifiedOn') > WM))
if raw.isEmpty():
print(f'[silver.account] No new data since {WM}. Skipping.')
else:
# ── 2. Remove soft deletes
───────────────────────────────────────────
active = active_records(raw)
# ── 3. Decode option-sets
────────────────────────────────────────────
INDUSTRY_MAP = {1:'Accounting',2:'Agriculture',3:'Banking',
4:'Consulting',7:'Education',100000000:'Technology'}
STATUS_MAP = {0:'Active', 1:'Inactive', 2:'Merged'}
# ── 4. Select, cast, rename
──────────────────────────────────────────
silver = active.select(
F.col('accountid').alias('account_id'),
F.col('name').alias('account_name'),
F.col('accountnumber').alias('account_number'),
F.trim(F.col('address1_city')).alias('city'),
F.trim(F.col('address1_stateorprovince')).alias('state'),
F.trim(F.upper(F.col('address1_country'))).alias('country_code'),
F.col('address1_postalcode').alias('postal_code'),
F.col('industrycode').cast('int').alias('industry_code'),
decode_statecode('industrycode', INDUSTRY_MAP).alias('industry_name'),
F.col('revenue').cast('decimal(19,4)').alias('annual_revenue'),
F.col('numberofemployees').cast('int').alias('employee_count'),
F.col('telephone1').alias('phone'),
F.col('websiteurl').alias('website'),
decode_statecode('statecode', STATUS_MAP).alias('status'),
F.col('ownerid').alias('owner_id'),
F.col('createdon').cast('timestamp').alias('created_ts'),
F.col('modifiedon').cast('timestamp').alias('modified_ts'),
F.col('SinkModifiedOn').alias('last_synced_ts'),
F.col('versionnumber').alias('version_number'),
)
# ── 5. MERGE into Silver managed table
──────────────────────────────
if not spark.catalog.tableExists(GOLD_TBL):
silver.write.format('delta').mode('overwrite').saveAsTable(GOLD_TBL)
print(f'[silver.account] Created table: {GOLD_TBL}')
else:
dt = DeltaTable.forName(spark, GOLD_TBL)
(dt.alias('t').merge(silver.alias('s'), 't.account_id = s.account_id')
.whenMatchedUpdateAll()
.whenNotMatchedInsertAll()
.execute())
new_wm = raw.agg(F.max('SinkModifiedOn')).collect()[0][0]
rows = silver.count()
set_watermark(TABLE, LAYER, new_wm, rows)
print(f'[silver.account] Processed {rows:,} rows. New watermark:
{new_wm}')
```

## 7.5 Silver Table: silver.contact

```python
# Notebook: notebooks/silver/03_silver_contact.py
from notebooks.silver.helpers import *
from pyspark.sql import functions as F
from delta.tables import DeltaTable
TABLE, LAYER = 'contact', 'silver'
WM = get_watermark(TABLE, LAYER)
raw =
spark.table('d365_catalog.raw.contact').filter(F.col('SinkModifiedOn')
> WM)
if not raw.isEmpty():
active = active_records(raw)
silver = active.select(
F.col('contactid').alias('contact_id'),
F.col('accountid').alias('account_id'), # FK validated below
F.trim(F.col('fullname')).alias('full_name'),
F.trim(F.col('firstname')).alias('first_name'),
F.trim(F.col('lastname')).alias('last_name'),
F.lower(F.trim(F.col('emailaddress1'))).alias('email'),
F.col('mobilephone').alias('mobile'),
F.col('jobtitle').alias('job_title'),
F.col('department').alias('department'),
F.col('createdon').cast('timestamp').alias('created_ts'),
F.col('modifiedon').cast('timestamp').alias('modified_ts'),
F.col('SinkModifiedOn').alias('last_synced_ts'),
F.col('versionnumber').alias('version_number'),
)
# ── FK referential integrity check (warn, not fail)
─────────────────
valid_accounts =
spark.table('d365_catalog.silver.account').select('account_id')
orphaned = silver.join(valid_accounts,
silver.account_id == valid_accounts.account_id, 'left_anti')
orphan_cnt = orphaned.count()
if orphan_cnt > 0:
print(f'WARNING: {orphan_cnt} contacts have orphaned account_id
(account not in Silver)')
SILVER_TBL = 'd365_catalog.silver.contact'
if not spark.catalog.tableExists(SILVER_TBL):
silver.write.format('delta').mode('overwrite').saveAsTable(SILVER_TBL)
else:
dt = DeltaTable.forName(spark, SILVER_TBL)
(dt.alias('t').merge(silver.alias('s'),'t.contact_id=s.contact_id')
.whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
set_watermark(TABLE, LAYER,
raw.agg(F.max('SinkModifiedOn')).collect()[0][0], silver.count())
```

## 7.6 Silver Table: silver.salesorder

```python
# Notebook: notebooks/silver/03_silver_salesorder.py
from notebooks.silver.helpers import *
from pyspark.sql import functions as F
from delta.tables import DeltaTable
TABLE, LAYER = 'salesorder', 'silver'
WM = get_watermark(TABLE, LAYER)
raw =
spark.table('d365_catalog.raw.salesorder').filter(F.col('SinkModifiedOn')
> WM)
if not raw.isEmpty():
active = active_records(raw)
STATUS_MAP = {1:'In
Progress',2:'Active',3:'Invoiced',4:'Cancelled',100001:'Backordered'}
silver = active.select(
F.col('salesorderid').alias('sales_order_id'),
F.col('ordernumber').alias('order_number'),
F.col('customerid').alias('account_id'),
F.col('ownerid').alias('owner_id'),
F.col('pricelevelid').alias('price_level_id'),
F.col('totalamount').cast('decimal(19,4)').alias('total_amount'),
F.col('totallineitemamount').cast('decimal(19,4)').alias('line_total'),
F.col('totaldiscountamount').cast('decimal(19,4)').alias('discount_amount'),
F.col('totaltax').cast('decimal(19,4)').alias('tax_amount'),
F.col('freightamount').cast('decimal(19,4)').alias('freight_amount'),
decode_statecode('statuscode', STATUS_MAP).alias('status'),
F.col('statecode').cast('int').alias('state_code'),
F.col('createdon').cast('date').alias('order_date'),
F.col('submitdate').cast('date').alias('submit_date'),
F.col('requestdeliveryby').cast('date').alias('requested_delivery_date'),
F.col('createdon').cast('timestamp').alias('created_ts'),
F.col('modifiedon').cast('timestamp').alias('modified_ts'),
F.col('SinkModifiedOn').alias('last_synced_ts'),
F.col('versionnumber').alias('version_number'),
)
SILVER_TBL = 'd365_catalog.silver.salesorder'
if not spark.catalog.tableExists(SILVER_TBL):
(silver.write.format('delta').mode('overwrite')
.partitionBy('order_date').saveAsTable(SILVER_TBL))
else:
dt = DeltaTable.forName(spark, SILVER_TBL)
(dt.alias('t').merge(silver.alias('s'),'t.sales_order_id=s.sales_order_id')
.whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
set_watermark(TABLE, LAYER,
raw.agg(F.max('SinkModifiedOn')).collect()[0][0], silver.count())
```

## 7.7 Optional Enhancement: Delta Live Tables Expectations for Data Quality

![Figure 5](diagrams/figure-5.png)

*Figure 5 — DLT Pipeline Flow: Raw → Expectations → Silver / Quarantine;
pass/drop routing, event log, and downstream consumers*

Delta Live Tables (DLT) is a Databricks framework that lets you declare
data transformations together with data-quality expectations. Where the
Silver helpers in this guide use manual assert() calls and MERGE
statements to enforce row-level rules, DLT lets you attach @dlt.expect
decorators to a streaming or batch table definition and have Databricks
automatically track, log, and act on violations. Use DLT when you want
declarative pipeline definitions with built-in lineage, automatic
retries, event logs, and a UI-driven quality dashboard — or when the
team prefers managed pipelines over hand-orchestrated notebooks.

When the existing MERGE-based Silver pattern is already running and
stable, you do not need to rewrite it. Treat DLT as an optional overlay
for tables where declarative data-quality reporting, automated
quarantine, or built-in observability is more valuable than the
fine-grained control of the helper-library approach.

|                                                                                                                                                                                                                                                                                                 |
|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **NOTE:** Delta Live Tables requires Databricks Premium tier or above. The DLT product is billed per DBU separately from job clusters; check current pricing in the Databricks documentation. DLT pipelines run on dedicated DLT compute and cannot share clusters with regular Workflow tasks. |

### DLT Pipeline Example — silver.salesorder with Expectations

```python
# Notebook: notebooks/dlt/silver_salesorder_dlt.py
# Pipeline type: Triggered (run on schedule via Databricks Workflow
trigger)
# Edition: Advanced (required for @dlt.expect_or_drop and quarantine
routing)
import dlt
from pyspark.sql import functions as F
from pyspark.sql.functions import col, current_timestamp
# ── Streaming source: read Raw External Table as append-only stream
─────
# DLT pipelines cannot use the watermark control table pattern
directly;
# use Auto Loader on the ADLS deltalake/ path or a Delta streaming
read instead.
@dlt.table(
name='salesorder_raw_stream',
comment='Streaming read of Raw External Table for salesorder',
temporary=True
)
def salesorder_raw_stream():
return (spark.readStream
.format('delta')
.table('d365_catalog.raw.salesorder'))
# ── Silver target table with expectations
───────────────────────────────
@dlt.table(
name='salesorder',
comment='Cleansed sales orders with data-quality expectations',
table_properties={
'delta.autoOptimize.optimizeWrite': 'true',
'delta.autoOptimize.autoCompact': 'true',
'quality': 'silver'
}
)
@dlt.expect('valid_total_amount', 'total_amount >= 0')
@dlt.expect('non_null_account_id', 'account_id IS NOT NULL')
@dlt.expect('valid_order_date', "order_date >= '2000-01-01'")
@dlt.expect_or_drop('valid_status', 'order_status IS NOT NULL')
def silver_salesorder():
return (
dlt.read_stream('salesorder_raw_stream')
.filter(col('IsDelete') == False)
.select(
col('salesorderid').alias('sales_order_id'),
col('ordernumber').alias('order_number'),
col('customerid_account').alias('account_id'),
col('totalamount').cast('decimal(19,4)').alias('total_amount'),
col('statuscode').alias('order_status'),
col('createdon').alias('order_date'),
col('SinkModifiedOn').alias('source_modified_on'),
current_timestamp().alias('silver_loaded_at')
)
)
```

### DLT Quarantine Pattern — Capture Failing Rows

Use @dlt.expect_or_drop to filter rows that violate hard rules (silent
drop, metrics still recorded). To inspect dropped rows, define a
parallel quarantine table with inverted expectations:

```python
# Parallel quarantine table — captures rows that would have been
dropped
@dlt.table(
name='silver_quarantine_salesorder',
comment='Sales orders that failed Silver expectations — for triage'
)
@dlt.expect_all_or_drop({
'invalid_status': 'order_status IS NULL'
})
def silver_quarantine_salesorder():
return dlt.read_stream('salesorder_raw_stream')
# After the pipeline runs, the table silver_quarantine.salesorder
# contains every row dropped by silver.salesorder for human review.
# Set up a Databricks SQL Alert on row count > 0 to notify the data
team.
```

### Comparison: Manual MERGE Pattern vs DLT Pipeline

|                               |                                                                                                                |                                                                                                         |                                                                                                                    |
|-------------------------------|----------------------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------|--------------------------------------------------------------------------------------------------------------------|
| **Approach**                  | **Pros**                                                                                                       | **Cons**                                                                                                | **When to use**                                                                                                    |
| Manual MERGE (helper library) | Full control over watermark, idempotency, and partial-row updates; works with Standard tier                    | Quality checks are imperative (assert); no built-in event log; orchestration is hand-wired in Workflows | Production pipelines already running this pattern; tight cost control needed; Standard-tier workspace              |
| Delta Live Tables (DLT)       | Declarative expectations; automatic lineage; event log table per pipeline; quarantine routing; managed retries | Premium tier required; separate DLT compute billed per DBU; cannot reuse the watermark control table    | New tables where data-quality reporting is critical; teams that prefer declarative pipelines; quarantine workflows |

|                                                                                                                                                                                                                                                                                                                                                               |
|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **NOTE:** DLT pipelines cannot use the _watermarks control table pattern directly. Their incremental processing model relies on Auto Loader checkpoints or Delta streaming source offsets. If you need watermark-based reruns (e.g. reset to '1970-01-01' for a backfill), stay with the manual MERGE helpers in Section 7.3 or maintain parallel pipelines. |

|                                                                                                                                                                                                                                                                                                                            |
|----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **NAMING UPDATE:** Databricks rebranded Delta Live Tables to "Lakeflow Spark Declarative Pipelines" in 2025/2026. The product API (@dlt.table, @dlt.expect, dlt.read_stream, etc.) and pricing model are unchanged; only the marketing name has shifted. References to "DLT" throughout this document apply to both names. |

# 8 Gold Layer — Star Schema Managed Delta Tables

The Gold layer contains business-ready Delta tables modelled as a Star
Schema, sourced exclusively from Silver. Gold tables are optimised for
Power BI DirectQuery, Databricks SQL, and downstream API consumption.
They are partitioned, Z-ORDERed, and statistics-analysed after each
refresh.

## 8.1 Gold Star Schema Design

|                   |           |                           |                                                                    |
|-------------------|-----------|---------------------------|--------------------------------------------------------------------|
| **Table**         | **Type**  | **Source(s)**             | **Grain / Description**                                            |
| dim_account       | Dimension | silver.account            | One row per D365 account (company/organisation)                    |
| dim_contact       | Dimension | silver.contact            | One row per D365 contact (person)                                  |
| dim_product       | Dimension | silver.product            | One row per product/SKU                                            |
| dim_date          | Dimension | Generated                 | Date spine, one row per calendar day (2000–2035)                   |
| fact_salesorder   | Fact      | silver.salesorder + dims  | One row per sales order; FK to all dims; partitioned by order_date |
| fact_opportunity  | Fact      | silver.opportunity + dims | One row per opportunity (CRM pipeline)                             |
| agg_sales_monthly | Aggregate | fact_salesorder           | Pre-aggregated monthly revenue, orders, avg deal size              |

**Generate dim_date**

```python
# Notebook: notebooks/gold/04_dim_date.py (run once, then yearly
top-up)
from pyspark.sql import functions as F
from pyspark.sql.types import DateType
date_df = spark.range(0, 365 * 36).select(
F.expr("date_add(date'2000-01-01', CAST(id AS
INT))").cast(DateType()).alias('date_id')
).select(
F.col('date_id'),
F.year('date_id').alias('year'),
F.quarter('date_id').alias('quarter'),
F.month('date_id').alias('month'),
F.dayofmonth('date_id').alias('day'),
F.dayofweek('date_id').alias('day_of_week'),
F.date_format('date_id','MMMM').alias('month_name'),
F.date_format('date_id','EEEE').alias('day_name'),
F.expr("date_format(date_id,'yyyy-MM')").alias('year_month'),
F.expr("concat('Q',quarter(date_id),'-',year(date_id))").alias('fiscal_quarter'),
)
(date_df.write.format('delta').mode('overwrite')
.saveAsTable('d365_catalog.gold.dim_date'))
```

## 8.2 Gold Table: dim_account

```python
# Notebook: notebooks/gold/05_dim_account.py
from notebooks.silver.helpers import get_watermark, set_watermark
from pyspark.sql import functions as F
from delta.tables import DeltaTable
TABLE, LAYER = 'account', 'gold'
WM = get_watermark(TABLE, LAYER)
svr = (spark.table('d365_catalog.silver.account')
.filter(F.col('last_synced_ts') > WM))
if not svr.isEmpty():
dim = svr.select(
F.col('account_id').alias('account_key'),
F.col('account_name'),
F.col('account_number'),
F.col('city'),
F.col('state'),
F.col('country_code'),
F.col('postal_code'),
F.col('industry_code'),
F.col('industry_name'),
F.col('annual_revenue'), # decimal(19,4) — Dataverse Money type
F.col('employee_count'),
F.col('status').alias('account_status'),
F.col('created_ts').cast('date').alias('created_date'),
F.current_timestamp().alias('gold_refresh_ts'),
)
GOLD_TBL = 'd365_catalog.gold.dim_account'
if not spark.catalog.tableExists(GOLD_TBL):
dim.write.format('delta').mode('overwrite').saveAsTable(GOLD_TBL)
else:
dt = DeltaTable.forName(spark, GOLD_TBL)
(dt.alias('t').merge(dim.alias('s'),'t.account_key=s.account_key')
.whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
set_watermark(TABLE, LAYER,
svr.agg(F.max('last_synced_ts')).collect()[0][0], dim.count())
```

## 8.3 Gold Table: fact_salesorder

```python
# Notebook: notebooks/gold/05_fact_salesorder.py
from pyspark.sql import functions as F
from delta.tables import DeltaTable
# ── Read from Silver
────────────────────────────────────────────────────
orders = spark.table('d365_catalog.silver.salesorder')
accounts =
spark.table('d365_catalog.gold.dim_account').select('account_key')
dates = spark.table('d365_catalog.gold.dim_date').select('date_id')
# ── Build Fact table (join to dims for FK validation)
───────────────────
fact = (orders
.join(accounts, orders.account_id == accounts.account_key, 'left')
.select(
F.col('sales_order_id').alias('sales_order_key'),
F.col('order_number'),
F.col('account_id').alias('account_key'),
F.col('owner_id').alias('owner_key'),
F.col('price_level_id').alias('price_level_key'),
F.col('order_date'),
F.col('submit_date'),
F.col('requested_delivery_date'),
F.col('total_amount'), # decimal(19,4) — Dataverse Money type
F.col('line_total'),
F.col('discount_amount'),
F.col('tax_amount'),
F.col('freight_amount'),
(F.col('total_amount') -
F.col('discount_amount')).alias('net_amount'),
F.col('status').alias('order_status'),
F.col('state_code'),
F.col('last_synced_ts'),
F.current_timestamp().alias('gold_refresh_ts'),
))
GOLD_TBL = 'd365_catalog.gold.fact_salesorder'
if not spark.catalog.tableExists(GOLD_TBL):
(fact.write.format('delta').mode('overwrite')
.partitionBy('order_date').saveAsTable(GOLD_TBL))
else:
dt = DeltaTable.forName(spark, GOLD_TBL)
(dt.alias('t').merge(fact.alias('s'),
't.sales_order_key = s.sales_order_key')
.whenMatchedUpdateAll().whenNotMatchedInsertAll().execute())
```

## 8.4 Gold Aggregate Table: agg_sales_monthly

```python
# Notebook: notebooks/gold/06_agg_sales_monthly.py
# Fully recomputed on each refresh (fast — reads from Gold fact
table)
from pyspark.sql import functions as F
fact = spark.table('d365_catalog.gold.fact_salesorder')
agg = (fact
.groupBy(
F.date_format('order_date','yyyy-MM').alias('year_month'),
F.year('order_date').alias('year'),
F.month('order_date').alias('month'),
'order_status',
)
.agg(
F.count('sales_order_key').alias('order_count'),
F.sum('total_amount').alias('gross_revenue'),
F.sum('net_amount').alias('net_revenue'),
F.sum('discount_amount').alias('total_discounts'),
F.avg('total_amount').alias('avg_order_value'),
F.countDistinct('account_key').alias('unique_customers'),
F.current_timestamp().alias('gold_refresh_ts'),
)
)
(agg.write.format('delta').mode('overwrite')
.saveAsTable('d365_catalog.gold.agg_sales_monthly'))
```

# 9 Performance Optimisation

## 9.1 Optimisation Strategy per Layer

|                     |                                 |                |                      |                                                                                                             |
|---------------------|---------------------------------|----------------|----------------------|-------------------------------------------------------------------------------------------------------------|
| **Layer**           | **OPTIMIZE?**                   | **ZORDER?**    | **VACUUM?**          | **Notes**                                                                                                   |
| Raw (External)      | NEVER                           | NEVER          | NEVER                | Synapse Link manages all compaction automatically. Running VACUUM from Databricks will corrupt CDC history. |
| Silver (Managed)    | YES — after each refresh        | By primary key | YES — 168h retention | Frequent small merges create many small files; OPTIMIZE after each run is critical.                         |
| Gold Fact (Managed) | YES — after each refresh        | By date + FK   | YES — 168h retention | Partitioned tables benefit most from OPTIMIZE + ZORDER by query predicates.                                 |
| Gold Dim (Managed)  | YES — weekly                    | None needed    | YES — 168h retention | Dimension tables are small; weekly optimization is sufficient.                                              |
| Gold Agg (Managed)  | YES — after each full recompute | None needed    | YES — 168h retention | Agg tables are fully overwritten; OPTIMIZE after write.                                                     |

## 9.2 Optimisation Notebook

```
# Notebook: notebooks/maintenance/07_optimize_silver_gold.py
# Run daily by Databricks Workflow (off-peak hours)
SILVER_TABLES = [
('d365_catalog.silver.account', 'account_id'),
('d365_catalog.silver.contact', 'contact_id'),
('d365_catalog.silver.salesorder', 'sales_order_id'),
('d365_catalog.silver.opportunity', 'opportunity_id'),
]
GOLD_TABLES = [
('d365_catalog.gold.dim_account', None),
('d365_catalog.gold.dim_contact', None),
('d365_catalog.gold.fact_salesorder', 'account_key'),
('d365_catalog.gold.agg_sales_monthly',None),
]
for (tbl, zorder) in SILVER_TABLES + GOLD_TABLES:
print(f'Optimizing {tbl}...')
if zorder:
spark.sql(f'OPTIMIZE {tbl} ZORDER BY ({zorder})')
else:
spark.sql(f'OPTIMIZE {tbl}')
spark.sql(f'VACUUM {tbl} RETAIN 168 HOURS')
spark.sql(f'ANALYZE TABLE {tbl} COMPUTE STATISTICS FOR ALL COLUMNS')
print(f' done.')
```

# 10 Cost Optimisation

This section breaks down the cost model for every billable component in
the pipeline and gives concrete recommendations to keep the run-rate as
low as possible without sacrificing freshness or reliability. The
Synapse Link service itself is free; the only Azure costs are storage,
Spark pool compute (per vCore-hour while a job runs), and the Databricks
DBU consumption for Silver/Gold transformations and SQL serving.

## 10.1 Cost Drivers

|                            |                                                             |                                                                                                                                                    |
|----------------------------|-------------------------------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------------------|
| **Component**              | **Cost Model**                                              | **Optimisation Recommendation**                                                                                                                    |
| Synapse Link service       | Free (included with Dataverse / Power Platform licence)     | No additional charge beyond Azure storage costs. Confirm with your Microsoft account team for any tenant-specific limits.                          |
| Synapse Spark Pool compute | Charged per vCore-hour only when a job runs; zero when idle | Use autoscale (3-10 nodes); increase Synapse Link time interval for non-critical tables; pool auto-pauses after 5 min idle.                        |
| ADLS Gen2 storage          | Per GB/month (hot/cool/archive tiers)                       | Run Delta VACUUM with 168h retention on Silver/Gold; keep Snappy compression on Parquet; consider Cool tier for historical Raw data after 90 days. |
| Databricks compute (DBU)   | Per DBU-hour (Job vs All-Purpose; Standard vs Premium)      | Use Job Clusters (not All-Purpose) for Workflow tasks; auto-terminate after job; use Photon for SQL workloads where supported.                     |
| Databricks SQL Warehouse   | Per DBU-hour (Serverless / Pro / Classic)                   | Use Serverless SQL Warehouses for Gold-layer Power BI queries; auto-stop after 10 min idle; right-size starting cluster.                           |

|                                                                                                                                                                                                                    |
|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **TIP:** Synapse Link itself has no additional licensing cost beyond your existing Power Platform / Dataverse capacity. Only the underlying Azure resources (Spark pool compute and ADLS Gen2 storage) are billed. |

## 10.2 Recommendations

- Set the Synapse Link time interval to 60 minutes for historical and
  non-operational tables (audit logs, archive entities); keep 15 minutes
  only for tables that feed real-time dashboards or alerting.

- Use Job Clusters in Databricks Workflows rather than All-Purpose
  clusters. Job Clusters are billed at a lower DBU rate and terminate
  automatically when the job finishes — All-Purpose clusters keep
  running until you stop them.

- Enable soft-delete on the ADLS Gen2 dataverse-link container (7-day
  retention) to protect against accidental deletions. The cost overhead
  is small because Synapse Link rarely deletes files, and the recovery
  saves a full re-export.

- Monitor Spark pool cost with Azure Cost Management filters on the
  resource group. Set a monthly budget alert at 80% of expected spend so
  you find out about run-away costs in days rather than at month-end.

- Schedule Databricks OPTIMIZE and VACUUM jobs during off-peak hours
  (e.g. 02:00 local) on a daily basis. Skipping OPTIMIZE causes
  small-file accumulation that increases query cost over time.

- Right-size the Spark pool node SKU. Small (4 vCore / 32 GB) with
  autoscale 3-10 is sufficient for most Dataverse footprints; only step
  up to Medium or Large if single-table sync exceeds the time interval.

## 10.3 Enable Soft-Delete on ADLS Gen2 (Azure CLI)

Run this once per storage account to enable container soft-delete with a
7-day retention. This protects against accidental container or blob
deletion at no compute cost — only marginal storage cost for
soft-deleted data during the retention window.

```bash
# Variables — adjust to your environment
RG='rg-d365-data'
STORAGE='stdataversegen2prod'
# Enable container soft-delete with 7-day retention
az storage account blob-service-properties update \
--account-name $STORAGE \
--resource-group $RG \
--enable-container-delete-retention true \
--container-delete-retention-days 7
# Enable blob (file-level) soft-delete with 7-day retention
az storage account blob-service-properties update \
--account-name $STORAGE \
--resource-group $RG \
--enable-delete-retention true \
--delete-retention-days 7
# Verify
az storage account blob-service-properties show \
--account-name $STORAGE \
--resource-group $RG \
--query
'{container:containerDeleteRetentionPolicy,blob:deleteRetentionPolicy}'
```

|                                                                                                                                                                                                                                                      |
|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **NOTE:** Soft-delete protects against accidental deletion. It does NOT protect against ransomware encryption of in-place files. For full protection, combine soft-delete with versioning and immutable blob policy on the dataverse-link container. |

# 11 Orchestration — Databricks Workflows

All scheduling and dependency management is handled by Databricks
Workflows. No Azure Data Factory and no Synapse Pipelines are required.
Synapse Link independently manages its own scheduling for the Spark CDC
merge jobs.

|                                                                                                    |
|----------------------------------------------------------------------------------------------------|
| **Databricks Workflow: wf_d365_full_pipeline**                                                     |
| Trigger: Schedule every 30–60 minutes (production), or on demand                                   |
|                                                                                                    |
| TASK 1: validate_raw                                                                               |
| Notebook: notebooks/raw/02_validate_raw.py                                                         |
| Purpose: Assert raw tables are current and PKs are non-null                                        |
|                                                                                                    |
| TASK 2a: silver_account \[depends_on: validate_raw\]                                               |
| TASK 2b: silver_contact \[depends_on: validate_raw\]                                               |
| TASK 2c: silver_salesorder \[depends_on: validate_raw\]                                            |
| TASK 2d: silver_opportunity \[depends_on: validate_raw\]                                           |
| TASK 2e: silver_product \[depends_on: validate_raw\]                                               |
| (All Silver tasks run in parallel after validate_raw)                                              |
|                                                                                                    |
| TASK 3a: gold_dim_account \[depends_on: silver_account\]                                           |
| TASK 3b: gold_dim_contact \[depends_on: silver_contact\]                                           |
| TASK 3c: gold_dim_product \[depends_on: silver_product\]                                           |
|                                                                                                    |
| TASK 4: gold_fact_salesorder \[depends_on: gold_dim_account, gold_dim_contact, silver_salesorder\] |
| TASK 5: gold_agg_monthly \[depends_on: gold_fact_salesorder\]                                      |
|                                                                                                    |
| TASK 6: optimize_silver_gold \[depends_on: gold_agg_monthly\] — runs at low priority               |
|                                                                                                    |
| ON FAILURE: Email alert + Teams webhook notification                                               |

## 11.1 Schedule

|                              |                           |                                               |                       |
|------------------------------|---------------------------|-----------------------------------------------|-----------------------|
| **Workflow / Job**           | **Trigger**               | **Frequency**                                 | **Owner**             |
| wf_d365_full_pipeline        | Cron schedule             | Every 30–60 minutes                           | Data Engineering team |
| wf_d365_optimize_maintenance | Cron schedule             | Daily 02:00 UTC (off-peak)                    | Data Engineering team |
| wf_d365_initial_register     | Manual (run once)         | On demand — after first Synapse Link sync     | Data Engineering team |
| Synapse Link Spark jobs      | Automated by Synapse Link | Every 15 minutes (configured in Synapse Link) | Platform / Azure      |

# 12 Monitoring & Observability

## 12.1 Monitoring Surfaces

|                     |                                                 |                                                                |
|---------------------|-------------------------------------------------|----------------------------------------------------------------|
| **Surface**         | **What to Monitor**                             | **How**                                                        |
| Synapse Link Status | Table sync state (Active/Error), last sync time | Power Apps → Azure Synapse Link → table status badges          |
| Databricks Workflow | Job success/failure, task duration, run history | Databricks UI → Workflows → job run history + email alerts     |
| ADLS Gen2           | Blob availability, write rates, storage size    | Azure Monitor → Metrics → Transactions, Capacity               |
| Delta Table Health  | Row counts, latest watermark, history           | DESCRIBE HISTORY + custom validation notebook                  |
| Watermark Table     | Last successful run per layer per table         | Query d365_catalog.raw._watermarks                            |
| Data Quality        | Null PKs, orphaned FKs, decode failures         | validate_raw notebook assertions + DLT expectations (optional) |

## 12.2 Monitoring Queries

```sql
-- Pipeline health dashboard queries
-- 1. Current watermarks per layer
SELECT table_name, layer, last_run_ts, rows_processed, run_status,
updated_at
FROM d365_catalog.raw._watermarks
ORDER BY layer, table_name;
-- 2. Row counts across all layers
SELECT 'raw.account' AS table_ref, COUNT(*) AS row_count FROM
d365_catalog.raw.account
UNION ALL
SELECT 'silver.account', COUNT(*) FROM d365_catalog.silver.account
UNION ALL
SELECT 'gold.dim_account', COUNT(*) FROM
d365_catalog.gold.dim_account;
-- 3. Synapse Link lag (time since last raw sync)
SELECT table_name,
MAX(SinkModifiedOn) AS latest_raw_sync,
ROUND((unix_timestamp() - unix_timestamp(MAX(SinkModifiedOn)))/60, 1)
AS lag_minutes
FROM d365_catalog.raw.account
UNION ALL
SELECT 'salesorder', MAX(SinkModifiedOn),
ROUND((unix_timestamp()-unix_timestamp(MAX(SinkModifiedOn)))/60,1)
FROM d365_catalog.raw.salesorder;
-- 4. Delta table history (detect unexpected writes)
DESCRIBE HISTORY d365_catalog.raw.account LIMIT 5;
```

# 13 Security & Compliance

## 13.1 Access Control Matrix

|                                       |                                |                |                |
|---------------------------------------|--------------------------------|----------------|----------------|
| **Principal / Group**                 | **Raw**                        | **Silver**     | **Gold**       |
| Data Engineering (group)              | SELECT, REFRESH                | ALL PRIVILEGES | ALL PRIVILEGES |
| Data Analysts (group)                 | No access                      | No access      | SELECT only    |
| ML / Data Science (group)             | No access                      | SELECT only    | SELECT only    |
| Power BI Service Account              | No access                      | No access      | SELECT only    |
| Databricks Workflow Service Principal | SELECT (reads Raw)             | ALL PRIVILEGES | ALL PRIVILEGES |
| Synapse Link Managed Identity         | READ + WRITE (owns deltalake/) | No access      | No access      |

## 13.2 PII Column Masking

```sql
-- Unity Catalog column masking (applied at Silver layer — never
expose in Gold)
-- Create masking function
CREATE FUNCTION d365_catalog.silver.mask_email(email STRING)
RETURNS STRING
RETURN CASE
WHEN is_member('data-engineers') THEN email
ELSE concat(left(email, 2), '****@****.***')
END;
-- Apply to column
ALTER TABLE d365_catalog.silver.contact
ALTER COLUMN email
SET MASK d365_catalog.silver.mask_email;
-- Row-level security on Gold fact table
CREATE FUNCTION d365_catalog.gold.rls_salesorder(account_key STRING)
RETURNS BOOLEAN
RETURN is_member('global-analysts') OR
EXISTS (SELECT 1 FROM d365_catalog.gold.dim_account
WHERE account_key = account_key
AND country_code IN (
SELECT country FROM user_region_access
WHERE user = current_user()));
ALTER TABLE d365_catalog.gold.fact_salesorder
SET ROW FILTER d365_catalog.gold.rls_salesorder ON (account_key);
```

## 13.3 Unity Catalog Audit Logging

Unity Catalog automatically captures all data access events for tables
it governs — every SELECT, INSERT, UPDATE, DELETE, MERGE, OPTIMIZE, and
VACUUM is logged together with the user, source IP, user agent, notebook
or SQL warehouse ID, and the full table name accessed. Query the
system.access.audit table to monitor who accessed which tables, when,
and from where. This is the single source of truth for access audit
across the d365_catalog catalog.

```sql
-- View all SELECT operations on Gold tables in the last 7 days
SELECT
event_time,
user_name,
action_name,
request_params.table_full_name AS table_accessed,
source_ip_address,
user_agent
FROM system.access.audit
WHERE event_time >= current_timestamp() - INTERVAL 7 DAYS
AND action_name IN ('SELECT', 'READ')
AND request_params.table_full_name LIKE 'd365_catalog.gold.%'
ORDER BY event_time DESC;
-- Detect any write operations on Raw External Tables (should never
happen)
SELECT event_time, user_name, action_name,
request_params.table_full_name
FROM system.access.audit
WHERE action_name IN ('INSERT', 'UPDATE', 'DELETE', 'MERGE',
'OPTIMIZE', 'VACUUM')
AND request_params.table_full_name LIKE 'd365_catalog.raw.%'
ORDER BY event_time DESC;
-- Top 10 most-accessed Gold tables in the last 30 days
SELECT
request_params.table_full_name AS table_accessed,
COUNT(DISTINCT user_name) AS distinct_users,
COUNT(*) AS access_count
FROM system.access.audit
WHERE event_time >= current_timestamp() - INTERVAL 30 DAYS
AND action_name IN ('SELECT', 'READ')
AND request_params.table_full_name LIKE 'd365_catalog.gold.%'
GROUP BY request_params.table_full_name
ORDER BY access_count DESC
LIMIT 10;
```

|                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 |
|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **NOTE:** The system.access.audit table is in Public Preview on Databricks Premium workspaces with Unity Catalog. The access schema must be explicitly enabled by an account admin via the Unity Catalog system schemas API; once enabled, there is typically a 12-24 hour backfill delay before historical events appear. Confirm enrolment by running SHOW SCHEMAS IN system — if the access schema is missing, ask an account admin to enable it. Audit-table column names (action_name, request_params.\*) may change as the preview evolves; check Microsoft Learn for the current schema. |

# 14 Disaster Recovery & Data Lineage

![Figure 6](diagrams/figure-6.png)

*Figure 6 — Disaster Recovery: 7-step Silver + Gold rebuild procedure;
RPO per layer; time-travel as alternative for managed tables*

The Medallion architecture used in this pipeline is inherently
resilient: Raw is ground-truth in ADLS Gen2, and both Silver and Gold
are fully recomputable from Raw via the published notebooks. This
section formalises the Recovery Point Objective (RPO) and recovery
procedure for each layer, then describes how Unity Catalog lineage and
Delta time travel are used to investigate and roll back bad loads.

## 14.1 RPO / Recovery Procedure per Layer

|                         |                                                                                       |                                                                                                    |                                                                                                               |
|-------------------------|---------------------------------------------------------------------------------------|----------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------|
| **Layer**               | **Recovery Point Objective**                                                          | **Recovery Procedure**                                                                             | **Notes**                                                                                                     |
| Raw (External Tables)   | RPO = Synapse Link interval (15 min) — ADLS data is authoritative                     | DROP External Tables and re-register using the bulk registration script (Section 6.1)              | Data in ADLS is unaffected by dropping External Tables; only the metadata pointer is recreated                |
| Silver (Managed Tables) | RPO = _watermarks.last_run_ts — rerun deterministically from the stored watermark    | Reset _watermarks for affected tables to '1970-01-01' and re-run the Silver notebooks in parallel | Full re-process if Silver managed table storage is lost; partial re-process from a chosen watermark otherwise |
| Gold (Managed Tables)   | RPO = fully recomputable from Silver — Gold is derived, Silver is the source of truth | Re-run all Gold notebooks in dependency order (dims first, then facts, then aggregates)            | Gold can be safely truncated and rebuilt without data loss as long as Silver is intact                        |

## 14.2 Full Silver + Gold Loss — Recovery Procedure

Use this procedure when the databricks-managed container is lost,
corrupted, or must be rebuilt from scratch. The dataverse-link container
(Raw source data) is assumed intact — if it is not, restart from the
Synapse Link unlink/relink process described in Section 6.3.

12. Verify Raw External Tables are accessible and current by running the
    validate notebook (Section 6.2). Confirm that row counts and
    SinkModifiedOn max values are within the expected Synapse Link
    interval.

13. Reset _watermarks for all Silver tables to '1970-01-01' using the
    helper UPDATE in Section 7.2. This forces a full re-read of every
    Raw record on the next Silver run.

14. Run Silver notebooks in parallel — account, contact, salesorder,
    opportunity, product. Each notebook is idempotent and writes via
    DeltaTable.forName().merge(), so concurrent execution is safe.

15. Verify Silver row counts match Raw active record counts (filter for
    IsDelete = false). Discrepancies indicate a Silver transformation
    issue, not a Raw issue.

16. Run Gold notebooks in dependency order — dim_date and other
    dimensions first, then fact tables, then aggregate tables. Use the
    Databricks Workflow DAG to enforce ordering.

17. Run OPTIMIZE on all Silver and Gold tables to compact the small
    files generated by the catch-up MERGE. Skip OPTIMIZE on Raw —
    Synapse Link manages that layer.

18. Notify downstream consumers (Power BI dataset owners, ML team,
    downstream APIs) that the refresh is complete. Trigger Power BI
    dataset refresh from the Workflow's final task.

## 14.3 Unity Catalog Lineage

Unity Catalog automatically captures table-level and column-level
lineage for every Spark SQL or DataFrame operation against UC-governed
tables, including all MERGE statements run by the Silver and Gold
notebooks. To view lineage in the Databricks UI, open Catalog Explorer,
navigate to the catalog → schema → table, and click the Lineage tab. The
upstream graph shows the source Raw / Silver tables and any intermediate
views; the downstream graph shows every notebook, workflow, and
dashboard that consumes the table.

Column-level lineage requires the operation to be expressible as Spark
SQL or PySpark DataFrame — Python UDFs that read from one column and
write to another lose column granularity. Stick to SQL expressions and
DataFrame transformations wherever possible to preserve full lineage.

## 14.4 Delta Time Travel — Recovery Examples

Every Databricks-managed Delta table maintains a transaction log that
allows queries against historical versions of the data. Use this to
recover from a bad load, investigate when a value changed, or simulate
the state of a table at a specific point in time.

```sql
-- Recover Gold table to state before a bad load
RESTORE TABLE d365_catalog.gold.fact_salesorder TO VERSION AS OF 5;
-- Inspect what changed in a specific version
DESCRIBE HISTORY d365_catalog.gold.fact_salesorder;
-- Read Silver data as it was 48 hours ago for investigation
SELECT * FROM d365_catalog.silver.salesorder
TIMESTAMP AS OF (current_timestamp() - INTERVAL 48 HOURS)
WHERE sales_order_id = '<order-id>';
-- Read Gold fact table as it was at a specific UTC timestamp
SELECT COUNT(*) AS rows_at_snapshot
FROM d365_catalog.gold.fact_salesorder
TIMESTAMP AS OF '2026-05-01T00:00:00Z';
```

|                                                                                                                                                                                                                                                                                                                                                                                |
|--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **NOTE:** RESTORE TABLE is a Databricks-managed Delta operation. It is NOT applicable to Raw External Tables — Raw tables always reflect the live Synapse Link Delta output in ADLS and have no Databricks-side version history. To roll back Raw, you must work at the ADLS level (e.g. via container soft-delete or ADLS versioning) or trigger a full Synapse Link re-sync. |

|                                                                                                                                                                                                                                                                                                                        |
|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **NOTE:** Time travel is bounded by the VACUUM retention window. The default retention is 168 hours (7 days). If you rely on time travel for compliance, audit, or recovery windows beyond 7 days, set delta.deletedFileRetentionDuration explicitly on the table and align the maintenance VACUUM retention to match. |

# 15 Troubleshooting Guide

|                                                               |                                                                                                     |                                                                                                                                                                                                   |
|---------------------------------------------------------------|-----------------------------------------------------------------------------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| **Symptom**                                                   | **Root Cause**                                                                                      | **Resolution**                                                                                                                                                                                    |
| deltalake/ folder empty after initial sync                    | Spark pool Delta conversion not yet run                                                             | Wait 15–30 min. Check Synapse Workspace → Monitor → Spark applications for status.                                                                                                                |
| Raw External Table: schema mismatch error                     | Dataverse schema evolved; Delta log updated but UC cache stale                                      | Run: MSCK REPAIR TABLE d365_catalog.raw.<table>; then re-read.                                                                                                                                  |
| Silver MERGE fails: column not found                          | New column added in Dataverse; Silver schema needs evolution                                        | Add .option('mergeSchema','true') to write, or ALTER TABLE ADD COLUMN.                                                                                                                            |
| IsDelete rows leaking into Silver                             | IsDelete filter missing or applied before watermark filter                                          | Ensure active_records() is applied AFTER the watermark filter, not before.                                                                                                                        |
| Gold table stale — Silver not updated                         | Workflow failure; Databricks cluster auto-start disabled                                            | Check wf_d365_full_pipeline run history; verify cluster policy allows auto-start.                                                                                                                 |
| Duplicate rows in Gold                                        | MERGE condition incorrect (missing compound key)                                                    | Verify MERGE ON clause covers all business keys; add logging to count matched vs inserted.                                                                                                        |
| Storage Credential / External Location 403                    | Managed Identity missing RBAC role on ADLS                                                          | Re-assign Storage Blob Data Contributor to the Databricks Access Connector MI.                                                                                                                    |
| VALIDATE STORAGE CREDENTIAL fails                             | Firewall on ADLS blocking Databricks IPs                                                            | Add 'Allow Azure services and resources' exception on ADLS Gen2 network settings.                                                                                                                 |
| Synapse Spark pool not triggering                             | Incorrect Spark pool linked to Synapse workspace                                                    | In Synapse Studio → Manage → Apache Spark pools; verify pool is attached correctly.                                                                                                               |
| versionnumber missing in changelog                            | Changelogs not enabled on Synapse Link profile                                                      | Re-configure Synapse Link profile; re-initialise affected tables.                                                                                                                                 |
| OPTIMIZE/VACUUM fails on Raw table                            | Attempted on External Table without Delta write permission                                          | Do NOT run OPTIMIZE/VACUUM on Raw tables — they are READ ONLY by design.                                                                                                                          |
| Column data type changed in Dataverse; Synapse Link breaks    | Breaking schema change — Synapse Link does not support column type changes                          | Per official FAQ: changing a column data type is a breaking change. You must unlink and relink the Synapse Link profile, which triggers a full re-export of all data.                             |
| High Silver MERGE latency (>30 min)                          | Too many small Parquet files in Silver table                                                        | Run OPTIMIZE on Silver table immediately; increase cluster workers for merge jobs.                                                                                                                |
| Synapse Link shows error state after Spark pool upgrade       | Old Spark 3.4 pool retired; profile still references it                                             | In Power Apps → Azure Synapse Link → select profile → click Upgrade in ribbon → select the new Spark 3.5 pool from the dropdown → click Update. Allow up to 48 hours for the upgrade to complete. |
| Silver MERGE is slow — taking more than 30 minutes per table  | Too many small Parquet files accumulated in Silver managed table from frequent incremental merges   | Run OPTIMIZE d365_catalog.silver.<table> immediately after the slow merge. Increase Databricks job cluster workers from 4 to 8. Schedule daily OPTIMIZE in the maintenance workflow.            |
| Power BI DirectQuery on Gold fact table returns timeout error | Gold fact table has too many small files; no ZORDER applied for query predicate columns             | Run OPTIMIZE d365_catalog.gold.fact_salesorder ZORDER BY (order_date, account_key). Ensure the Databricks SQL Warehouse is Serverless tier with auto-scaling enabled.                             |
| Delta time travel query returns 'version not found' error     | VACUUM removed the requested version; retention window was shorter than the time travel query range | Increase VACUUM retention: VACUUM <table> RETAIN 336 HOURS (14 days). Do not run VACUUM with less than 168 hours retention on Gold tables that support time travel reporting.                   |

# 16 Official Documentation & References

### Microsoft Learn — Synapse Link & Dataverse

[<u>→ Export Dataverse Data in Delta Lake
Format</u>](https://learn.microsoft.com/en-us/power-apps/maker/data-platform/azure-synapse-link-delta-lake)

[<u>→ Azure Synapse Link for Dataverse —
Overview</u>](https://learn.microsoft.com/en-us/power-apps/maker/data-platform/export-to-data-lake)

[<u>→ Read Incremental Updates —
SinkModifiedOn</u>](https://learn.microsoft.com/power-apps/maker/data-platform/azure-synapse-link-incremental)

[<u>→ Synapse Link FAQs — Append Mode,
versionnumber</u>](https://learn.microsoft.com/en-us/power-apps/maker/data-platform/export-data-lake-faq)

[<u>→ Transition FAQ: Export to Data Lake → Synapse
Link</u>](https://learn.microsoft.com/en-us/power-apps/maker/data-platform/azure-synapse-link-transition-faq)

### Azure Databricks — Unity Catalog

[<u>→ Connect ADLS Gen2 External Location to Unity
Catalog</u>](https://learn.microsoft.com/en-us/azure/databricks/connect/unity-catalog/cloud-storage/external-locations-adls)

[<u>→ Work with External Tables in Unity
Catalog</u>](https://docs.databricks.com/aws/en/tables/external)

[<u>→ Manage External
Locations</u>](https://learn.microsoft.com/en-us/azure/databricks/connect/unity-catalog/cloud-storage/manage-external-locations)

[<u>→ Column Masks and Row Filters in Unity
Catalog</u>](https://docs.databricks.com/data-governance/unity-catalog/row-and-column-filters.html)

[<u>→ Delta Lake — MERGE, OPTIMIZE,
VACUUM</u>](https://docs.databricks.com/delta/delta-update.html)

[<u>→ Medallion Architecture — Phase 6: Delta Lake
Design</u>](https://learn.microsoft.com/en-us/azure/databricks/lakehouse-architecture/deployment-guide/delta-lake)

### Azure Databricks — D365 / Lakeflow Connect

[<u>→ Configure D365 Dataverse Source for Databricks
Ingestion</u>](https://learn.microsoft.com/en-us/azure/databricks/ingestion/lakeflow-connect/d365-source-setup)

[<u>→ D365 Connector Reference — versionnumber
cursor</u>](https://learn.microsoft.com/en-us/azure/databricks/ingestion/lakeflow-connect/d365-reference)

[<u>→ D365 Connector
FAQs</u>](https://docs.databricks.com/aws/en/ingestion/lakeflow-connect/d365-faq)

### Azure Storage & Security

[<u>→ ADLS Gen2 —
Introduction</u>](https://learn.microsoft.com/en-us/azure/storage/blobs/data-lake-storage-introduction)

[<u>→ Service Principal Auth for ADLS Gen2
(Databricks)</u>](https://docs.databricks.com/storage/azure-storage.html)

[<u>→ Azure Key Vault — Databricks Secret
Scopes</u>](https://docs.databricks.com/security/secrets/secret-scopes.html)

### Additional References

The following Microsoft Learn pages cover topics referenced in the new
sections of this guide (Delta Live Tables, Synapse Link advanced
configuration, and structured streaming reads/writes on Delta).

- Delta Lake structured streaming reads and writes:
  https://learn.microsoft.com/en-us/azure/databricks/delta/table-streaming-reads-and-writes

- Synapse Link advanced configuration options (time interval, partition
  strategy):
  https://learn.microsoft.com/en-us/power-apps/maker/data-platform/azure-synapse-link-advanced-configuration

- Delta Live Tables — official documentation:
  https://learn.microsoft.com/en-us/azure/databricks/delta/delta-live-tables

# Appendix A — Architecture Diagrams Reference

All three architecture diagrams are reproduced below at full page width
for easy reference, printing, and presentation use.

## A1 — — End-to-End Architecture

Complete pipeline view: D365 / Dataverse → Synapse Link (Spark Pool) →
ADLS Gen2 (Pure Delta) → Azure Databricks (Raw · Silver · Gold) →
Consumers. Shows zone boundaries, data format at each stage, and the
Unity Catalog governance layer.

![Figure 1](diagrams/figure-1.png)

*Figure A1 — End-to-End Architecture: D365 Dataverse → ADLS Gen2 (Pure
Delta/Parquet) → Databricks Raw / Silver / Gold → Consumers*

## A2 — — Medallion Architecture: Data Transformation Flow

Table-level transformation flow across all three Databricks layers.
Shows which tables exist in each layer, what transformations are applied
at Silver, and how Gold tables are modelled from Silver sources.
Includes Consumer zone and Unity Catalog governance.

![Figure 2](diagrams/figure-2.png)

*Figure A2 — Medallion Architecture: Raw (Bronze) → Silver → Gold,
table-level flow with transformations applied at each layer*

## A3 — — Unity Catalog Object Hierarchy

Complete Unity Catalog hierarchy for d365_catalog. Shows the full chain
from Azure Access Connector → Storage Credential → External Location
through to the Catalog, each Schema (raw / silver / gold), and all
tables registered within each schema.

![Figure 3](diagrams/figure-3.png)

*Figure A3 — Unity Catalog hierarchy: Azure Access Connector → Storage
Credential → External Location → d365_catalog → Schemas → Tables*

|                                  |                                                                                    |                             |
|----------------------------------|------------------------------------------------------------------------------------|-----------------------------|
| **Diagram**                      | **Description**                                                                    | **Where Used**              |
| A1 — End-to-End Architecture     | Full pipeline zones: Source, Landing Zone, Databricks (Raw/Silver/Gold), Consumers | Section 2.1 and Appendix A1 |
| A2 — Medallion Architecture Flow | Table-level data flow and transformations applied at each Medallion layer          | Section 2.2 and Appendix A2 |
| A3 — Unity Catalog Hierarchy     | Storage Credential → External Location → Catalog → Schema → Table object chain     | Section 2.3 and Appendix A3 |

*All documentation verified from official Microsoft Learn and Databricks
documentation. June 2026.*

This document is confidential and intended for internal use only.
