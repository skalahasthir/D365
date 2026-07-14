# Enterprise Standard for Fact Grain and Raw-to-Silver Table Design

## 1. Purpose

This document defines enterprise-level standards for:

- defining the grain of transactional, snapshot, and reference datasets;
- designing Silver-layer tables directly from Raw D365 Finance & Operations tables;
- preserving source-system meaning while improving consistency, quality, usability, and performance;
- preparing governed Silver data products that can later support dimensions, facts, OBTs, semantic models, APIs, and advanced analytics.

This standard assumes that D365 Finance & Operations data is replicated into the Raw layer through an approved mechanism such as Azure Synapse Link or Microsoft Fabric Link.

The Silver layer described here is built from physical D365 source tables and metadata. It is **not based on D365 data entities**.

---

## 2. Target Architecture

```text
D365 Finance & Operations
        |
        | Synapse Link / Fabric Link
        v
Raw Layer
- Source-aligned tables
- Minimal transformation
- Full technical lineage
- Append/change history where available
        |
        | Standardisation, validation, deduplication,
        | business-key alignment and controlled joins
        v
Silver Layer
- Business-aligned tables
- Defined grain
- Standardised data types and codes
- Resolved keys and relationships
- Reconciled and quality controlled
        |
        | Dimensional modelling
        v
Gold Layer
- Dimensions
- Facts
- Aggregate tables
- OBTs
- Power BI semantic models
```

---

## 3. Core Design Principle

Every Silver table must have an explicit and testable grain.

> **The grain states exactly what one row represents.**

The grain must be defined before:

- selecting columns;
- joining source tables;
- calculating measures;
- resolving duplicates;
- defining keys;
- implementing incremental loads;
- designing downstream dimensions or facts.

A table without a documented grain is not production ready.

---

# Part I — Fact-Table Grain

## 4. What Is Grain?

The grain is the lowest level of detail represented by one row.

Examples:

```text
One row per posted sales invoice line per legal entity.

One row per general-ledger accounting entry per voucher,
main account and financial-dimension combination.

One row per inventory transaction and inventory-dimension combination.

One row per item, warehouse, inventory status and snapshot date.

One row per customer per legal entity.
```

A complete grain definition identifies:

1. the business event or object;
2. the row-level identifier;
3. the legal-entity context;
4. the relevant date or time context;
5. any dimensional combination that changes row uniqueness.

---

## 5. Grain-Definition Template

Use the following template for every fact candidate and Silver transaction table.

```text
Table name:
Business process:
Table type:
Grain:
Source system:
Source tables:
Business key:
Technical key:
Legal-entity handling:
Date/time basis:
Measures:
Descriptive attributes:
Expected uniqueness:
Late-arriving data handling:
Correction/reversal handling:
Incremental-load column:
Reconciliation control:
```

Example:

```text
Table name:
silver_sales_invoice_line

Business process:
Posted customer invoicing

Table type:
Transaction-level Silver table

Grain:
One row per posted customer invoice line per legal entity

Source tables:
CustInvoiceTrans
CustInvoiceJour
SalesLine
SalesTable
InventDim

Business key:
DataAreaId + InvoiceId + InvoiceLineRecId

Technical key:
Hash of DataAreaId, InvoiceId and InvoiceLineRecId

Date/time basis:
Invoice posting date

Measures:
InvoicedQuantity
LineAmountTransactionCurrency
LineAmountAccountingCurrency
TaxAmount
CostAmount

Expected uniqueness:
Exactly one current row per business key
```

---

## 6. Types of Grain

### 6.1 Transaction Grain

One row represents one business event.

Examples:

- one sales invoice line;
- one vendor invoice line;
- one inventory movement;
- one ledger entry;
- one payment transaction.

Transaction-grain tables are normally append-oriented, although corrections, reversals and late updates must still be handled.

---

### 6.2 Periodic Snapshot Grain

One row represents the state of a business object at a regular point in time.

Examples:

- one item and warehouse balance per day;
- one customer open balance per month end;
- one bank-account balance per day;
- one project status per week.

Example grain:

```text
One row per snapshot date, legal entity, item, site,
warehouse, inventory status, batch and serial number.
```

Snapshot tables must clearly define:

- snapshot frequency;
- snapshot cutoff time;
- timezone;
- whether the snapshot is beginning-of-period or end-of-period;
- treatment of late-arriving transactions;
- whether snapshots are restated.

---

### 6.3 Accumulating Snapshot Grain

One row represents a process instance that progresses through milestones.

Example:

```text
One row per sales-order line, updated as the line moves through
creation, confirmation, picking, packing, shipment and invoicing.
```

Typical milestone dates:

- order-created date;
- confirmed date;
- requested ship date;
- actual ship date;
- invoice date;
- cancellation date.

Accumulating snapshots are useful for process-duration and bottleneck analysis.

---

### 6.4 Factless Fact Grain

One row represents the occurrence of an event or relationship without a numeric measure.

Examples:

- one employee training attendance;
- one customer promotion eligibility;
- one product available in one warehouse;
- one workflow approval event.

The row count itself becomes the measure.

---

## 7. Grain Must Not Be Mixed

A table must not combine different levels of detail.

Incorrect example:

```text
Invoice header total
Invoice line amount
Tax summary amount
Payment amount
```

These values may exist at different grains.

Mixing them in one table can cause:

- duplicated measures;
- incorrect totals;
- many-to-many joins;
- ambiguous aggregation;
- inconsistent filters;
- reconciliation failures.

Preferred design:

```text
silver_sales_invoice_header
Grain: one row per invoice

silver_sales_invoice_line
Grain: one row per invoice line

silver_sales_invoice_tax
Grain: one row per invoice, tax code and tax component

silver_customer_payment
Grain: one row per payment transaction
```

These tables can later feed separate facts or controlled bridges.

---

## 8. Grain and Measure Compatibility

Every measure must be valid at the table's grain.

For each measure, document:

- source table and source column;
- calculation rule;
- currency;
- unit of measure;
- sign convention;
- additive behaviour;
- null treatment;
- reversal treatment.

### Additive measure

Can be summed across all applicable dimensions.

Examples:

- invoiced quantity;
- net sales amount;
- debit amount;
- credit amount.

### Semi-additive measure

Can be summed across some dimensions but not across time.

Examples:

- inventory balance;
- bank balance;
- open receivable balance.

### Non-additive measure

Should not be summed directly.

Examples:

- margin percentage;
- unit price;
- exchange rate;
- average cost;
- percentage discount.

For non-additive measures, store the additive components where possible.

Instead of storing only:

```text
MarginPercentage
```

also store:

```text
RevenueAmount
CostAmount
MarginAmount
```

Calculate margin percentage downstream as:

```text
MarginAmount / RevenueAmount
```

---

# Part II — Raw Layer Standards

## 9. Purpose of the Raw Layer

The Raw layer preserves source data with minimal semantic alteration.

Its primary objectives are:

- traceability;
- reproducibility;
- change capture;
- auditability;
- source reconciliation;
- recovery and replay.

Raw tables should remain close to the source D365 physical-table structure.

Typical retained technical columns include:

```text
RECID
DATAAREAID
PARTITION
RECVERSION
CREATEDDATETIME
MODIFIEDDATETIME
SourceFileName
SourceExtractTimestamp
SourceCommitVersion
IngestionTimestamp
RecordHash
IsDeleted
```

Not every source exposes every field. Available fields must be documented.

---

## 10. Raw-Layer Rules

The Raw layer should:

- preserve original source values;
- preserve source column names where practical;
- avoid business-friendly renaming that hides source lineage;
- preserve source precision and scale;
- retain deleted-record indicators where available;
- retain extraction and ingestion metadata;
- support replay of Silver processing;
- avoid destructive deduplication;
- avoid untraceable manual corrections.

The Raw layer may perform limited technical transformations such as:

- converting unreadable source encodings;
- standardising ingestion metadata;
- flattening transport envelopes;
- enforcing a technically readable file format;
- quarantining structurally corrupt records.

Business transformations belong in Silver or Gold.

---

# Part III — Silver-Layer Purpose

## 11. What Is a Silver Table?

A Silver table is a governed, reusable, quality-controlled representation of source data aligned to a clear business object or business event.

It should be easier to consume than Raw data while remaining sufficiently detailed and traceable for multiple downstream uses.

Silver is not automatically:

- a fact table;
- a dimension;
- an OBT;
- a Power BI table;
- a replica of a D365 entity;
- a report-specific dataset.

A Silver table is an intermediate enterprise data product.

---

## 12. Recommended Silver Table Categories

### 12.1 Core Master Tables

Examples:

```text
silver_customer
silver_vendor
silver_product
silver_main_account
silver_warehouse
silver_worker
silver_legal_entity
```

These tables standardise master records and natural keys.

---

### 12.2 Transaction Tables

Examples:

```text
silver_sales_order_line
silver_sales_invoice_line
silver_purchase_order_line
silver_vendor_invoice_line
silver_inventory_transaction
silver_general_ledger_entry
```

These preserve atomic business-event detail.

---

### 12.3 Reference Tables

Examples:

```text
silver_currency
silver_unit_of_measure
silver_country_region
silver_payment_terms
silver_customer_group
silver_vendor_group
silver_inventory_status
```

---

### 12.4 Relationship and Bridge Tables

Examples:

```text
silver_product_category_assignment
silver_customer_address_relationship
silver_worker_position_assignment
silver_account_financial_dimension_combination
```

Use a bridge when a genuine many-to-many relationship exists.

---

### 12.5 Snapshot Tables

Examples:

```text
silver_inventory_daily_snapshot
silver_customer_open_balance_snapshot
silver_vendor_open_balance_snapshot
silver_budget_monthly_snapshot
```

---

# Part IV — Raw-to-Silver Design Standards

## 13. Start With the Business Process, Not the Source Table

Do not create Silver tables merely because Raw tables exist.

Incorrect approach:

```text
Raw table exists
        ↓
Create one Silver copy
```

Preferred approach:

```text
Business process and required grain
        ↓
Identify authoritative Raw source tables
        ↓
Define transformations and relationships
        ↓
Create reusable Silver business table
```

One Raw table may feed multiple Silver tables.

Several Raw tables may feed one Silver table.

---

## 14. Establish Source-System Authority

For every Silver attribute, identify the authoritative source.

Example:

| Silver attribute | Authoritative source |
|---|---|
| Customer account | CustTable.AccountNum |
| Customer party name | DirPartyTable.Name |
| Customer group | CustTable.CustGroup |
| Primary address | Party-location and postal-address tables |
| Default currency | CustTable.Currency |
| Legal entity | DATAAREAID |

Do not select a source merely because the same value is available in multiple tables.

Define precedence rules where multiple candidates exist.

---

## 15. Define Join Conditions Explicitly

Every join must document:

- left and right tables;
- join columns;
- expected cardinality;
- legal-entity scope;
- effective-date condition;
- active/current-row condition;
- handling of unmatched rows;
- duplicate-risk control.

Example:

```text
Join:
CustTable.Party = DirPartyTable.RecId

Expected cardinality:
Many customer records to one party record across the relevant scope

Join type:
Left join

Unmatched handling:
Retain customer row and set party attributes to unknown;
raise a data-quality exception.
```

Never assume that matching column names imply a valid relationship.

---

## 16. Respect D365 Legal-Entity Scope

`DATAAREAID` is frequently part of the business key and join condition.

Incorrect join:

```sql
SalesTable.SalesId = SalesLine.SalesId
```

Safer conceptual join:

```sql
SalesTable.DataAreaId = SalesLine.DataAreaId
AND SalesTable.SalesId = SalesLine.SalesId
```

Ignoring legal-entity scope can create:

- cross-company duplicates;
- incorrect customer attribution;
- duplicated transaction amounts;
- false relationships.

Document whether each table is:

- company specific;
- shared;
- global;
- virtual-company scoped;
- partition scoped.

---

## 17. Preserve RECID but Do Not Treat It as a Universal Business Key

`RECID` is a source-system technical identifier.

It is useful for:

- source lineage;
- internal relationships;
- change detection;
- troubleshooting;
- joining related source records.

However:

- it may not carry business meaning;
- it may differ between environments;
- it may not support cross-system matching;
- it should not be exposed as the only enterprise business identifier.

Silver tables should retain both:

```text
SourceRecId
BusinessNaturalKey
EnterpriseTechnicalKey
```

---

## 18. Define Business Keys

A business key uniquely identifies a business object or event from the source-system perspective.

Examples:

```text
Customer:
DATAAREAID + AccountNum

Vendor:
DATAAREAID + AccountNum

Sales order header:
DATAAREAID + SalesId

Sales order line:
DATAAREAID + SalesId + LineNum
```

Do not assume a business key is unique until tested.

Tests must verify:

- duplicate count;
- null count;
- reuse over time;
- differences across legal entities;
- correction scenarios;
- source-system sequencing.

Where the natural key is unstable, retain the natural key but use a durable technical key downstream.

---

## 19. Create Deterministic Technical Keys

A deterministic technical key can be generated from the complete business key.

Example:

```text
SHA-256(
  upper(trim(DATAAREAID)) ||
  '|' ||
  upper(trim(SALESID)) ||
  '|' ||
  canonical(LINENUM)
)
```

Technical-key rules must standardise:

- null representation;
- trimming;
- casing;
- dates;
- decimals;
- separators;
- Unicode handling;
- source-system identifier.

The same input must always produce the same key.

---

## 20. Standardise Data Types

Silver tables should convert source-specific types into enterprise-approved types.

Examples:

| Source issue | Silver treatment |
|---|---|
| Empty string used as missing value | Convert to null where semantically valid |
| String date | Convert to date/timestamp |
| Numeric flag | Convert to governed Boolean or status code |
| Inconsistent decimal scale | Standardise precision and scale |
| Local timestamp | Store UTC and business-local representation |
| Enumeration integer | Retain code and add governed label |
| Fixed-width padded string | Trim according to standard |

Never cast silently when invalid values would be lost.

Invalid conversions should be:

- quarantined;
- flagged;
- counted;
- reported.

---

## 21. Handle D365 Enumerations Correctly

D365 tables commonly store enumeration values as integers.

Silver should generally retain:

```text
StatusCode
StatusName
```

Example:

```text
SalesStatusCode = 3
SalesStatusName = Invoiced
```

The label must come from governed metadata or a controlled reference mapping.

Do not embed unexplained numeric meanings in downstream reports.

Do not replace the numeric code completely; retaining both code and label improves auditability.

---

## 22. Standardise Dates and Times

For every date/time field, document:

- source timezone;
- storage timezone;
- business timezone;
- date-only versus timestamp meaning;
- sentinel-date treatment;
- daylight-saving handling;
- fiscal-date derivation;
- cutoff logic.

Recommended pattern:

```text
CreatedDateTimeUtc
CreatedDateTimeLocal
CreatedDate
```

Do not treat dates such as `1900-01-01`, `1901-01-01` or other minimum values as valid business dates without confirming source semantics.

Convert known source sentinel dates to null only through a documented rule.

---

## 23. Preserve Monetary Context

Every monetary amount must identify:

- transaction currency;
- accounting currency;
- reporting currency;
- exchange-rate type;
- exchange-rate date;
- sign convention;
- decimal precision;
- whether tax is included;
- whether the amount is posted, estimated or settled.

Preferred Silver columns:

```text
TransactionCurrencyCode
TransactionCurrencyAmount
AccountingCurrencyCode
AccountingCurrencyAmount
ReportingCurrencyCode
ReportingCurrencyAmount
ExchangeRate
ExchangeRateDate
```

Do not combine amounts in different currencies into one column without an explicit currency column.

---

## 24. Preserve Quantity and Unit Context

Every quantity must identify its unit of measure.

Preferred pattern:

```text
Quantity
UnitOfMeasureCode
InventoryQuantity
InventoryUnitOfMeasureCode
StandardQuantity
StandardUnitOfMeasureCode
ConversionFactor
```

Unit conversions must be:

- deterministic;
- traceable;
- effective-date aware where required;
- based on governed conversion rules.

---

## 25. Handle Financial Dimensions Carefully

D365 financial dimensions can involve:

- ledger-dimension combinations;
- default dimensions;
- main accounts;
- dimension attributes;
- dimension attribute values;
- value combinations and value sets.

Do not flatten financial dimensions casually into one text field.

Preferred Silver design:

```text
silver_ledger_dimension_combination
silver_default_dimension_value
silver_financial_dimension_value
silver_financial_dimension_set
```

A transaction table can retain:

```text
LedgerDimensionRecId
DefaultDimensionRecId
MainAccount
```

Downstream models may pivot selected dimensions such as:

```text
CostCentre
Department
BusinessUnit
Project
Region
```

The design must support new dimensions without rebuilding the entire Raw layer.

---

## 26. Deduplication Standard

Deduplication must be based on a documented rule, not a blanket `DISTINCT`.

For each duplicate scenario, identify:

- duplicate key;
- reason duplicates occur;
- authoritative record;
- ordering column;
- tie-breaker;
- whether duplicates represent valid history.

Example:

```text
Partition by:
DATAAREAID, SALESID, LINENUM

Order by:
MODIFIEDDATETIME descending,
RECVERSION descending,
RECID descending

Keep:
Latest valid current record
```

`SELECT DISTINCT` is prohibited as a default deduplication strategy because it can conceal source or join problems.

---

## 27. Soft Deletes and Hard Deletes

Silver processing must define deletion behaviour.

Possible source indicators:

- explicit `IsDeleted`;
- change-feed delete event;
- missing record in a full snapshot;
- source validity flag;
- cancellation status.

Recommended Silver fields:

```text
IsDeleted
DeletedDateTime
RecordValidFrom
RecordValidTo
IsCurrent
```

A hard delete in the source must not silently remove historical analytical evidence unless retention policy explicitly permits it.

---

## 28. Change Data Capture and Incremental Processing

Incremental loads may use:

- source change-feed version;
- source commit version;
- modified datetime;
- ingestion timestamp;
- watermark;
- hash comparison;
- snapshot comparison.

`ModifiedDateTime` alone may be insufficient when:

- updates do not consistently change it;
- multiple records share the same timestamp;
- deletes are not represented;
- clocks or timezones differ;
- late-arriving records appear.

An enterprise incremental pattern should include:

```text
LastProcessedSourceVersion
LastSuccessfulLoadTimestamp
CurrentBatchId
SourceRecordHash
OperationType
```

Loads must be:

- idempotent;
- restartable;
- deterministic;
- auditable;
- capable of replay from Raw.

---

## 29. Idempotency

Running the same Silver load twice with the same Raw input must produce the same Silver result.

Idempotency prevents:

- duplicate rows;
- doubled measures;
- inconsistent current-state flags;
- broken history.

Use deterministic merge conditions based on the complete grain and approved keys.

---

## 30. Late-Arriving and Out-of-Order Data

Silver design must expect records to arrive after related records or after their business-effective date.

Examples:

- invoice line arrives before invoice header;
- transaction arrives before customer master;
- reversal arrives days after original posting;
- source correction changes a prior accounting period.

Possible handling:

- retain unresolved foreign reference;
- assign an unknown member later in Gold;
- reprocess impacted partitions;
- perform targeted backfill;
- restate downstream snapshots;
- flag reconciliation as provisional.

Do not discard a valid transaction solely because a descriptive master record is temporarily missing.

---

## 31. Reversals, Corrections and Cancellations

Do not overwrite or remove financial events without understanding their accounting treatment.

A reversal may be represented as:

- a new transaction with opposite sign;
- a status change;
- a link to the original transaction;
- a cancellation record;
- a corrected replacement record.

Silver should retain:

```text
OriginalTransactionKey
ReversalTransactionKey
IsReversal
IsReversed
CorrectionReason
TransactionStatus
```

Reporting rules must distinguish:

- gross activity;
- reversals;
- net activity;
- cancelled but never posted transactions.

---

## 32. History Strategy in Silver

Not every Silver table requires slowly changing dimension logic.

Choose deliberately among:

### Current-state table

Contains only the latest source state.

Useful for:

- current operational lookup;
- current customer status;
- current product description.

### Append-only event table

Retains all events.

Useful for:

- invoice postings;
- ledger entries;
- inventory movements.

### Bitemporal or effective-dated history table

Retains business-valid and system-processing periods.

Useful when analytics must answer:

- what was believed at the time;
- when the source changed;
- which value was effective on a historical date.

Recommended history columns:

```text
ValidFrom
ValidTo
SystemFrom
SystemTo
IsCurrent
```

---

## 33. Null, Blank, Zero and Unknown Are Different

Do not treat these as interchangeable:

- null;
- empty string;
- zero;
- unknown;
- not applicable;
- not yet available.

Define column-level semantics.

Example:

```text
DiscountAmount = 0
```

may mean no discount.

```text
DiscountAmount = null
```

may mean not available or not applicable.

A controlled unknown value should be used only when semantically appropriate.

---

## 34. Column Naming Standards

Silver names should be business-readable but traceable.

Recommended:

```text
CustomerAccount
CustomerName
InvoiceDate
NetSalesAmount
SourceRecId
SourceTableName
SourceSystemCode
```

Avoid:

- unexplained abbreviations;
- source-specific prefixes without purpose;
- spaces or special characters;
- overloaded generic names such as `Value`, `Code`, or `Status`;
- renaming that breaks lineage.

Maintain a mapping between:

```text
Silver column
Raw table
Raw column
Transformation rule
Business definition
```

---

## 35. Audit Columns

Every Silver table should include appropriate audit metadata.

Recommended minimum:

```text
SourceSystemCode
SourceTableName
SourceRecId
SourceRecordHash
SourceModifiedDateTime
IngestionDateTime
SilverCreatedDateTime
SilverUpdatedDateTime
LoadBatchId
PipelineRunId
IsDeleted
DataQualityStatus
```

For multi-source tables, retain attribute-level lineage in metadata even if not every lineage field is physically stored in the table.

---

# Part V — Data Quality

## 36. Required Data-Quality Dimensions

Every production Silver table should be evaluated for:

- completeness;
- uniqueness;
- validity;
- consistency;
- accuracy;
- timeliness;
- referential integrity;
- reconciliation.

---

## 37. Mandatory Grain Tests

For each table:

### Uniqueness

```text
Count of rows by declared business key must equal 1
for current-state datasets unless duplicates are explicitly valid.
```

### Null key test

```text
Required grain columns must not be null.
```

### Join-multiplication test

```text
Row count and measure totals before and after enrichment joins
must remain within documented expectations.
```

### Source reconciliation

```text
Silver row count and control totals must reconcile to Raw
after documented filters, exclusions and deduplication.
```

### Incremental consistency

```text
Incremental result must match a full rebuild for the same cutoff.
```

---

## 38. Data-Quality Disposition

Each record may be classified as:

```text
PASS
WARNING
QUARANTINE
REJECT
```

### PASS

Meets all mandatory rules.

### WARNING

Usable but contains a non-critical issue.

### QUARANTINE

Retained outside the trusted Silver output pending investigation.

### REJECT

Structurally or semantically invalid and excluded according to policy.

Rejected records must never disappear silently.

---

## 39. Reconciliation Controls

Recommended controls include:

- source row count;
- Silver row count;
- duplicate count;
- deleted-record count;
- rejected-record count;
- sum of key quantities;
- sum of transaction-currency amounts;
- sum of accounting-currency amounts;
- minimum and maximum business dates;
- distinct legal-entity count;
- hash or checksum by partition.

For financial tables, reconciliation should occur by relevant combinations such as:

```text
Legal entity
Accounting date
Posting layer
Currency
Main account
Voucher
```

---

# Part VI — Performance and Physical Design

## 40. Partitioning

Partition based on access and load patterns, not only source layout.

Common partition candidates:

- accounting date;
- transaction date;
- snapshot date;
- legal entity;
- source commit version.

Avoid excessive high-cardinality partitioning.

The partition strategy should support:

- incremental loads;
- targeted reprocessing;
- retention;
- common query filters;
- file-size optimisation.

---

## 41. File and Table Optimisation

For lakehouse implementations:

- avoid excessive small files;
- compact incrementally;
- select appropriate file sizes;
- cluster or order by commonly filtered columns where supported;
- collect statistics where supported;
- avoid over-wide Silver tables;
- separate rarely used large text fields where justified.

Performance optimisation must not change semantic grain.

---

## 42. Schema Evolution

Silver pipelines must explicitly handle:

- new source columns;
- removed source columns;
- renamed columns;
- data-type widening;
- data-type narrowing;
- enumeration changes;
- changed nullability.

Recommended policy:

```text
Additive compatible change:
Accept through controlled deployment and metadata update.

Breaking change:
Stop affected trusted publication, quarantine impacted data,
and require impact assessment.
```

Do not silently coerce incompatible schema changes.

---

# Part VII — Security and Governance

## 43. Sensitive Data

Classify columns such as:

- personal identifiers;
- bank details;
- tax identifiers;
- email addresses;
- phone numbers;
- addresses;
- salary data;
- authentication-related data.

Apply:

- least-privilege access;
- masking or tokenisation;
- row-level or column-level security;
- retention rules;
- audit logging.

Silver should not expose sensitive fields merely because they exist in Raw.

---

## 44. Ownership

Every Silver table must have:

```text
Business owner
Data owner
Technical owner
Data steward
Support group
Service-level objective
Refresh frequency
Criticality classification
```

---

## 45. Required Documentation

Each Silver table must document:

- purpose;
- grain;
- source tables;
- source-to-target mappings;
- joins;
- filters;
- keys;
- calculations;
- enumeration mappings;
- legal-entity handling;
- date/time handling;
- currency handling;
- data-quality rules;
- reconciliation rules;
- incremental-load method;
- deletion handling;
- history strategy;
- known limitations;
- downstream consumers.

---

# Part VIII — D365-Specific Raw-to-Silver Considerations

## 46. Do Not Assume D365 Table Names Explain Business Meaning

D365 physical tables are designed for application processing.

A business concept may be distributed across:

- header and line tables;
- party framework tables;
- address framework tables;
- inventory-dimension tables;
- financial-dimension tables;
- posting tables;
- parameter tables;
- reference tables.

Source relationships must be validated through:

- D365 metadata;
- table relations;
- application developers;
- functional consultants;
- sample data;
- source-system reconciliation.

---

## 47. Header and Line Tables

Header and line tables must normally remain separate in Silver unless the resulting grain is explicitly line level.

If header attributes are added to lines:

- verify one header per line;
- ensure the join does not duplicate lines;
- label attributes as header-level;
- avoid repeating header-level additive amounts.

---

## 48. Posted Versus Unposted Transactions

Separate operational documents from posted accounting events.

Examples:

```text
Sales order line
does not equal
posted sales invoice line.

Purchase order line
does not equal
posted vendor invoice line.

General journal line
does not equal
posted general-ledger entry.
```

A report requiring actual revenue should normally use posted invoice/accounting data, not open sales orders.

Silver table names should clearly identify state:

```text
silver_sales_order_line
silver_sales_packing_slip_line
silver_sales_invoice_line
silver_general_ledger_entry
```

---

## 49. Inventory Dimensions

Inventory-related grain may depend on:

- site;
- warehouse;
- location;
- batch;
- serial;
- inventory status;
- configuration;
- size;
- colour;
- style.

Do not aggregate these dimensions away in Silver unless the use case explicitly requires it.

Retain the inventory-dimension identifier and resolved components.

---

## 50. Posting Layers and Accounting Context

General-ledger and financial tables may include multiple posting layers or accounting representations.

The grain and reconciliation must account for:

- current layer;
- operations layer;
- tax layer;
- custom layers;
- accounting currency;
- reporting currency;
- transaction currency.

Do not combine layers without an explicit business rule.

---

## 51. Shared Versus Company-Specific Tables

A D365 table may be:

- shared globally;
- company specific;
- shared with company-specific relationships.

Do not add `DATAAREAID` blindly to every key, and do not omit it blindly.

The legal-entity design must be based on table metadata and business behaviour.

---

## 52. Table Inheritance and Replacement Keys

Some D365 structures use inheritance or related-table patterns.

Silver design must determine:

- base table;
- derived table;
- inheritance discriminator;
- active subtype;
- correct join pattern.

Do not assume that all business attributes are stored in a single physical table.

---

# Part IX — Silver to Gold Handoff

## 53. Silver Is Not the Final Dimensional Model

A Silver customer table may still contain:

- source natural keys;
- source status codes;
- current-state attributes;
- source-level history;
- multiple addresses.

Gold `DimCustomer` may add:

- surrogate key;
- conformed customer identity;
- slowly changing dimension handling;
- unknown member;
- standard hierarchy;
- reporting classifications.

A Silver invoice-line table may still contain:

- source references;
- operational codes;
- multiple currencies;
- original units.

Gold `FactSalesInvoice` may add:

- dimension surrogate keys;
- conformed measures;
- reporting currency;
- measure classifications;
- downstream performance optimisation.

---

## 54. Silver Readiness Checklist for Dimension Creation

A Silver master table is ready to feed a dimension when:

- grain is one row per defined source business object or effective version;
- natural key is documented and tested;
- duplicate handling is deterministic;
- important attributes are standardised;
- history strategy is known;
- deleted records are represented;
- legal-entity scope is correct;
- data-quality thresholds are met;
- source lineage is complete.

---

## 55. Silver Readiness Checklist for Fact Creation

A Silver transaction table is ready to feed a fact when:

- one row represents one atomic business event;
- event key is unique;
- posting status is clear;
- transaction date and accounting date are available;
- quantities include units;
- amounts include currencies;
- reversals and corrections are represented;
- dimension lookup keys are available;
- source totals reconcile;
- late-arriving data can be reprocessed;
- incremental processing is idempotent.

---

# Part X — Enterprise Review Checklist

## 56. Grain Review

- [ ] Is the one-row meaning stated clearly?
- [ ] Is the business process identified?
- [ ] Is the grain atomic enough for known analytical requirements?
- [ ] Are mixed grains avoided?
- [ ] Are all key columns included?
- [ ] Has uniqueness been proven with data?
- [ ] Are measures valid at this grain?
- [ ] Are snapshot cutoffs defined where applicable?
- [ ] Are legal entity and time context included where required?

---

## 57. Source and Join Review

- [ ] Are authoritative source tables documented?
- [ ] Is every join condition documented?
- [ ] Is expected join cardinality defined?
- [ ] Are cross-company joins prevented?
- [ ] Are effective-date conditions applied?
- [ ] Are unmatched records retained or handled explicitly?
- [ ] Has join multiplication been tested?
- [ ] Are source relationships validated against D365 metadata?

---

## 58. Transformation Review

- [ ] Are data types standardised?
- [ ] Are sentinel values handled?
- [ ] Are enumeration codes and labels retained?
- [ ] Are currencies and units explicit?
- [ ] Are null and zero semantics documented?
- [ ] Are derived columns reproducible?
- [ ] Are business rules approved?
- [ ] Are transformations report independent?

---

## 59. Load and History Review

- [ ] Is incremental logic deterministic?
- [ ] Are deletes captured?
- [ ] Are loads idempotent?
- [ ] Can the table be rebuilt from Raw?
- [ ] Are late-arriving records supported?
- [ ] Are reversals and corrections preserved?
- [ ] Is history strategy documented?
- [ ] Are replay and backfill procedures defined?

---

## 60. Quality and Governance Review

- [ ] Are uniqueness and completeness rules implemented?
- [ ] Are reconciliation totals available?
- [ ] Are rejected records visible?
- [ ] Is data lineage complete?
- [ ] Is ownership assigned?
- [ ] Is sensitive data classified?
- [ ] Are service levels documented?
- [ ] Are schema changes monitored?
- [ ] Are downstream consumers registered?

---

# Part XI — Example: Raw Sales Tables to Silver Sales Invoice Line

## 61. Business Requirement

Create a reusable Silver table representing posted customer invoice lines.

---

## 62. Declared Grain

```text
One row per posted customer invoice line per legal entity.
```

---

## 63. Possible Raw Sources

```text
CustInvoiceTrans
CustInvoiceJour
SalesLine
SalesTable
InventDim
CustTable
InventTable
```

The exact source set must be confirmed against the deployed D365 version and business requirements.

---

## 64. Design Rules

1. Use the posted invoice-line table as the atomic driver.
2. Join invoice header data without changing line count.
3. Include `DATAAREAID` in company-specific joins.
4. Retain source invoice-line `RECID`.
5. retain invoice number and source document references.
6. retain posting date and transaction date separately.
7. retain item, quantity and unit of measure.
8. retain transaction, accounting and reporting currency amounts where available.
9. retain inventory-dimension reference and resolved attributes.
10. represent credit notes and reversals using governed sign rules.
11. do not copy invoice-header totals onto every line.
12. reconcile line totals to posted invoice controls.

---

## 65. Example Silver Columns

```text
SalesInvoiceLineKey
LegalEntityCode
InvoiceNumber
InvoiceLineNumber
InvoiceLineSourceRecId
SalesOrderNumber
SalesOrderLineNumber
InvoiceDate
AccountingDate
CustomerAccount
ItemNumber
InventoryDimensionId
SiteCode
WarehouseCode
BatchNumber
SerialNumber
InvoicedQuantity
SalesUnitCode
TransactionCurrencyCode
GrossAmountTransactionCurrency
DiscountAmountTransactionCurrency
NetAmountTransactionCurrency
AccountingCurrencyCode
NetAmountAccountingCurrency
CostAmountAccountingCurrency
IsCreditNote
IsReversal
SourceModifiedDateTime
LoadBatchId
DataQualityStatus
```

---

## 66. Required Tests

```text
1. Unique by legal entity and source invoice-line identifier.
2. No null legal entity.
3. No null invoice identifier for posted lines.
4. Quantity reconciles to Raw after approved exclusions.
5. Accounting-currency amount reconciles by legal entity and posting date.
6. Header join does not increase line count.
7. Inventory-dimension join does not increase line count.
8. Incremental load matches full rebuild.
9. Credit-note sign treatment matches finance-approved rules.
10. Deleted or corrected records are traceable.
```

---

# Part XII — Anti-Patterns

## 67. Anti-Patterns to Avoid

### Creating Silver as a renamed Raw copy

This adds little enterprise value.

### Using `SELECT DISTINCT` to hide duplicates

This conceals grain or join errors.

### Building one Silver table per Power BI report

This creates duplication and inconsistent business definitions.

### Flattening every source table into one wide table

This mixes grains and increases maintenance cost.

### Using one amount column without currency context

This makes aggregation unsafe.

### Removing source keys

This destroys traceability.

### Overwriting posted financial transactions

This breaks auditability.

### Ignoring legal entity

This creates cross-company duplication.

### Treating ModifiedDateTime as guaranteed CDC

This may miss deletes or late changes.

### Deriving dimensions and facts before grain is approved

This propagates structural errors into every downstream model.

---

# 68. Final Enterprise Standard

The Raw-to-Silver process must produce tables that are:

```text
Grain-defined
Source-traceable
Business-aligned
Legally scoped
Deterministically keyed
Deduplicated through explicit rules
Currency and unit aware
History aware
Deletion aware
Incrementally maintainable
Idempotent
Reconciled
Quality controlled
Secure
Reusable
Independent of any single report
```

The governing rule is:

> **Define what one row represents, prove that the data conforms to that definition, and preserve enough source context to reproduce, reconcile and audit every transformation.**

D365 data entities may be consulted as metadata or for validation where useful, but the Silver-layer design in this standard is based on physical Raw D365 tables, approved table relationships, source metadata, and business-process requirements.
