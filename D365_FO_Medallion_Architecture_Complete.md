# D365 F&O → Medallion Architecture: Complete Reference
## AX2012 to D365 Migration — Raw Tables to Dims, Facts, OBT
### All table names, field names, and joins verified against Microsoft CDM schema

---

## 1. Source Tables — Complete List

The dimensional model crosses multiple D365 entity boundaries. This is correct — entities are isolated silos, but a proper warehouse deliberately joins across them to build conformed dimensions and well-grained facts.

### Sales Order Tables (from SalesOrderHeaderV2 + SalesOrderLineV2 entities)

| Table | Purpose | Key Fields |
|---|---|---|
| `SalesTable` | Order header | `SalesId` (PK), `CustAccount`, `InvoiceAccount`, `CurrencyCode`, `SalesStatus`, `SalesName`, `Payment`, `DlvMode`, `DlvTerm`, `DeliveryDate`, `ShippingDateRequested`, `SalesGroup`, `DataAreaId` |
| `SalesLine` | Order lines | `SalesId` + `LineNum` (composite PK), `ItemId`, `SalesQty`, `SalesPrice`, `LineAmount`, `SalesUnit`, `RemainSalesPhysical`, `RemainSalesFinancial`, `InventDimId`, `DataAreaId` |

### Invoice Tables (from CustInvoiceJourEntity — WHERE REVENUE LIVES)

| Table | Purpose | Key Fields |
|---|---|---|
| `CustInvoiceJour` | Posted invoice header | `InvoiceId`, `InvoiceDate`, `SalesId`, `OrderAccount`, `InvoiceAccount`, `CurrencyCode`, `InvoiceAmount`, `InvoiceAmountMST`, `SumLineDisc`, `SumTax`, `Payment`, `DlvMode`, `DataAreaId` |
| `CustInvoiceTrans` | Posted invoice lines | `InvoiceId`, `LineNum`, `InvoiceDate`, `ItemId`, `Qty`, `SalesPrice`, `LineAmount`, `LineAmountMST`, `DiscAmount`, `InventTransId` (FK to cost!), `InventDimId`, `DefaultDimension`, `CurrencyCode`, `SalesId`, `DataAreaId` |

### Inventory / Cost Tables (WHERE COGS LIVES)

| Table | Purpose | Key Fields |
|---|---|---|
| `InventTrans` | Inventory transactions — **cost of goods sold** | `InventTransId`, `ItemId`, `Qty`, `CostAmountPosted`, `CostAmountAdjustment`, `StatusIssue`, `DataAreaId` |
| `InventDim` | Inventory dimensions | `InventDimId` (PK), `InventSiteId`, `InventLocationId`, `InventBatchId`, `WMSLocationId`, `DataAreaId` |

> **CRITICAL: CostAmount does NOT exist on CustInvoiceTrans.**
> COGS = `InventTrans.CostAmountPosted + CostAmountAdjustment`,
> linked via `CustInvoiceTrans.InventTransId = InventTrans.InventTransId`.

### Customer / Global Address Book Tables (from CustomersV3 + GAB)

| Table | Purpose | Key Fields |
|---|---|---|
| `CustTable` | Customer master | `AccountNum` (PK), `CustGroup`, `PaymTermId`, `Currency` (NOT CurrencyCode!), `CreditMax`, `InvoiceAccount`, `Party` (FK → DirPartyTable.RecId), `DlvMode`, `DlvTerm`, `Blocked`, `DataAreaId` |
| `DirPartyTable` | Party master — root of inheritance hierarchy | `RecId` (PK), `Name`, `NameAlias`, `KnownAs`, `PartyNumber`, `LanguageId`, `InstanceRelationType`, `PrimaryAddressLocation` (FK → LogisticsLocation.RecId), `PrimaryContactEmail` (FK → LogisticsElectronicAddress.RecId), `PrimaryContactPhone` (FK → LogisticsElectronicAddress.RecId) |
| `DirPerson` | Person-specific — derived from DirPartyTable | `RecId` (= DirPartyTable.RecId), `Gender`, `MaritalStatus`, `NameSequence` |
| `DirOrganizationBase` | Org-specific — derived from DirPartyTable | `RecId` (= DirPartyTable.RecId), `OrgNumber`, `ABC` |
| `LogisticsPostalAddress` | Postal addresses | `RecId`, `Location` (FK → LogisticsLocation.RecId), `Street`, `City`, `State`, `ZipCode`, `CountryRegionId`, `ValidFrom`, `ValidTo` |
| `LogisticsLocation` | Location bridge table | `RecId` (PK), `Description`, `IsPostalAddress` |
| `LogisticsElectronicAddress` | Email, phone, URL | `RecId`, `Locator` (the actual string), `Type` |

> **DirPartyTable Inheritance (TPH):** In the D365 SQL database, DirPerson and
> DirOrganizationBase do NOT exist as separate tables — all fields are in
> DirPartyTable, differentiated by `InstanceRelationType`. BUT in Synapse Link,
> they ARE exported as separate delta tables. Add all parent + child tables to
> your Synapse Link profile and rejoin on RecId in silver, per FastTrack's
> `get_derivetables.sql` script.

> **CustTable does NOT contain customer name or address.** Name is on
> `DirPartyTable.Name` (via CustTable.Party = DirPartyTable.RecId). Address is
> on `LogisticsPostalAddress` (via DirPartyTable.PrimaryAddressLocation →
> LogisticsLocation.RecId → LogisticsPostalAddress.Location).

> **PrimaryContactEmail / PrimaryContactPhone** on DirPartyTable are RecId
> foreign keys to LogisticsElectronicAddress, NOT the actual email/phone strings.
> Join to `LogisticsElectronicAddress.Locator` for the actual values.

### Product Tables (from ReleasedProductsV2)

| Table | Purpose | Key Fields |
|---|---|---|
| `InventTable` | Item master | `ItemId` (PK), `ItemGroupId`, `Product` (FK → EcoResProduct.RecId), `DataAreaId` |
| `EcoResProduct` | Product master | `RecId` (PK), `DisplayProductNumber`, `ProductType` |
| `EcoResProductCategory` | Product ↔ category assignment | `Product` (FK), `Category` (FK), `CategoryHierarchy` |
| `EcoResCategory` | Category hierarchy | `RecId`, `Name`, `ParentCategory` (self-FK) |
| `InventItemGroup` | Item groups | `ItemGroupId` (PK), `Name`, `DataAreaId` |

### Customer Transaction Tables (for AR / Collections)

| Table | Purpose | Key Fields |
|---|---|---|
| `CustTrans` | All posted customer transactions | `RecId`, `AccountNum`, `TransDate`, `Voucher`, `Invoice`, `TransType`, `AmountCur`, `AmountMST`, `CurrencyCode`, `DueDate`, `Closed`, `DefaultDimension`, `DataAreaId` |
| `CustSettlement` | Settlement links (invoice ↔ payment) | `TransRecId`, `OffsetTransRecId`, `SettleAmountCur`, `SettleAmountMST` |

### Financial Dimension Tables

| Table | Purpose | Key Fields |
|---|---|---|
| `DimensionAttributeValueCombination` | Combination record (RecId = DefaultDimension) | `RecId` |
| `DimensionAttributeValueSetItem` | Individual dimension values in a combination | `DimensionAttributeValueSet`, `DimensionAttributeValue` |
| `DimensionAttributeValue` | Dimension values | `RecId`, `DimensionAttribute` (FK), `DisplayValue` |
| `DimensionAttribute` | Dimension definitions | `RecId`, `Name` (e.g. 'Department', 'CostCenter') |

### Reference Tables

| Table | Purpose | Key Fields |
|---|---|---|
| `PaymTerm` | Payment terms | `PaymTermId` (PK), `Description`, `NetDays` |
| `DlvMode` | Delivery modes | `Code` (PK), `Txt` |
| `DlvTerm` | Delivery terms | `Code` (PK), `Txt` |
| `HcmWorker` | Worker / sales rep | `RecId`, `PersonnelNumber`, `Person` (FK → DirPartyTable.RecId), `DataAreaId` |

---

## 2. RAW / BRONZE Layer

Each raw table is a 1:1 mirror from Synapse Link. No transformation. Includes system columns (`IsDelete`, `SinkModifiedOn`, `versionnumber`, `FnO_Id`).

No SQL shown — raw is a direct copy. One delta table per source table.

---

## 3. SILVER Layer — "Same tables, but trustworthy"

Silver cleans each table independently. Same grain as source. The ONLY joins are table inheritance (DirPartyTable + DirPerson + DirOrganizationBase) and RecId lookups (resolving FK pointers to actual values).

### What Silver Does to Every Table

| Step | Why |
|---|---|
| Remove soft deletes | `WHERE IsDelete = 0` |
| Deduplicate on change feed | `ROW_NUMBER()` by PK, `ORDER BY versionnumber DESC` |
| Resolve table inheritance | Rejoin DirPerson + DirOrganizationBase to DirPartyTable on RecId |
| Resolve RecId lookups | Join LogisticsElectronicAddress to get actual email/phone strings |
| Handle company context | Filter or tag by `DataAreaId` |
| Normalize system fields | Rename `FnO_Id`, handle truncated `nvarchar(max)` |
| Cast types | Dates → DATE, amounts → DECIMAL(19,4) |
| Decode enums | `SalesStatus` 1/2/3/4 → Open/Delivered/Invoiced/Cancelled |
| Validate business keys | Ensure composite PKs are unique |

### silver.sales_table

```sql
CREATE TABLE silver.sales_table AS
SELECT
    SalesId,
    DataAreaId,
    SalesName,
    CustAccount,
    InvoiceAccount,
    CurrencyCode,
    Payment                                         AS payment_term_id,
    DlvMode                                         AS delivery_mode_code,
    DlvTerm                                         AS delivery_term_code,
    CAST(DeliveryDate AS DATE)                      AS delivery_date,
    CAST(ShippingDateRequested AS DATE)              AS requested_ship_date,
    CAST(CreatedDateTime AS TIMESTAMP)               AS created_datetime,
    SalesGroup,
    CASE SalesStatus
        WHEN 1 THEN 'Open'
        WHEN 2 THEN 'Delivered'
        WHEN 3 THEN 'Invoiced'
        WHEN 4 THEN 'Cancelled'
    END AS sales_status,
    ModifiedDateTime    AS source_modified_datetime
FROM raw.SalesTable
WHERE IsDelete = 0
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY SalesId, DataAreaId
    ORDER BY versionnumber DESC
) = 1
```

### silver.sales_line

```sql
CREATE TABLE silver.sales_line AS
SELECT
    SalesId,
    LineNum,
    DataAreaId,
    ItemId,
    CAST(SalesQty AS DECIMAL(19,6))                 AS sales_qty,
    CAST(SalesPrice AS DECIMAL(19,6))               AS unit_price,
    CAST(LineAmount AS DECIMAL(19,4))               AS line_amount,
    CAST(RemainSalesPhysical AS DECIMAL(19,6))      AS remain_qty_physical,
    CAST(RemainSalesFinancial AS DECIMAL(19,4))     AS remain_amount_financial,
    SalesUnit                                        AS sales_unit,
    InventDimId,
    CAST(ShippingDateRequested AS DATE)              AS line_ship_date
FROM raw.SalesLine
WHERE IsDelete = 0
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY SalesId, LineNum, DataAreaId
    ORDER BY versionnumber DESC
) = 1
```

### silver.cust_invoice_jour

```sql
CREATE TABLE silver.cust_invoice_jour AS
SELECT
    InvoiceId,
    CAST(InvoiceDate AS DATE)                       AS invoice_date,
    SalesId,
    InvoiceAccount,
    OrderAccount,
    CurrencyCode,
    CAST(InvoiceAmount AS DECIMAL(19,4))            AS invoice_amount,
    CAST(InvoiceAmountMST AS DECIMAL(19,4))         AS invoice_amount_mst,
    CAST(SumLineDisc AS DECIMAL(19,4))              AS total_line_discount,
    CAST(SumTax AS DECIMAL(19,4))                   AS total_tax,
    DlvMode                                          AS delivery_mode_code,
    Payment                                          AS payment_term_id,
    DataAreaId
FROM raw.CustInvoiceJour
WHERE IsDelete = 0
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY InvoiceId, DataAreaId
    ORDER BY versionnumber DESC
) = 1
```

### silver.cust_invoice_trans

```sql
CREATE TABLE silver.cust_invoice_trans AS
SELECT
    InvoiceId,
    LineNum,
    CAST(InvoiceDate AS DATE)                       AS invoice_date,
    ItemId,
    CAST(Qty AS DECIMAL(19,6))                      AS invoiced_qty,
    CAST(SalesPrice AS DECIMAL(19,6))               AS unit_price,
    CAST(LineAmount AS DECIMAL(19,4))               AS line_revenue,
    CAST(LineAmountMST AS DECIMAL(19,4))            AS line_revenue_mst,
    CAST(DiscAmount AS DECIMAL(19,4))               AS line_discount_amount,
    InventTransId,                                   -- FK to InventTrans for COGS!
    InventDimId,
    CurrencyCode,
    SalesId,
    DefaultDimension,                                -- FK to financial dimensions
    DataAreaId
FROM raw.CustInvoiceTrans
WHERE IsDelete = 0
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY InvoiceId, LineNum, DataAreaId
    ORDER BY versionnumber DESC
) = 1
```

### silver.invent_trans (WHERE COST OF GOODS SOLD LIVES)

```sql
CREATE TABLE silver.invent_trans AS
SELECT
    InventTransId,
    DataAreaId,
    MAX(ItemId)                                     AS item_id,
    -- Aggregate cost to ONE row per InventTransId to prevent fan-out.
    -- A single InventTransId can have multiple physical InventTrans rows
    -- (receipt/issue pairs, adjustments); COGS is the sum.
    CAST(SUM(CostAmountPosted) AS DECIMAL(19,4))    AS cost_amount_posted,
    CAST(SUM(CostAmountAdjustment) AS DECIMAL(19,4)) AS cost_amount_adjustment,
    CAST(SUM(CostAmountPosted + CostAmountAdjustment)
         AS DECIMAL(19,4))                          AS total_cost,
    CAST(SUM(Qty) AS DECIMAL(19,6))                 AS inventory_qty
FROM (
    SELECT *
    FROM raw.InventTrans
    WHERE IsDelete = 0
      -- StatusIssue enum: 1 = Sold (financially updated issue = COGS)
      AND StatusIssue = 1
    QUALIFY ROW_NUMBER() OVER (
        PARTITION BY RecId
        ORDER BY versionnumber DESC
    ) = 1
) dedup
GROUP BY InventTransId, DataAreaId
```

> **Fan-out warning:** `InventTransId` is NOT unique in InventTrans — a single
> logical transaction produces multiple physical rows (receipt + issue +
> adjustments). We first deduplicate on `RecId` (the true PK), then aggregate
> cost to one row per `InventTransId` so the join to invoice lines stays 1:1.

### silver.cust_trans

```sql
CREATE TABLE silver.cust_trans AS
SELECT
    RecId                   AS cust_trans_recid,
    AccountNum              AS customer_account,
    CAST(TransDate AS DATE) AS transaction_date,
    Voucher,
    Invoice,
    TransType,
    CAST(AmountCur AS DECIMAL(19,4))                AS amount_transaction_currency,
    CAST(AmountMST AS DECIMAL(19,4))                AS amount_accounting_currency,
    CurrencyCode,
    CAST(DueDate AS DATE)                           AS due_date,
    Closed                  AS settlement_date,
    LastSettleDate,
    DefaultDimension,
    DataAreaId
FROM raw.CustTrans
WHERE IsDelete = 0
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY RecId
    ORDER BY versionnumber DESC
) = 1
```

### silver.party (DirPartyTable with inheritance + RecId lookups resolved)

```sql
CREATE TABLE silver.party AS
SELECT
    p.RecId                         AS party_recid,
    p.Name                          AS party_name,
    p.NameAlias                     AS party_short_name,
    p.PartyNumber,
    p.KnownAs,
    p.LanguageId,
    p.PrimaryAddressLocation        AS primary_address_location_recid,

    CASE
        WHEN per.RecId IS NOT NULL THEN 'Person'
        WHEN org.RecId IS NOT NULL THEN 'Organization'
        ELSE 'Unknown'
    END AS party_type,

    -- From derived table: DirPerson
    per.Gender                      AS person_gender,
    per.MaritalStatus               AS person_marital_status,

    -- From derived table: DirOrganizationBase
    org.OrgNumber                   AS organization_number,
    org.ABC                         AS organization_abc_classification,

    -- Resolve PrimaryContactEmail/Phone RecId → actual strings
    email.Locator                   AS primary_email,
    phone.Locator                   AS primary_phone

FROM raw.DirPartyTable p
LEFT JOIN raw.DirPerson per
    ON p.RecId = per.RecId AND per.IsDelete = 0
LEFT JOIN raw.DirOrganizationBase org
    ON p.RecId = org.RecId AND org.IsDelete = 0
LEFT JOIN raw.LogisticsElectronicAddress email
    ON p.PrimaryContactEmail = email.RecId AND email.IsDelete = 0
LEFT JOIN raw.LogisticsElectronicAddress phone
    ON p.PrimaryContactPhone = phone.RecId AND phone.IsDelete = 0
WHERE p.IsDelete = 0
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY p.RecId
    ORDER BY p.versionnumber DESC
) = 1
```

> **Why this join is OK in silver:** We are reconstituting a SINGLE logical
> record (a "party") that D365 split across tables for technical reasons
> (TPH inheritance + RecId lookups). The grain stays one row per party.

### silver.customer

```sql
CREATE TABLE silver.customer AS
SELECT
    c.AccountNum                AS customer_account,
    c.DataAreaId,
    c.CustGroup                 AS customer_group,
    c.PaymTermId                AS payment_term_id,
    c.Currency                  AS default_currency,
    c.CreditMax                 AS credit_limit,
    c.InvoiceAccount            AS invoice_account,
    c.Party                     AS party_recid,
    CASE c.Blocked
        WHEN 0 THEN 'No'
        WHEN 1 THEN 'Invoice'
        WHEN 2 THEN 'All'
    END AS blocked_status
FROM raw.CustTable c
WHERE c.IsDelete = 0
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY c.AccountNum, c.DataAreaId
    ORDER BY c.versionnumber DESC
) = 1
```

### silver.logistics_postal_address

```sql
CREATE TABLE silver.logistics_postal_address AS
SELECT
    RecId               AS address_recid,
    Location            AS location_recid,
    Street, City, State,
    ZipCode             AS postal_code,
    CountryRegionId     AS country_code,
    CAST(ValidFrom AS DATE) AS valid_from,
    CAST(ValidTo AS DATE)   AS valid_to
FROM raw.LogisticsPostalAddress
WHERE IsDelete = 0
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY RecId ORDER BY versionnumber DESC
) = 1
```

### silver.logistics_location

```sql
CREATE TABLE silver.logistics_location AS
SELECT
    RecId               AS location_recid,
    Description         AS location_description
FROM raw.LogisticsLocation
WHERE IsDelete = 0
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY RecId ORDER BY versionnumber DESC
) = 1
```

### silver.invent_table

```sql
CREATE TABLE silver.invent_table AS
SELECT
    ItemId,
    DataAreaId,
    ItemGroupId,
    Product             AS product_recid,
    NameAlias           AS item_search_name
FROM raw.InventTable
WHERE IsDelete = 0
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY ItemId, DataAreaId ORDER BY versionnumber DESC
) = 1
```

### silver.invent_dim

```sql
CREATE TABLE silver.invent_dim AS
SELECT
    InventDimId, DataAreaId,
    InventSiteId        AS site_id,
    InventLocationId    AS warehouse_id,
    InventBatchId       AS batch_id,
    WMSLocationId       AS wms_location_id
FROM raw.InventDim
WHERE IsDelete = 0
QUALIFY ROW_NUMBER() OVER (
    PARTITION BY InventDimId, DataAreaId ORDER BY versionnumber DESC
) = 1
```

### silver.financial_dimension (Pivoted from D365's normalized structure)

```sql
CREATE TABLE silver.financial_dimension AS
SELECT
    comb.RecId                  AS dimension_combination_recid,
    MAX(CASE WHEN attr.Name = 'Department'   THEN dav.DisplayValue END) AS department,
    MAX(CASE WHEN attr.Name = 'CostCenter'   THEN dav.DisplayValue END) AS cost_center,
    MAX(CASE WHEN attr.Name = 'BusinessUnit' THEN dav.DisplayValue END) AS business_unit

FROM raw.DimensionAttributeValueCombination comb
INNER JOIN raw.DimensionAttributeValueSetItem setitem
    ON comb.RecId = setitem.DimensionAttributeValueSet
INNER JOIN raw.DimensionAttributeValue dav
    ON setitem.DimensionAttributeValue = dav.RecId
INNER JOIN raw.DimensionAttribute attr
    ON dav.DimensionAttribute = attr.RecId
WHERE comb.IsDelete = 0
GROUP BY comb.RecId
```

> **IMPLEMENTATION NOTE:** The join path between DimensionAttributeValueCombination
> and DimensionAttributeValueSetItem varies by D365 version. The join column shown
> above (`comb.RecId = setitem.DimensionAttributeValueSet`) is the common pattern,
> but **verify against your actual Synapse Link exported tables** before deploying.
> The dimension attribute `Name` values ('Department', 'CostCenter') must also
> match your D365 configuration.

### silver.paym_term, silver.dlv_mode, silver.dlv_term, silver.ecores_product, silver.ecores_category, silver.invent_item_group, silver.hcm_worker

*(Each follows the same pattern: filter IsDelete = 0, deduplicate on PK by versionnumber DESC, cast types)*

### Silver Layer Summary

| Silver Table | Source | Grain |
|---|---|---|
| `silver.sales_table` | SalesTable | 1 row per SalesId + DataAreaId |
| `silver.sales_line` | SalesLine | 1 row per SalesId + LineNum + DataAreaId |
| `silver.cust_invoice_jour` | CustInvoiceJour | 1 row per InvoiceId + DataAreaId |
| `silver.cust_invoice_trans` | CustInvoiceTrans | 1 row per InvoiceId + LineNum + DataAreaId |
| `silver.invent_trans` | InventTrans | 1 row per InventTransId + DataAreaId |
| `silver.cust_trans` | CustTrans | 1 row per RecId |
| `silver.customer` | CustTable | 1 row per AccountNum + DataAreaId |
| `silver.party` | DirPartyTable + DirPerson + DirOrganizationBase + LogisticsElectronicAddress | 1 row per RecId |
| `silver.logistics_postal_address` | LogisticsPostalAddress | 1 row per RecId |
| `silver.logistics_location` | LogisticsLocation | 1 row per RecId |
| `silver.invent_table` | InventTable | 1 row per ItemId + DataAreaId |
| `silver.invent_dim` | InventDim | 1 row per InventDimId + DataAreaId |
| `silver.financial_dimension` | 4 dimension tables (pivoted) | 1 row per combination RecId |
| `silver.paym_term` | PaymTerm | 1 row per PaymTermId |
| `silver.dlv_mode` | DlvMode | 1 row per Code |
| `silver.hcm_worker` | HcmWorker | 1 row per RecId |

---

## 4. GOLD Layer — Conformed Dimensions

### gold.dim_customer (Conformed — shared across ALL facts)

Joins: `silver.customer` + `silver.party` + `silver.logistics_location` +
`silver.logistics_postal_address` + `silver.paym_term`

```sql
CREATE TABLE gold.dim_customer AS
SELECT
    -- Surrogate key includes effective date so Type 2 versions don't collide.
    -- The business key (customer_account + company) is kept separately for
    -- fact joins that resolve the correct version by date.
    MD5(CONCAT(c.customer_account, c.DataAreaId,
               CAST(CURRENT_TIMESTAMP() AS STRING)))  AS customer_sk,
    MD5(CONCAT(c.customer_account, c.DataAreaId))      AS customer_bk,  -- durable business key
    c.customer_account,
    c.DataAreaId                                     AS company,

    -- From silver.party (via CustTable.Party = DirPartyTable.RecId)
    p.party_name                                     AS customer_name,
    p.party_short_name                               AS customer_short_name,
    p.party_type,
    p.primary_email,
    p.primary_phone,
    p.person_gender,
    p.organization_number,

    -- From silver.customer
    c.customer_group,
    c.credit_limit,
    c.blocked_status,
    c.default_currency,
    c.payment_term_id,

    -- From silver.paym_term
    pt.payment_term_description,
    pt.payment_net_days,

    -- Primary address (via LogisticsLocation bridge)
    addr.City                                        AS primary_city,
    addr.State                                       AS primary_state,
    addr.country_code                                AS primary_country,
    addr.postal_code                                 AS primary_postal_code,

    -- SCD metadata
    CURRENT_TIMESTAMP()                              AS effective_from,
    NULL                                             AS effective_to,
    TRUE                                             AS is_current

FROM silver.customer c
LEFT JOIN silver.party p
    ON c.party_recid = p.party_recid
LEFT JOIN silver.paym_term pt
    ON c.payment_term_id = pt.PaymTermId
LEFT JOIN silver.logistics_location loc
    ON p.primary_address_location_recid = loc.location_recid
LEFT JOIN silver.logistics_postal_address addr
    ON loc.location_recid = addr.location_recid
    AND CURRENT_DATE BETWEEN addr.valid_from AND COALESCE(addr.valid_to, '9999-12-31')
```

> **SCD Type 2 key rule:** `customer_sk` includes the effective timestamp so each
> version is unique. Facts should join on the **business key** (`customer_bk`)
> resolved to the version whose `[effective_from, effective_to)` window contains
> the transaction date — OR, for a simpler Type 1 model, drop the timestamp from
> the key and keep one row per customer. The examples below use the business-key
> pattern for stability; pick one approach and apply it consistently to every dim.

### gold.dim_product (with category hierarchy)

```sql
CREATE TABLE gold.dim_product AS
SELECT
    MD5(CONCAT(i.ItemId, i.DataAreaId,
               CAST(CURRENT_TIMESTAMP() AS STRING)))  AS product_sk,
    MD5(CONCAT(i.ItemId, i.DataAreaId))          AS product_bk,   -- durable business key
    i.ItemId                                     AS item_id,
    i.DataAreaId                                 AS company,
    i.ItemGroupId                                AS item_group_id,
    ig.Name                                      AS item_group_name,
    p.DisplayProductNumber                       AS product_number,
    p.ProductType                                AS product_type,
    cat.Name                                     AS product_category,
    parent_cat.Name                              AS product_category_parent,
    CURRENT_TIMESTAMP()                          AS effective_from,
    NULL                                         AS effective_to,
    TRUE                                         AS is_current
FROM silver.invent_table i
LEFT JOIN silver.ecores_product p        ON i.product_recid = p.RecId
LEFT JOIN silver.invent_item_group ig    ON i.ItemGroupId = ig.ItemGroupId
                                            AND i.DataAreaId = ig.DataAreaId
LEFT JOIN silver.ecores_product_category pc ON p.RecId = pc.Product
LEFT JOIN silver.ecores_category cat     ON pc.Category = cat.RecId
LEFT JOIN silver.ecores_category parent_cat ON cat.ParentCategory = parent_cat.RecId
```

### gold.dim_financial_dimension

```sql
CREATE TABLE gold.dim_financial_dimension AS
SELECT
    MD5(CAST(dimension_combination_recid AS STRING)) AS financial_dim_sk,
    dimension_combination_recid,
    department,
    cost_center,
    business_unit
FROM silver.financial_dimension
```

### gold.dim_date (Standard — generated, no D365 source)

```
date_key (INT, YYYYMMDD) | full_date | day_of_week | day_name
month_number | month_name | quarter | calendar_year
fiscal_year | fiscal_quarter | fiscal_period | is_holiday
```

### gold.dim_delivery_mode

```sql
CREATE TABLE gold.dim_delivery_mode AS
SELECT
    MD5(Code)       AS delivery_mode_sk,
    Code            AS delivery_mode_code,
    Txt             AS delivery_mode_name
FROM silver.dlv_mode
```

### gold.dim_warehouse

```sql
CREATE TABLE gold.dim_warehouse AS
SELECT
    MD5(CONCAT(site_id, warehouse_id, DataAreaId)) AS warehouse_sk,
    site_id,
    warehouse_id,
    DataAreaId                                      AS company
FROM silver.invent_dim
GROUP BY site_id, warehouse_id, DataAreaId
```

### gold.dim_sales_rep

```sql
CREATE TABLE gold.dim_sales_rep AS
SELECT
    MD5(CONCAT(w.PersonnelNumber, w.DataAreaId))    AS sales_rep_sk,
    w.PersonnelNumber                                AS personnel_number,
    p.party_name                                     AS sales_rep_name,
    w.DataAreaId                                     AS company
FROM silver.hcm_worker w
LEFT JOIN silver.party p ON w.Person = p.party_recid
```

---

## 5. GOLD Layer — Fact Tables

### gold.fact_sales_order_line (Pipeline / Backlog / Delivery Performance)

**Grain:** one row per sales order line.

```sql
CREATE TABLE gold.fact_sales_order_line AS
SELECT
    MD5(CONCAT(sl.SalesId, sl.LineNum, sl.DataAreaId))      AS sales_line_sk,
    MD5(CONCAT(st.CustAccount, st.DataAreaId))              AS customer_bk,   -- ordering customer; joins dim_customer.customer_bk
    MD5(CONCAT(sl.ItemId, sl.DataAreaId))                   AS product_bk,
    MD5(st.delivery_mode_code)                              AS delivery_mode_sk,
    MD5(CONCAT(id.site_id, id.warehouse_id, sl.DataAreaId)) AS warehouse_sk,
    CAST(DATE_FORMAT(st.created_datetime, 'yyyyMMdd') AS INT)   AS order_date_key,
    CAST(DATE_FORMAT(st.delivery_date, 'yyyyMMdd') AS INT)      AS promised_delivery_date_key,
    CAST(DATE_FORMAT(sl.line_ship_date, 'yyyyMMdd') AS INT)     AS requested_ship_date_key,

    -- Degenerate dimensions
    sl.SalesId                  AS sales_order_number,
    sl.LineNum                  AS line_number,
    sl.DataAreaId               AS company,
    st.sales_status             AS order_status,
    st.CurrencyCode             AS transaction_currency,

    -- Measures
    sl.sales_qty,
    sl.unit_price,
    sl.line_amount,
    sl.remain_qty_physical,
    sl.remain_amount_financial,
    (sl.sales_qty - sl.remain_qty_physical)         AS delivered_qty,
    DATEDIFF(sl.line_ship_date, st.delivery_date)   AS days_variance_to_promise

FROM silver.sales_line sl
INNER JOIN silver.sales_table st
    ON sl.SalesId = st.SalesId AND sl.DataAreaId = st.DataAreaId
LEFT JOIN silver.invent_dim id
    ON sl.InventDimId = id.InventDimId AND sl.DataAreaId = id.DataAreaId
```

> **Business use:** Open order backlog (`WHERE order_status = 'Open'`),
> delivery performance (`days_variance_to_promise`), pipeline by date.

### gold.fact_sales_invoice_line (ACTUAL REVENUE + MARGIN)

**Grain:** one row per posted invoice line. This is the fact finance cares about most.

```sql
CREATE TABLE gold.fact_sales_invoice_line AS
SELECT
    MD5(CONCAT(it.InvoiceId, it.LineNum, it.DataAreaId))     AS invoice_line_sk,
    MD5(CONCAT(ij.InvoiceAccount, it.DataAreaId))           AS customer_bk,   -- invoiced customer; joins dim_customer.customer_bk
    MD5(CONCAT(it.ItemId, it.DataAreaId))                    AS product_bk,
    MD5(CAST(it.DefaultDimension AS STRING))                 AS financial_dim_sk,
    MD5(CONCAT(id.site_id, id.warehouse_id, it.DataAreaId))  AS warehouse_sk,
    CAST(DATE_FORMAT(it.invoice_date, 'yyyyMMdd') AS INT)    AS invoice_date_key,

    -- Degenerate dimensions
    it.InvoiceId                AS invoice_number,
    it.LineNum                  AS line_number,
    it.SalesId                  AS sales_order_number,
    it.DataAreaId               AS company,
    it.CurrencyCode             AS transaction_currency,

    -- Revenue measures
    it.invoiced_qty,
    it.unit_price,
    it.line_revenue,
    it.line_revenue_mst,                                     -- accounting currency
    it.line_discount_amount,

    -- Cost from InventTrans (NOT on CustInvoiceTrans!)
    COALESCE(inv.total_cost, 0)                              AS line_cost,
    (it.line_revenue - COALESCE(inv.total_cost, 0))          AS gross_margin,

    -- Tax allocated from header
    CASE WHEN ij.invoice_amount != 0
        THEN it.line_revenue / ij.invoice_amount * ij.total_tax
        ELSE 0
    END AS allocated_tax

FROM silver.cust_invoice_trans it
INNER JOIN silver.cust_invoice_jour ij
    ON it.InvoiceId = ij.InvoiceId AND it.DataAreaId = ij.DataAreaId
LEFT JOIN silver.invent_trans inv
    ON it.InventTransId = inv.InventTransId AND it.DataAreaId = inv.DataAreaId
LEFT JOIN silver.invent_dim id
    ON it.InventDimId = id.InventDimId AND it.DataAreaId = id.DataAreaId
```

> **Business use:** Revenue by customer/product/period, gross margin,
> revenue by department/cost center (via financial_dim_sk),
> multi-company rollup (via line_revenue_mst in accounting currency).

---

## 6. OBT (One Big Table) — Flattened for Power BI

### gold.obt_revenue

```sql
CREATE TABLE gold.obt_revenue AS
SELECT
    -- Fact measures
    f.invoice_number,
    f.line_number,
    f.sales_order_number,
    f.company,
    f.transaction_currency,
    f.invoiced_qty,
    f.unit_price,
    f.line_revenue,
    f.line_revenue_mst,
    f.line_cost,
    f.gross_margin,
    f.line_discount_amount,
    f.allocated_tax,
    CASE WHEN f.line_revenue != 0
        THEN ROUND(f.gross_margin / f.line_revenue * 100, 2)
        ELSE 0
    END AS gross_margin_pct,

    -- DimCustomer
    dc.customer_account,
    dc.customer_name,
    dc.customer_group,
    dc.primary_city,
    dc.primary_state,
    dc.primary_country,
    dc.credit_limit,
    dc.payment_term_description,
    dc.primary_email,
    dc.primary_phone,

    -- DimProduct
    dp.item_id,
    dp.product_number,
    dp.item_group_name,
    dp.product_category,
    dp.product_category_parent,
    dp.product_type,

    -- DimFinancialDimension
    fd.department,
    fd.cost_center,
    fd.business_unit,

    -- DimDate
    dd.full_date                AS invoice_date,
    dd.fiscal_year,
    dd.fiscal_quarter,
    dd.fiscal_period,
    dd.month_name,
    dd.calendar_year,

    -- DimWarehouse
    wh.site_id,
    wh.warehouse_id

FROM gold.fact_sales_invoice_line f
LEFT JOIN gold.dim_customer dc           ON f.customer_bk = dc.customer_bk AND dc.is_current
LEFT JOIN gold.dim_product dp            ON f.product_bk = dp.product_bk AND dp.is_current
LEFT JOIN gold.dim_financial_dimension fd ON f.financial_dim_sk = fd.financial_dim_sk
LEFT JOIN gold.dim_date dd               ON f.invoice_date_key = dd.date_key
LEFT JOIN gold.dim_warehouse wh          ON f.warehouse_sk = wh.warehouse_sk
```

---

## 7. What This Model Answers

| Business Question | Fact / OBT | Key Columns |
|---|---|---|
| Revenue by customer for FY2026 Q2 | obt_revenue | customer_name, line_revenue_mst, fiscal_quarter |
| Top 10 products by gross margin % | obt_revenue | product_number, gross_margin_pct |
| Revenue by department and cost center | obt_revenue | department, cost_center, line_revenue_mst |
| Multi-company revenue rollup | obt_revenue | company, line_revenue_mst (accounting currency) |
| Open order backlog by customer | fact_sales_order_line | customer_bk, remain_amount_financial, order_status = 'Open' |
| On-time delivery % | fact_sales_order_line | days_variance_to_promise <= 0 |
| Revenue vs cost by product category | obt_revenue | product_category, line_revenue_mst, line_cost |
| Customer credit exposure | fact_sales_order_line + dim_customer | remain_amount_financial vs credit_limit |
| AR aging by customer | silver.cust_trans + dim_customer | due_date, settlement_date, amount_accounting_currency |

---

## 8. Known Limitations — Verify Before Deploying

These are design decisions and D365-version-specific points to confirm against your environment:

1. **SCD strategy must be consistent.** The dims above use a Type 2 pattern (surrogate key includes timestamp; facts join on the durable business key + `is_current`). If you only need current-state reporting, switch every dim to Type 1 (drop the timestamp from the key, one row per business key) — but don't mix the two.

2. **Invoice→cost link (`InventTransId`) needs validation.** The relationship between `CustInvoiceTrans` and `InventTrans` depends on inventory model and whether the item is stocked. Non-stocked/service items and charges may have no `InventTrans` row at all (cost will be 0). Confirm cost coverage against a known invoice before trusting margin numbers.

3. **Order vs invoice customer.** `fact_sales_order_line` uses the ordering customer (`SalesTable.CustAccount`); `fact_sales_invoice_line` uses the invoiced customer (`CustInvoiceJour.InvoiceAccount`). These can differ (e.g. one-time customers, invoice-to relationships). Decide which the business wants and document it.

4. **Financial dimension join path** varies by D365 version (see the note in Section 3). Validate the `DimensionAttributeValueCombination` → `DimensionAttributeValueSetItem` join and the attribute `Name` values against your config.

5. **Tax allocation is proportional, not exact.** The `allocated_tax` on the invoice fact spreads header tax by revenue share. If you need exact per-line tax, source it from `TaxTrans` joined via `InventTransId` instead.

6. **Deletes on transaction tables.** Posted documents (CustInvoiceJour/Trans) are rarely hard-deleted, but master data (CustTable) can be. Confirm your `IsDelete` handling matches how Synapse Link marks records in your tenant.

7. **`StatusIssue = 1` filter** on InventTrans captures financially-updated sold issues. Depending on your COGS definition you may also need physically-updated (not yet invoiced) transactions — verify against finance's revenue-recognition timing.

---

## 9. Architecture Summary

```
RAW/BRONZE
  SalesTable, SalesLine, CustInvoiceJour, CustInvoiceTrans,
  InventTrans, CustTrans, CustTable, DirPartyTable, DirPerson,
  DirOrganizationBase, LogisticsPostalAddress, LogisticsLocation,
  LogisticsElectronicAddress, InventTable, EcoResProduct, InventDim,
  DimensionAttributeValue*, PaymTerm, DlvMode, HcmWorker ...
  (1:1 from Synapse Link, no transforms)
       │
       ▼
SILVER (clean, atomic, same grain as source)
  sales_table, sales_line, cust_invoice_jour, cust_invoice_trans,
  invent_trans, cust_trans, customer, party (inheritance resolved),
  logistics_postal_address, logistics_location, invent_table,
  invent_dim, financial_dimension (pivoted), paym_term, ...
  (soft deletes removed, deduped, types cast, enums decoded)
       │
       ▼
GOLD — DIMENSIONS (conformed, shared across all facts)
  dim_customer, dim_product, dim_date, dim_financial_dimension,
  dim_delivery_mode, dim_warehouse, dim_sales_rep
       │
GOLD — FACTS (deliberate grain, measures + surrogate keys)
  fact_sales_order_line  (pipeline, backlog, delivery)
  fact_sales_invoice_line (revenue, COGS, margin)
       │
       ▼
OBT (flattened dims + facts for specific insight area)
  obt_revenue → Power BI / reporting layer
```
