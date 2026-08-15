# 1. Source Data Dictionary

**Scope:** the 21 TPC-DI source files this platform ingests, plus 2
operational control feeds. This is the system-of-record definition for
"what does the vendor actually send us" — every downstream decision in
`02_bronze_design.md` through `07_governance.md` traces back to a row in
this document. Get this wrong and every later layer inherits the error;
this is why it's document §1, not an appendix.

This is taken from the official [TPC-DI benchmark specification](https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-di_v1.1.0.pdf)
(v1.1.0, §2.2.2.x), but the field-level structure below is verified
against real sample data, not just the spec prose — several corrections
below exist because the spec text and the actual file disagreed.

> [!NOTE]
> **Terminology — "quasi-CDC."** The spec tags `HoldingHistory`,
> `WatchHistory`, `DailyMarket`, and `CashTransaction` as CDC sources.
> A closer look shows they don't actually function as CDC in the
> classical sense (insert **and** update event stream): the `CDC_FLAG`
> is always `'I'`, never `'U'`, and rows are never revised once written
> — it's an **insert-only event log** wearing CDC-shaped column names.
> This platform calls that pattern **quasi-CDC** throughout, to keep it
> distinct from the two sources that carry genuine update semantics
> (`Account.txt`, `Customer.txt`, `Trade.txt`). The distinction drives a
> real modeling decision downstream — see `04_silver.md` §4, "Two dedup
> shapes, one macro."

## Table of contents
- [1.1 Batch Scope Matrix](#11-batch-scope-matrix)
- [1.2 Source Definitions](#12-source-definitions)
- [1.3 Control & Audit Feeds](#13-control--audit-feeds)

---

## 1.1 Batch Scope Matrix

**Theory:** before modeling a single column, a senior engineer's first
question is "what's the temporal shape of this feed" — one-time load,
full re-extract, or true incremental delta. That answer, not the file
format, is what determines the correct load strategy (`02_bronze_design.md`
§2 defines five archetypes directly from this matrix).

| File | Batch1 (Historical) | Batch2/3 (Incremental) | CDC fields present? |
|---|---|---|---|
| `CustomerMgmt.xml` | ✅ | ❌ (replaced by `Account.txt`/`Customer.txt`) | N/A — `ActionType` instead |
| `Account.txt` | ❌ | ✅ | **Real CDC** |
| `Customer.txt` | ❌ | ✅ | **Real CDC** |
| `HR.csv` | ✅ | ❌ | No |
| `Prospect.csv` | ✅ | ✅ (full re-extract each batch, not incremental) | No |
| `FINWIRE` (quarterly) | ✅ | ❌ | No |
| `Trade.txt` | ✅ | ✅ | **Real CDC** |
| `TradeHistory.txt` | ✅ | ❌ (Historical Load only, per spec) | No |
| `HoldingHistory.txt` | ✅ | ✅ | No in Batch1, yes in Batch2/3 — **quasi-CDC** |
| `CashTransaction.txt` | ✅ | ✅ | No in Batch1, yes in Batch2/3 — **quasi-CDC** |
| `WatchHistory.txt` | ✅ | ✅ | No in Batch1, yes in Batch2/3 — **quasi-CDC** |
| `DailyMarket.txt` | ✅ | ✅ | No in Batch1, yes in Batch2/3 — **quasi-CDC** |
| `Date.txt` / `Time.txt` / `StatusType.txt` / `TaxRate.txt` / `Industry.txt` / `TradeType.txt` | ✅ | ❌ | No |
| `BatchDate.txt` | ✅ | ✅ | N/A — control file |
| `*_audit.csv` | ✅ | ✅ | N/A |

**The pattern to internalize:** several files (`Trade`, `HoldingHistory`,
`WatchHistory`, `DailyMarket`) do not have a **single fixed schema** —
column count differs between Batch1 and Batch2/3, because the spec
states plainly that CDC fields "are not present in the data set used by
the Historical Load." This is the single fact that forces the
schema-shift handling documented in `02_bronze_design.md` (Archetype B)
and `03_ingestion.md` §3 (`_split_cdc()`).


---

## 1.2 Source Definitions

### 1.2.1 `CustomerMgmt.xml`

**Format:** nested XML · **Scope:** Batch1 (Historical) only

An `Action` element wraps each event (`ActionType`, `ActionTS`). Inside,
a `Customer` element carries `C_ID` (required), `C_TAX_ID`, `C_GNDR`,
`C_TIER`, `C_DOB`, and contains:
- an optional `TaxInfo` block (`C_LCL_TX_ID`, `C_NAT_TX_ID`)
- zero or more `Account` elements (`CA_ID` required, `CA_TAX_ST`, plus
  `CA_B_ID`/`CA_NAME`)

```xml
<Action ActionType="NEW" ActionTS="2015-03-03T08:47:33">
  <Customer C_ID="1500000" C_TAX_ID="123-45-6789" C_GNDR="M" C_TIER="2" C_DOB="1975-06-01">
    <TaxInfo>
      <C_LCL_TX_ID>US1</C_LCL_TX_ID>
      <C_NAT_TX_ID>US2</C_NAT_TX_ID>
    </TaxInfo>
    <Account CA_ID="5000001" CA_TAX_ST="1">
      <CA_B_ID>1284</CA_B_ID>
      <CA_NAME>Growth Account</CA_NAME>
    </Account>
  </Customer>
</Action>
```

→ Parsed by `xml_loader.py` (`03_ingestion.md` §3.3). Structural
outlier — see Archetype D, `02_bronze_design.md` §2.

### 1.2.2 `Account.txt`

**Format:** pipe-delimited · **Scope:** Batch2/3 (Incremental) only

| Column | Description |
|---|---|
| `CDC_FLAG` | `I` / `U` |
| `CDC_DSN` | Sequence number |
| `CA_ID` | Account ID |
| `CA_B_ID` | Managing broker ID |
| `CA_C_ID` | Owning customer ID |
| `CA_NAME` | Account name |
| `CA_TAX_ST` | Tax status code |
| `CA_ST_ID` | Status code |

**Row example:** `I|8214563|20469|1284|10284|WSrAJPnvZzbENxGPc...|0|ACTV`

### 1.2.3 `Customer.txt`

**Format:** pipe-delimited · **Scope:** Batch2/3 (Incremental) only

CDC metadata followed by the complete customer record. Unlike
`CustomerMgmt.xml`, each phone number is flattened into four separate
fields (country code, area code, local number, extension) — a deliberate
naming/shape alignment used later to unify the two sources (see
`04_silver.md` ADR-001).

| Column | Description |
|---|---|
| `CDC_FLAG` | `I` / `U` |
| `CDC_DSN` | Sequence number |
| `C_ID` | Customer identifier |
| `C_TAX_ID` | Government tax identifier |
| `C_ST_ID` | Customer status code |
| `C_L_NAME` / `C_F_NAME` / `C_M_NAME` | Last / first / middle name |
| `C_GNDR` | Gender |
| `C_TIER` | Customer tier |
| `C_DOB` | Date of birth |
| `C_ADLINE1` / `C_ADLINE2` | Address lines |
| `C_ZIPCODE` / `C_CITY` / `C_STATE_PROV` / `C_CTRY` | Address components |
| `C_CTRY_n` / `C_AREA_n` / `C_LOCAL_n` / `C_EXT_n` (n=1..3) | Three flattened phone numbers |
| `C_PRIM_EMAIL` / `C_ALT_EMAIL` | Email addresses |
| `C_LCL_TX_ID` / `C_NAT_TX_ID` | Tax jurisdiction codes |

**Row example (verified Batch3 sample):**
```text
I|6455|4739|016-32-5107|ACTV|Moncur|Vittorio||M|3|1983-06-21|19452 Bryant Irvin West||H2E 1V8|Paterson|TX|United States of America|||821-2946||||205-8612|06614|1|968|027-5679||Vittorio.Moncur@farce.de||MD4|MT5
```

### 1.2.4 `HR.csv`

**Format:** comma-delimited · **Scope:** Batch1 only, ordered by `EmployeeID`

| Column | Type | Nullability |
|---|---|---|
| `EmployeeID` | IDENT_T | not null |
| `ManagerID` | IDENT_T | not null |
| `EmployeeFirstName` / `EmployeeLastName` | CHAR(30) | not null |
| `EmployeeMI` | CHAR(1) | |
| `EmployeeJobCode` | NUM(3) | |
| `EmployeeBranch` | CHAR(30) | |
| `EmployeeOffice` | CHAR(10) | |
| `EmployeePhone` | CHAR(14) | |

**Row example:** `140501,140102,John,Smith,R,314,Chicago Branch,7B,(312) 555-0142`

### 1.2.5 `Prospect.csv`

**Format:** comma-delimited · **Scope:** Batch1 & Batch2/3 — **full
re-extract every batch, no CDC**

| Column | Type | Restriction |
|---|---|---|
| `AgencyID` | CHAR(30) | not null — unique agency identifier |
| `LastName` / `FirstName` | CHAR(30) | not null |
| `MiddleInitial` | CHAR(1) | |
| `Gender` | CHAR(1) | `M`/`F`/`U` |
| `AddressLine1` / `AddressLine2` / `PostalCode` | CHAR | |
| `City` / `State` | CHAR | not null |
| `Country` / `Phone` | CHAR | |
| `Income` | NUM(9) | annual income |
| `NumberCars` / `NumberChildren` | NUM(2) | |
| `MaritalStatus` | CHAR(1) | `S`/`M`/`D`/`W`/`U` |
| `Age` | NUM(3) | |
| `CreditRating` | NUM(4) | |
| `OwnOrRentFlag` | CHAR(1) | `O`/`R`/`U` — **corrected from spec's "OwnHome"** |
| `Employer` | CHAR(30) | |
| `NumberCreditCards` | NUM(2) | **corrected from spec's "CreditCard"** |
| `NetWorth` | NUM(12) | |

**Row example:** `PEL0,PELLAND,Netti,,F,21847 Olympia Street,,T6B 1I1,Fairbanks,MA,United States of America,1-712-522-6088,368776,,3,W,20,760,O,Brink's,,1058868`

**Load implication:** no reliable key uniqueness within a single batch —
Archetype C (snapshot dimension) in `02_bronze_design.md` §2.

### 1.2.6 `FINWIRE`

**Format:** fixed-width, 3 interleaved record types (`CMP`/`SEC`/`FIN`)
· **Scope:** Batch1 only, quarterly

Every line starts with `PTS` (15-char timestamp) and a 3-char `RecType`
that dispatches the rest of the line's parsing (`03_ingestion.md`
§3.2). One field, `CoNameOrCIK`, is polymorphic — numeric-only means CIK,
anything else means company name — resolved at parse time into two
separate columns. Structural outlier — Archetype D.

**CMP (company), SEC (security), FIN (quarterly financials)** — field
lists per the TPC-DI spec's fixed-width layout; see the spec §2.2.2.6
for the authoritative column-offset table.

**CMP Example:**\
`19670914-023913CMPNkpmsBILljFbVIAskKSudkLAIHYbeCnBzSVbIcIypZcePYIXqxdlNs      0000000095ACTVSBCCC-1921040222767 Misty Street                                                                                                                                              45206       Torrance                 OK                  United States of AmericaPhilipson                                     lVJASHoMGelpWJMEbCiWJBVKeSdhXXDEhWORWuTAVfHQOoCPCtDdxBGCqPJHNER ITQIpMMraDKEKsFUFDJPEHcOTSbHKrkhkCnVwBDASypJbj`

**SEC Example:**\
`19670926-205039SECAAAAAAAAAAAAAHLPREF_AACTVeDuNDDBObVQJcXSJiVMfkO                                                NASDAQ141200231    1955022218501023        2.320000000081`

**FIN Example:**\
`19670705-152952FIN196731967070119670705     333091604.98     206200734.95        1.08        1.07        0.62     883806847.78  233589694804.18    4834168239.85    191000965    1932707650000000047`
### 1.2.7 `Trade.txt`

**Format:** pipe-delimited · **Scope:** Batch1 & Batch2/3 — **schema
differs between them**

- **Batch1 (Historical Load)** — 14 fields, no CDC columns.
- **Batch2/3 (Incremental)** — 16 fields, `CDC_FLAG`/`CDC_DSN` prepended.

This is the canonical example of the schema-shift pattern flagged in
§1.1 — see `02_bronze_design.md` Archetype B for the bronze-side
handling and `03_ingestion.md` §3.1 for the parsing logic.

### 1.2.8 `TradeHistory.txt`

**Format:** pipe-delimited · **Scope:** Batch1 (Historical Load) only,
per spec — no Batch2/3 counterpart exists at all

Trade-lifecycle fact data (linked to `Trade.txt` via `T_ID`), loaded
once like a static dimension but semantically fact data — Archetype E,
`02_bronze_design.md` §2.

> You can see how we worked with `Trade` and `TradeHistory` together in the `04_silver.md` §4.3.2 'ADR-002: Trade' section.

### 1.2.9 `HoldingHistory.txt`

**Format:** pipe-delimited · **Scope:** Batch1 (no CDC) & Batch2/3
(with CDC) — quasi-CDC

| Column | Description |
|---|---|
| `HH_H_T_ID` | Trade ID that **originally created** this holding row |
| `HH_T_ID` | Trade ID of the **current** (modifying) trade |
| `HH_BEFORE_QTY` | Quantity held before this trade |
| `HH_AFTER_QTY` | Quantity held after this trade |

**Row example (Historical Load, no CDC):** `0|0|2939|1110`

Incremental version prepends `CDC_FLAG`/`CDC_DSN` to the same 4 fields.

### 1.2.10 `Industry.txt`

**Format:** pipe-delimited · **Scope:** Batch1 only

| Column | Type | Description |
|---|---|---|
| `IN_ID` | CHAR(2) | Industry code |
| `IN_NAME` | CHAR(50) | Industry description |
| `IN_SC_ID` | CHAR(2) | Sector identifier |

**Row example:** `AA|Misc. Capital Goods|FNB`

### 1.2.11 `StatusType.txt`

**Format:** pipe-delimited · **Scope:** Batch1 only

`ST_ID` (CHAR(4)) / `ST_NAME` (CHAR(10)). **Row example:** `ACTV|Active`

### 1.2.12 `TaxRate.txt`

**Format:** pipe-delimited · **Scope:** Batch1 only

`TX_ID` (CHAR(4)) / `TX_NAME` (CHAR(50)) / `TX_RATE` (NUM(6,5)).
**Row example:** `US1|U.S. Income Tax Bracket|0.05000`

### 1.2.13 `Time.txt`

**Format:** pipe-delimited, ordered by `SK_TimeID` · **Scope:** Batch1
only

Standard smart-key time dimension source: `SK_TimeID`, `TimeValue`,
`HourID`/`HourDesc`, `MinuteID`/`MinuteDesc`, `SecondID`/`SecondDesc`,
`MarketHoursFlag`, `OfficeHoursFlag`.

**Row example:** `85|01:23:45|1|"01"|23|"23"|45|"45"|0|0`

### 1.2.14 `TradeType.txt`

**Format:** pipe-delimited · **Scope:** Batch1 only

**Correction from spec draft:** the draft table showed 2 fields; the
actual spec defines 4 — `TT_ID`, `TT_NAME`, `TT_IS_SELL`, `TT_IS_MRKT`.

**Row example:** `TMB|Market-Buy|0|1`

### 1.2.15 `WatchHistory.txt`

**Format:** pipe-delimited · **Scope:** Batch1 (no CDC) & Batch2/3
(with CDC) — quasi-CDC, `CDC_FLAG` always `'I'`

`W_C_ID`, `W_S_SYMB`, `W_DTS`, `W_ACTION` (`ACTV`/`CNCL`).

**Row example:** `17|AAAAAAAAAAAAAJR|2012-07-07 00:03:44|ACTV`

### 1.2.16 `DailyMarket.txt`

**Format:** pipe-delimited · **Scope:** Batch1 (no CDC) & Batch2/3
(with CDC) — quasi-CDC, `CDC_FLAG` always `'I'`

`DM_DATE`, `DM_S_SYMB`, `DM_CLOSE`, `DM_HIGH`, `DM_LOW`, `DM_VOL`.

**Row example:** `2015-07-06|AAAAAAAAAAAABOY|242.93|284.42|185.08|111904727`

### 1.2.17 `CashTransaction.txt`

**Format:** pipe-delimited · **Scope:** Batch1 (no CDC) & Batch2/3
(with CDC) — quasi-CDC, `CDC_FLAG` always `'I'`

`CDC_FLAG`/`CDC_DSN` (incremental only), `CT_CA_ID`, `CT_DTS`,
`CT_AMT` (negative = withdrawal), `CT_NAME`.

**Row example (Batch2):** `I|4937695|6507|2017-07-08 10:16:09|5519.45|AYJRCJpzLBMJUWKjS...`

### 1.2.18 `Date.txt`

**Format:** pipe-delimited, ordered by `SK_DateID` · **Scope:** Batch1 only

Standard smart-key date dimension source — `SK_DateID`, `DateValue`,
`DateDesc`, and the full calendar/fiscal year/quarter/month/week
breakdown plus `HolidayFlag`.

**Row example:** `20040707|2004-07-07|July 7, 2004|2004|2004|20042|2004 Q2|20047|2004 July|200428|2004-W28|3|Wednesday|2004|2004|20051|2005 Q1|0`

---

## 1.3 Control & Audit Feeds

These two feeds carry **no business data**. They exist purely to make
the pipeline auditable and idempotent — see `02_bronze_design.md` §2
(source classification table, "Operational/Control" archetype) and
`03_ingestion.md` §3 (idempotency mechanics) for how they're consumed.

### 1.3.1 `BatchDate.txt`

**Format:** plain text, single value · **Scope:** all batches — one
control file per batch stating its as-of date

**Real values confirmed for this project:**
- Batch1: `2017-07-07`
- Batch2: `2017-07-08`
- Batch3: `2017-07-09`

This is the **one required file** in the entire pipeline — its absence
halts the batch rather than being silently skipped. Full reasoning:
`03_ingestion.md` §3, "Batch idempotency / re-ingestion."

### 1.3.2 Audit files (`*_audit.csv`)

**Format:** CSV, header row present · **Scope:** generated per
component, per batch

All audit files share the exact same column structure —
`DataSet, BatchID, Date, Attribute, Value, DValue` — but row content
differs per component, since each component reports summary statistics
for its own slice of activity only. These are the vendor-supplied
**independent reconciliation source** used throughout
`06_data_quality.md` §6.1.8 and §6.3.

**Example (`HR_audit.csv`):**
```text
DataSet, BatchID ,Date , Attribute , Value, DValue
DimBroker,1,,HR_BROKERS,4293,
```

**Example (`Customer_audit.csv`):**
```text
DataSet, BatchID ,Date , Attribute , Value, DValue
DimAccount,1,,CA_ADDACCT,4280,
DimAccount,1,,CA_CLOSEACCT,1284,
DimAccount,1,,CA_UPDACCT,2568,
DimAccount,1,,CA_ID_HIST,-1,
DimCustomer,1,,C_NEW,4728,
DimCustomer,1,,C_UPDCUST,1284,
DimCustomer,1,,C_INACT,428,
DimCustomer,1,,C_DOB_TO,4,
DimCustomer,1,,C_DOB_TY,4,
DimCustomer,1,,C_TIER_INV,251,
```