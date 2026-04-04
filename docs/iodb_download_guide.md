# tvp_dbio — Technical Reference
**Supply-Chain Impact Analysis for Infrastructure Investments**
*Last updated: 2026-04-03*

---

## Project Overview

This system computes supply-chain impacts (GHG, Employment, Water, Value Added) for
infrastructure investments using Multi-Regional Input-Output (MRIO) analysis.
The supply-chain tier convention:

| Tier | Definition |
|------|-----------|
| **Tier 0** | Asset owner's one-time CAPEX transaction to acquire or commission the asset — investment distributed across direct procurement categories (contractors, equipment manufacturers, utilities directly contracted by the investor); no Leontief inversion (`y₀ = SECTOR_ALLOC × invest_M$`) |
| **Tier 1** | Supply chain triggered by the Tier 0 payment — what Tier 0 contractors buy from their upstream suppliers (steel mills, cement plants, raw materials); first Leontief round with bilateral sourcing-country breakdown (`y₁ = A · y₀`) |
| **Tier n** | nth upstream round (`yₙ = Aⁿ · y₀`) — each tier captures the suppliers' suppliers, n steps removed from the original CAPEX |

Two scripts do all the work:

```
tvp_dbio/
├── download_iodb.py        ← Step 1: download all IO databases
├── tvp_io_lib.py           ← Step 2: compute impacts (import as library)
├── input_iodb/             ← downloaded databases (created by Step 1, gitignored)
├── modeled_input_data/     ← project financial inputs (hospitals/hydro/rail CSVs)
├── data/                   ← benchmark outputs and reference data (local only, gitignored)
├── docs/                   ← this file + indicators overview
└── old/                    ← superseded scripts (pymrio/mario/iopy standalone)
```

---

## Step 1: Download IO Databases (`download_iodb.py`)

### What it downloads

| Database | Folder | Size | Time | Used for |
|---|---|---|---|---|
| EXIOBASE 3 pxp (2018) | `input_iodb/exiobase/` | ~900 MB | ~1 min | pymrio backend |
| WIOD 2013 (2014) | `input_iodb/wiod/` | ~350 MB | ~6 s | pymrio backend |
| OECD ICIO v2021 (2018) | `input_iodb/oecd/` | ~200 MB | ~40 s | pymrio backend |
| FIGARO 2018 ixi+pxp | `input_iodb/figaro/` | — | — | ✗ unavailable (Eurostat URL change) |
| Eora26 (2015) | `input_iodb/eora26/` | ~2 GB | ~1 min | pymrio backend |
| iopy OECD 2021 | iopy internal cache | ~95 MB | ~30 s | iopy backend |
| iopy ExioBase 3.81 ixi | iopy internal cache | ~729 MB | ~7 min | iopy backend |
| iopy ExioBase 3.81 pxp | iopy internal cache | ~500 MB | ~20 min | iopy backend |

iopy databases auto-download to a fixed internal cache path:
`<python_packages>/iopy/.temp_data/`
A symlink `input_iodb/iopy_cache/` points to this folder for visibility.

### Usage

```bash
# Download everything (total ~4.5 GB, ~30 min on fast connection)
python3 download_iodb.py \
    --eora-email your@institution.edu \
    --eora-password yourpassword

# Skip large files (EXIOBASE + Eora26 + iopy ExioBase)
python3 download_iodb.py --skip-large

# Specific databases only
python3 download_iodb.py --only oecd wiod
python3 download_iodb.py --only iopy
python3 download_iodb.py --only eora26 --eora-email x@y.com --eora-password pw

# Year overrides (defaults: exiobase=2018, oecd=2018, wiod=2014, figaro=2018)
python3 download_iodb.py --exiobase-year 2015 --oecd-year 2016
```

### Known issues

| Issue | Affected | Workaround |
|---|---|---|
| OECD 2022-small broken URL | iopy only | Use OECD 2021 (fully functional) |
| FIGARO 2022 HTTP 404 | iopy + mario | Eurostat moved to CIRCABC/JRC Data Catalogue; both libraries unpatched. Alternative: https://data.jrc.ec.europa.eu/collection/id-00403 |
| Eora26 needs credentials | pymrio + mario | Register free at https://worldmrio.com/login.jsp |
| WIOD 2016 lacks extensions | pymrio | Always use 2013 release (`download_wiod2013`) |

---

## Step 2: Compute Impacts (`tvp_io_lib.py`)

### Architecture

```
tvp_io_lib.py
│
├── tier0_impact()          ← primary function: direct (tier 0) impact
├── tier_impact()           ← power-series tier decomposition (T0→T8)
├── tier0_all_databases()   ← cross-database comparison table
├── list_databases()        ← availability check for all 11 databases
└── clear_cache()           ← release loaded databases from memory
```

The library consolidates three previously separate analysis pipelines:

| Old script | Backend | Now |
|---|---|---|
| `old/fastinfra_benchmark.py` | pymrio calibrated | → `tier0_impact(..., database="exiobase")` etc. |
| `old/run_all_db.py` | pymrio all 4 DBs | → `tier0_all_databases(...)` |
| `old/mario_analysis/fastinfra_mario.py` | mario | → `tier_impact(...)` (same calibrated model) |
| `old/iopy_analysis/fastinfra_iopy.py` | iopy | → `tier0_impact(..., database="iopy_exio_ixi")` etc. |

---

### 11 available databases

| Database key | Backend | Description |
|---|---|---|
| `"exiobase"` | calibrated | EXIOBASE 3 calibration — 44c+5RoW, 163 sec, EU-centric, best GHG detail |
| `"eora26"` | calibrated | Eora26 calibration — 190 countries, 26 sec, best for Africa/LATAM/Asia |
| `"wiod"` | calibrated | WIOD 2013 calibration — 43c, 56 sec, best labour (SEA satellite) |
| `"oecd"` | calibrated | OECD ICIO calibration — 66c, 45 sec, best policy/TiVA |
| `"exiobase_file"` | pymrio | Real EXIOBASE 3 pxp from `input_iodb/exiobase/` |
| `"eora26_file"` | pymrio | Real Eora26 from `input_iodb/eora26/` |
| `"wiod_file"` | pymrio | Real WIOD 2013 from `input_iodb/wiod/` |
| `"oecd_file"` | pymrio | Real OECD ICIO v2021 from `input_iodb/oecd/` |
| `"iopy_oecd"` | iopy | iopy OECD 2021 — 71 regions × 45 sectors (auto-cache) |
| `"iopy_exio_ixi"` | iopy | iopy ExioBase 3.81 ixi — 49 regions × 163 sectors (auto-cache) |
| `"iopy_exio_pxp"` | iopy | iopy ExioBase 3.81 pxp — 49 regions × 200 sectors (auto-cache) |

**Three backends:**

- **Calibrated**: Pure numpy, always available, no files needed. Uses an 8-sector global
  model (A_BASE, S_BASE) with per-database calibration factors (`s_ghg`, `s_emp`, `s_wat`,
  `a_delta`) and per-region intensity multipliers. Produces results in milliseconds.

- **pymrio** (`_file` variants): Loads real database files from `input_iodb/` using pymrio.
  When loaded, extracts GHG intensity directly from the satellite extension. Falls back to
  the calibrated model if the file is missing or extraction fails.

- **iopy**: Loads from iopy's internal auto-download cache. Extracts actual value-added
  intensity (V/X) per sector from the real database. GHG/Employment/Water rows use the
  calibrated model since iopy has no satellite accounts.

---

### Core model: 8-sector Leontief

All three backends share the same **8-sector global model** for sector allocation and
tier decomposition:

```
SECTORS_8 = [
    Construction, Energy_Utilities, Manufacturing, Transport_Logistics,
    Health_Social, Agriculture, Mining_Extraction, Water_Waste
]
```

**Technical coefficient matrix `A` (8×8)**
Calibrated from EXIOBASE 3.8 pxp. Each column `j` gives the input from each sector
per unit of output in sector `j`. Column sums ∈ [0.32, 0.58] — ensures the Leontief
power series converges within 8 tiers.

**Stressor intensity matrix `S` (4×8)**
Global average intensities per M$ of gross output:
- Row 0: GHG tCO2e/M$ — Construction=220, Energy=190, Manufacturing=380, ...
- Row 1: Employment FTE/M$ — Construction=14, Energy=5, Manufacturing=8, ...
- Row 2: Water 1000m³/M$ — Construction=0.8, Energy=1.2, Manufacturing=1.5, ...
- Row 3: Value Added M$/M$ — Construction=0.48, Energy=0.64, ...

**Regional multipliers** applied to S rows 0-2:
- Europe: GHG 0.52–0.85× (clean grid), Employment 0.68–0.78× (high productivity)
- Africa: GHG 1.12–1.90× (coal-heavy energy), Employment 1.48–1.62× (labour-intensive)
- LATAM: GHG 1.08–1.28×, Employment 1.18–1.28×
- Asia: GHG 1.10–1.68× (CN coal), Employment 1.32–1.42×

**Per-database calibration** (`DB_PROFILES`):

| Database | GHG bias | Employment bias | Rationale |
|---|---|---|---|
| EXIOBASE | 0.88–0.97× | 0.87–0.91× | EU-centric clean tech, high productivity |
| Eora26 | 1.05–1.15× | 1.10–1.22× | Developing-country supply chains, informal economy |
| WIOD | 1.02–1.06× | 1.15–1.25× | Pre-Paris 2014 vintage, SEA labour satellite |
| OECD | 0.92–0.99× | 1.02–1.06× | Post-2020 efficiency gains, intermediate labour |

---

### Tier 0 computation (asset owner's CAPEX transaction)

**Tier 0 = the asset owner's one-time CAPEX transaction.**  The investor acquires
or commissions the asset (railroad, hospital, power plant) by paying the direct
procurement categories — construction contractors, equipment manufacturers
(rolling stock, medical devices, turbines) contracted directly, energy utilities,
logistics providers.  No Leontief inversion — purely the direct procurement effect.

```
y0[j] = SECTOR_ALLOC[sector_code][j] × invest_M$       (8-vector, M$ per sector)
impact[stressor, j] = S_adjusted[stressor, j] × y0[j]
tier0_total[stressor] = Σ_j impact[stressor, j]
```

**Sector allocation (`SECTOR_ALLOC`)** — how the investment is split at tier 0:

| Sector code | Con | Ene | Man | Tra | Hlth | Agr | Min | Wat |
|---|---|---|---|---|---|---|---|---|
| Health_Social | 28% | 8% | 25% | 7% | 18% | 2% | 5% | 7% |
| Health_Specialized | 22% | 7% | 35% | 6% | 18% | 1% | 6% | 5% |
| Health_General | 30% | 10% | 22% | 8% | 15% | 2% | 5% | 8% |
| Energy | 38% | 10% | 32% | 7% | 3% | 1% | 7% | 2% |
| Rail_Dev | 35% | 10% | 28% | 10% | 4% | 1% | 8% | 4% |
| Rail_Op | 10% | 35% | 15% | 20% | 8% | 1% | 5% | 6% |

---

### Tier decomposition (T0 → T8)

Uses the **Leontief power series** — each tier is one round of upstream supply:

```
x_t = A^t · y0            (output vector at tier t)
impact_t = S · diag(x_t)  (stressor impact at tier t)
```

Tier 0 = the investor's CAPEX to the direct procurement categories.
Tier 1 = what those Tier 0 contractors buy from their own upstream suppliers.
Tier 2 = what Tier 1 suppliers buy, and so on.

Column sums of A ≈ 0.4 → geometric convergence ~60% captured by tier 0, ~85% by tier 2,
>99.9% by tier 8.

---

### Quick-start examples

```python
import tvp_io_lib as tio
from pathlib import Path

IODB = Path("./input_iodb")

# ── Tier 0 impact (direct) ────────────────────────────────────────────────
result = tio.tier0_impact(
    invest_usd  = 1_850_000_000,  # USD
    sector_code = "Rail_Dev",     # see SECTOR_ALLOC keys
    country     = "Europe",       # broad region or ISO2: "DE", "BR", "ZA", "CN"
    database    = "exiobase",     # any of 11 database keys
    iodb_path   = IODB,
)
print(result["GHG_tCO2e"])        # tCO2e at tier 0
print(result["Employment_FTE"])   # FTE at tier 0
print(result["ValueAdded_M$"])    # M$ value added at tier 0
print(result["impact_by_sector"]) # sector-level breakdown dict

# ── Cross-database comparison ─────────────────────────────────────────────
df = tio.tier0_all_databases(
    invest_usd  = 1_850_000_000,
    sector_code = "Rail_Dev",
    country     = "Europe",
    iodb_path   = IODB,
    include_iopy= False,           # set True to include slow iopy loads
)
# Returns DataFrame indexed by database name

# ── Tier decomposition (T0→T8) ────────────────────────────────────────────
df = tio.tier_impact(
    invest_usd  = 250_000_000,
    sector_code = "Health_Social",
    country     = "Africa",
    database    = "eora26",
    iodb_path   = IODB,
    max_tiers   = 8,
)
# Returns DataFrame: tier | supplying_sector | spend_M$ | GHG_tCO2e | Employment_FTE | ...

# ── Check what's available ────────────────────────────────────────────────
for name, info in tio.list_databases(IODB).items():
    status = "✓" if info["available"] else "○"
    print(f"{status} {name:<22} [{info['backend']}]  {info['note']}")
```

---

### Sample output — Rail_Dev / Europe / $2,016M at tier 0

| Database | GHG tCO2e | Employment FTE | ValueAdded M$ |
|---|---|---|---|
| exiobase | 379,713 | 13,599 | 1,043.7 |
| eora26 | 432,960 | 17,741 | 1,043.7 |
| wiod | 409,742 | 18,389 | 1,043.7 |
| oecd | 382,400 | 15,940 | 1,043.7 |

Database spread: GHG ±7%, Employment ±15% — reflects known methodological differences.
Value Added identical across calibrated backends (regional multipliers not applied to VA row).

### Sample output — tier decomposition, Health_Social / Africa / $250M (Eora26)

| Tier | Spend M$ | GHG tCO2e | Employment FTE |
|---|---|---|---|
| 0 | 30.00 | 12,483 | 575 |
| 1 | 15.83 | 6,491 | 282 |
| 2 | 7.62 | 3,059 | 139 |
| 3 | 3.62 | 1,444 | 66 |
| 4 | 1.71 | 682 | 31 |

---

## Project Data Inputs (`modeled_input_data/`)

The library uses financial inputs from three CSV files:

| File | Projects | Key fields |
|---|---|---|
| `hospitals_finance_input.csv` | 3 (LATAM/Africa/Europe) | Est_Investment_USD, Sector_Code, Region, Beneficiaries_H&S |
| `hydro_finance_input.csv` | 3 (Africa/Asia/Europe) | Est_Investment_USD, Region, Avoided_CO2_Tons |
| `rail_finance_input.csv` | 3 (Europe, Dev+Op stages) | Est_Capex_EUR, Stage, Reach_Ppl_Yr |

These were derived from the source Excel workbook using `old/convert_to_csv.py`
and manually enriched. The raw source files are kept locally in `data/` (gitignored —
not available in the online repository).

---

## Database Sector Mappings (for iopy backends)

The iopy backends (`iopy_oecd`, `iopy_exio_ixi`, `iopy_exio_pxp`) use native database
sector codes, verified by reading the index files directly from the downloaded ZIPs:

| Our 8 sectors | OECD code | ExioBase ixi | ExioBase pxp |
|---|---|---|---|
| Construction | `41T43` | `i45` | `p45` |
| Energy_Utilities | `35` | `i40.13` | `p40.13` |
| Manufacturing | `24` | `i28` | `p28` |
| Transport_Logistics | `49` | `i60.2` | `p60.2` |
| Health_Social | `86T88` | `i85` | `p85` |
| Agriculture | `01T02` | `i01.h` | `p01.h` |
| Mining_Extraction | `07T08` | `i13.1` | `p14.3` |
| Water_Waste | `36T39` | `i41` | `p41` |

ExioBase stores sectors as short `CodeNr` identifiers (from `industries.txt`/`products.txt`
inside the ZIP) — **not** the English sector names. English names are available via
`db.sector_name_mapping` but are not used for indexing.

---

## Archived Scripts (`old/`)

| Script | What it did | Replaced by |
|---|---|---|
| `old/convert_to_csv.py` | Converted Excel to three sector CSVs | Output files in `modeled_input_data/` (source Excel kept locally in `data/`, gitignored) |
| `old/fastinfra_benchmark.py` | pymrio FAST-Infra indicator benchmark | `tvp_io_lib.tier0_impact()` |
| `old/fastinfra_mrio_benchmark.py` | pymrio enhanced benchmark with MRIO load | `tvp_io_lib.tier0_impact(..., database="exiobase_file")` |
| `old/run_all_db.py` | pymrio all 4 databases in parallel | `tvp_io_lib.tier0_all_databases()` |
| `old/mario_analysis/fastinfra_mario.py` | mario IOT benchmark + linkages | `tvp_io_lib.tier_impact()` |
| `old/iopy_analysis/fastinfra_iopy.py` | iopy Leontief/Ghosh shocks | `tvp_io_lib.tier0_impact(..., database="iopy_*")` |

Output CSVs generated by the old scripts are preserved locally in `data/` (gitignored)
and `old/*/output/`. They are not included in the online repository.
