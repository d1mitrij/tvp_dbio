# tvp_dbio — Supply-Chain Impact Analysis for Infrastructure Investments

Multi-Regional Input-Output (MRIO) analysis toolkit for infrastructure project appraisal.
Computes GHG emissions, employment, water use, and value added across supply-chain tiers
for rail, health, and hydro power investments.

---

## Overview

The core model implements a **Leontief power-series decomposition** over an 8-sector global
model calibrated from EXIOBASE 3.8. Each analysis tier isolates one upstream supply-chain
round:

| Tier | Definition | Computation |
|------|-----------|-------------|
| **Tier 0** | One-time direct transaction (CAPEX paid to immediate suppliers) | `y₀ = SECTOR_ALLOC × invest_M$` — no Leontief inversion |
| **Tier 1** | First upstream round (what Tier 0 suppliers buy) | `y₁ = A · y₀` with bilateral sourcing-country breakdown |
| **Tier 2** | Second upstream round | `y₂ = A² · y₀` |
| **Tiers 3–10** | Deep upstream, aggregated | `yₙ = Aⁿ · y₀` |

Column sums of A ≈ 0.4 → geometric convergence: ~60% captured by Tier 0, ~85% by Tier 2,
>99.9% by Tier 8.

---

## Quick Start

```bash
# 1. Create conda environment
conda env create -f environment.yml
conda activate env_dbio

# 2. (Optional) Download IO databases — needed only for pymrio/iopy backends
python download_iodb.py --only oecd wiod          # fast (~1 min)
python download_iodb.py --skip-large              # skip EXIOBASE + Eora26 (>500 MB each)
python download_iodb.py \
    --eora-email your@institution.edu \
    --eora-password yourpassword                  # all databases (~4.5 GB, ~30 min)

# 3. Open a notebook
jupyter lab rail_tier_analysis.ipynb
```

---

## Repository Structure

```
tvp_dbio/
│
├── tvp_io_lib.py               ← Core MRIO library (all backends)
├── download_iodb.py            ← IO database downloader (EXIOBASE/Eora26/WIOD/OECD)
├── environment.yml             ← Conda environment (Python 3.11)
│
├── rail_tier_analysis.ipynb    ← Rail: 3 European projects, €1.85B dev + ops
├── health_tier_analysis.ipynb  ← Health: LATAM/Africa/Europe, $250M–$25M
├── hydro_tier_analysis.ipynb   ← Hydro: Africa/Asia/Europe, $2M–$150M + carbon payback
│
├── modeled_input_data/         ← Financial inputs derived from project data
│   ├── rail_finance_input.csv
│   ├── hospitals_finance_input.csv
│   ├── hydro_finance_input.csv
│   └── assumptions.txt
│
├── data/                       ← Benchmark outputs and reference data
│   ├── 20260317_Data_Greenings_clean_NG.xlsx
│   ├── rail.csv
│   ├── hospitals.csv
│   ├── hydro_power_plant.csv
│   ├── supply_chain_tiers*.csv
│   └── fastinfra_benchmark*.csv
│
├── output/                     ← Computed results
│   └── rail_tier_analysis.json
│
├── docs/
│   ├── iodb_download_guide.md  ← Technical reference: model, databases, API
│   └── indicators_overview_v4.docx
│
└── old/                        ← Superseded scripts (kept for reference)
    ├── convert_to_csv.py
    ├── fastinfra_benchmark.py
    ├── fastinfra_mrio_benchmark.py
    └── run_all_db.py
```

---

## Library API (`tvp_io_lib.py`)

```python
import tvp_io_lib as tio

# Tier 0 — direct one-time transaction (no Leontief inversion)
r = tio.tier0_impact(
    invest_usd  = 1_850_000_000,
    sector_code = "Rail_Dev",       # see SECTOR_ALLOC below
    country     = "Europe",         # broad region or ISO2: "DE", "BR", "ZA", "CN"
    database    = "exiobase",       # see 11 database keys below
    iodb_path   = "./input_iodb",
)
print(r["GHG_tCO2e"], r["Employment_FTE"], r["ValueAdded_M$"])
print(r["impact_by_sector"])        # sector-level breakdown

# Tier 1 — first upstream round with sourcing-country breakdown
r1 = tio.tier1_impact(1_850_000_000, "Rail_Dev", "Europe")
print(r1["sourcing_summary"])       # bilateral trade share table

# Tier decomposition — arbitrary range
df = tio.tier_impact(
    invest_usd  = 150_000_000,
    sector_code = "Energy",
    country     = "Asia",
    database    = "eora26",
    tier_from   = 0,
    tier_to     = 10,
)
# Returns DataFrame: tier | GHG_tCO2e | Employment_FTE | Water_1000m3 | ValueAdded_M$

# Cross-database comparison (all 4 calibrated backends)
df = tio.tier0_all_databases(1_850_000_000, "Rail_Dev", "Europe", "./input_iodb")
```

### Sector Allocation Codes

| `sector_code` | Con | Ene | Man | Tra | Hlth | Agr | Min | Wat | Archetype |
|--------------|-----|-----|-----|-----|------|-----|-----|-----|-----------|
| `Rail_Dev` | 35% | 10% | 28% | 10% | 4% | 1% | 8% | 4% | Rail construction |
| `Rail_Op` | 10% | 35% | 15% | 20% | 8% | 1% | 5% | 6% | Rail operations |
| `Energy` | 38% | 10% | 32% | 7% | 3% | 1% | 7% | 2% | Hydro / power |
| `Health_Social` | 28% | 8% | 25% | 7% | 18% | 2% | 5% | 7% | Primary/preventive care |
| `Health_Specialized` | 22% | 7% | 35% | 6% | 18% | 1% | 6% | 5% | Surgical/tertiary |
| `Health_General` | 30% | 10% | 22% | 8% | 15% | 2% | 5% | 8% | General hospital |

### Available Databases

| Key | Backend | Coverage | Best for |
|-----|---------|----------|---------|
| `"exiobase"` | calibrated | 44c+5RoW, 163 sec | GHG detail, EU projects |
| `"eora26"` | calibrated | 190 countries, 26 sec | Africa / LATAM / Asia |
| `"wiod"` | calibrated | 43c, 56 sec | Labour (SEA satellite) |
| `"oecd"` | calibrated | 66c, 45 sec | Policy / TiVA linkages |
| `"exiobase_file"` | pymrio | real EXIOBASE 3 pxp | Verification |
| `"eora26_file"` | pymrio | real Eora26 | Verification |
| `"wiod_file"` | pymrio | real WIOD 2013 | Verification |
| `"oecd_file"` | pymrio | real OECD ICIO v2021 | Verification |
| `"iopy_oecd"` | iopy | 71 regions × 45 sec | Policy detail |
| `"iopy_exio_ixi"` | iopy | 49 regions × 163 sec | Full EXIOBASE |
| `"iopy_exio_pxp"` | iopy | 49 regions × 200 sec | Full EXIOBASE pxp |

The calibrated backends (no suffix) require no database files — pure numpy, results in milliseconds.

---

## Notebooks

### `rail_tier_analysis.ipynb`
Three European rail projects (FAST-Infra archetype):
- **Rail_EU_DEV** — Development phase, €1.85B, Rail_Dev sector
- **Rail_EU_OP1** — Operations, €130K/yr, Rail_Op sector
- **Rail_EU_OP2** — Operations, €90K/yr, Rail_Op sector

Analysis layers: Tier 0 (direct CAPEX transaction) → Tier 1 (first upstream round) →
Tier 2 → Tiers 3–10 aggregated. Includes sourcing-country breakdown and cumulative
GHG/employment bar charts.

### `health_tier_analysis.ipynb`
Three health infrastructure projects across regions:
- **Proj_001** — LATAM, $250M, 5M beneficiaries, Health_Social
- **Proj_002** — Africa, $25M, 3K beneficiaries, Health_Specialized
- **Proj_003** — Europe, $75M, 500K beneficiaries, Health_General

Unique features: GHG per beneficiary normalisation, import leakage note for medical
equipment, care-type cost assumption documentation.

### `hydro_tier_analysis.ipynb`
Three hydro power refurbishment projects:
- **Hydro_AF** — Africa, $30M, 150K tCO2e/yr avoided
- **Hydro_AS** — Asia, $150M, 834K tCO2e/yr avoided
- **Hydro_EU** — Europe, $2M, 6.1K tCO2e/yr avoided

Unique features: **carbon payback analysis** — embodied supply-chain GHG vs operational
avoided CO₂, payback period in years. Africa Energy multiplier (1.90×) highlighted for
coal-heavy grid context.

---

## Environment

```yaml
# environment.yml
name: env_dbio
dependencies:
  - python=3.11
  - numpy>=1.26, pandas>=2.1, matplotlib>=3.8
  - jupyterlab>=4.0, ipykernel>=6.0, nbformat>=5.9
  - pip: [pymrio>=0.6.3, iopy>=0.2.4]
```

```bash
conda env create -f environment.yml
conda activate env_dbio
```

---

## Data Sources

- **EXIOBASE 3.8** — [exiobase.eu](https://www.exiobase.eu) — 44 countries, 163 products/industries, GHG satellite
- **Eora26** — [worldmrio.com](https://worldmrio.com) — 190 countries, 26 sectors (free registration required)
- **WIOD 2016** — [wiod.org](http://www.wiod.org) — 43 countries, 56 sectors
- **OECD ICIO v2021** — [oecd.org/sti/ind/inter-country-input-output-tables](https://www.oecd.org/sti/ind/inter-country-input-output-tables.htm) — 66 countries, 45 industries
- **FAST-Infra Label** — [fastinfrastructure.com](https://fastinfrastructure.com) — sustainable infrastructure assessment framework

---

## License

See [LICENSE](LICENSE).
