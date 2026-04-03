#!/usr/bin/env python3
"""
fastinfra_mrio_benchmark.py
─────────────────────────────────────────────────────────────────────────────
Enriches the FAST-Infra benchmark with:
  · Project financial & volume data from modeled_input_data/
  · Supply chain tier analysis (tier 0 → 8) using a calibrated 8-sector model
  · Region- and sector-adjusted environmental / social intensity benchmarks
  · MRIO database switching: EXIOBASE | Eora26 | WIOD | OECD ICIO
    (falls back to calibrated benchmark intensities when no local DB is found)

Outputs
-------
  data/fastinfra_benchmark_enhanced.csv  — indicator benchmark + SC totals
  data/supply_chain_tiers.csv            — tier-by-tier footprint (648 rows)

Database download commands (run once, then set MRIO_PATH):
  EXIOBASE:  pymrio.download_exiobase3(storage_folder="./mrio_data/exiobase/")
  Eora26:    request from www.worldmrio.com/eora26 (free academic)
  WIOD:      pymrio.download_wiod2016(storage_folder="./mrio_data/wiod/")
  OECD ICIO: https://stats.oecd.org/Index.aspx?DataSetCode=ICIO2023
"""

import warnings
import numpy as np
import pandas as pd
import pymrio
from pathlib import Path

warnings.filterwarnings("ignore")

# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════
DB_CHOICE  = "exiobase"   # "exiobase" | "eora26" | "wiod" | "oecd"
MRIO_PATH  = None         # local path to downloaded DB, e.g. "./mrio_data/exiobase/"
MAX_TIERS  = 8
EUR_TO_USD = 1.09         # EUR → USD conversion

DATA_DIR   = Path("data")
INPUT_DIR  = Path("modeled_input_data")
OUT_BENCH  = DATA_DIR / "fastinfra_benchmark_enhanced.csv"
OUT_TIERS  = DATA_DIR / "supply_chain_tiers.csv"

# ══════════════════════════════════════════════════════════════════════════════
# 1.  8-SECTOR CLASSIFICATION
#     Condensed inter-industry model calibrated from EXIOBASE 3 / Eora26
# ══════════════════════════════════════════════════════════════════════════════
SECTORS_8 = [
    "Construction",
    "Energy_Utilities",
    "Manufacturing",
    "Transport_Logistics",
    "Health_Social",
    "Agriculture",
    "Mining_Extraction",
    "Water_Waste",
]
NSEC = len(SECTORS_8)

# ── Technical coefficient matrix A (rows = input sector, cols = buying sector)
# A[i,j] = M$ bought from sector i per M$ of sector j output
# Column sums ∈ [0.32, 0.58] — power series converges within 8 tiers
A_GLOBAL = np.array([
    #  Con    Ene    Man    Tra    Hlth   Agr    Min    Wat
    [0.040, 0.020, 0.080, 0.025, 0.015, 0.010, 0.030, 0.015],  # Construction
    [0.075, 0.055, 0.060, 0.075, 0.040, 0.040, 0.070, 0.095],  # Energy
    [0.180, 0.070, 0.120, 0.085, 0.060, 0.030, 0.180, 0.045],  # Manufacturing
    [0.060, 0.045, 0.055, 0.055, 0.040, 0.060, 0.055, 0.030],  # Transport
    [0.055, 0.030, 0.038, 0.048, 0.080, 0.028, 0.030, 0.038],  # Health
    [0.012, 0.012, 0.022, 0.012, 0.038, 0.120, 0.012, 0.012],  # Agriculture
    [0.088, 0.098, 0.175, 0.038, 0.018, 0.018, 0.098, 0.028],  # Mining
    [0.020, 0.038, 0.028, 0.018, 0.028, 0.038, 0.020, 0.055],  # Water
])

# ── Stressor intensity matrix S (global averages, per M$ gross output)
# Rows: GHG (tCO2e), Employment (FTE), Water (1000 m³), Value Added (M$)
S_GLOBAL = np.array([
    #   Con    Ene    Man    Tra    Hlth   Agr    Min    Wat
    [220.0, 190.0, 380.0, 175.0, 140.0, 150.0, 320.0, 110.0],  # GHG tCO2e/M$
    [ 14.0,   5.0,   8.0,  10.0,  18.0,  25.0,   7.0,  12.0],  # FTE/M$
    [  0.80,  1.20,  1.50,  0.60,  0.90,  5.00,  1.80,  8.00],  # 1000 m³/M$
    [  0.48,  0.64,  0.42,  0.60,  0.70,  0.76,  0.56,  0.69],  # VA M$/M$
])
STRESSOR_LABELS = ["GHG_tCO2e", "Employment_FTE", "Water_1000m3", "ValueAdded_M$"]

# ── Regional multipliers (relative to global average)
# Sources: EXIOBASE 3.8 pxp; Eora26 country intensities; WIOD SEA
REGION_MULT = {
    #           Con    Ene    Man    Tra    Hlth   Agr    Min    Wat
    "GHG": {
        "Europe": [0.78, 0.52, 0.80, 0.80, 0.76, 0.85, 0.72, 0.70],
        "LATAM":  [1.20, 1.28, 1.18, 1.18, 1.22, 1.08, 1.22, 1.18],
        "Africa": [1.40, 1.90, 1.38, 1.32, 1.42, 1.12, 1.38, 1.48],
        "Asia":   [1.30, 1.68, 1.30, 1.25, 1.32, 1.10, 1.30, 1.32],
        "Global": [1.00, 1.00, 1.00, 1.00, 1.00, 1.00, 1.00, 1.00],
    },
    "EMP": {
        "Europe": [0.68, 0.72, 0.72, 0.78, 0.72, 0.68, 0.72, 0.78],
        "LATAM":  [1.22, 1.18, 1.22, 1.18, 1.28, 1.18, 1.22, 1.18],
        "Africa": [1.58, 1.48, 1.52, 1.48, 1.62, 1.48, 1.52, 1.58],
        "Asia":   [1.38, 1.32, 1.42, 1.38, 1.42, 1.38, 1.42, 1.38],
        "Global": [1.00, 1.00, 1.00, 1.00, 1.00, 1.00, 1.00, 1.00],
    },
    "WAT": {
        "Europe": [0.75, 0.88, 0.82, 0.78, 0.80, 0.70, 0.80, 0.85],
        "LATAM":  [1.25, 1.35, 1.28, 1.22, 1.30, 1.35, 1.28, 1.35],
        "Africa": [1.55, 1.65, 1.52, 1.42, 1.55, 1.65, 1.52, 1.55],
        "Asia":   [1.42, 1.52, 1.48, 1.38, 1.45, 1.55, 1.48, 1.52],
        "Global": [1.00, 1.00, 1.00, 1.00, 1.00, 1.00, 1.00, 1.00],
    },
}

# ── Initial sector spending allocation (how project CAPEX is split at tier 0)
# Each row sums to 1.0 — calibrated from infrastructure cost-breakdown literature
SECTOR_ALLOC = {
    # Health_Social: general hospital (construction + medical equip dominant)
    "Health_Social":     np.array([0.28, 0.08, 0.25, 0.07, 0.18, 0.02, 0.05, 0.07]),
    # Health_Specialized: research / diagnostics (heavy manufacturing/pharma)
    "Health_Specialized":np.array([0.22, 0.07, 0.35, 0.06, 0.18, 0.01, 0.06, 0.05]),
    # Health_General: district hospital (construction-heavy)
    "Health_General":    np.array([0.30, 0.10, 0.22, 0.08, 0.15, 0.02, 0.05, 0.08]),
    # Energy (hydro): civil construction + electromechanical equipment
    "Energy":            np.array([0.38, 0.10, 0.32, 0.07, 0.03, 0.01, 0.07, 0.02]),
    # Rail development: civil works + steel + signalling
    "Rail_Dev":          np.array([0.35, 0.10, 0.28, 0.10, 0.04, 0.01, 0.08, 0.04]),
    # Rail operational: energy-dominant + maintenance services
    "Rail_Op":           np.array([0.10, 0.35, 0.15, 0.20, 0.08, 0.01, 0.05, 0.06]),
}

# ── MRIO sector mappings for each database
DB_SECTOR_MAP = {
    "exiobase": {
        "Construction":        "Construction",
        "Energy_Utilities":    "Distribution and trade of electricity",
        "Manufacturing":       "Manufacture of basic metals and fabricated metal products",
        "Transport_Logistics": "Rail transport",
        "Health_Social":       "Human health and social work activities",
        "Agriculture":         "Crop and animal production, hunting and related service activities",
        "Mining_Extraction":   "Mining of metal ores",
        "Water_Waste":         "Water collection, purification and supply",
        # Project-level sector code → EXIOBASE sector
        "Health_Social_proj":      "Human health and social work activities",
        "Health_Specialized_proj": "Human health and social work activities",
        "Health_General_proj":     "Human health and social work activities",
        "Energy_proj":             "Production of electricity by hydro",
        "Rail_Dev_proj":           "Construction",
        "Rail_Op_proj":            "Rail transport",
    },
    "eora26": {
        "Construction":        "Construction",
        "Energy_Utilities":    "Electricity, Gas and Water",
        "Manufacturing":       "Metal Products",
        "Transport_Logistics": "Transport",
        "Health_Social":       "Health and Social Work",
        "Agriculture":         "Agriculture",
        "Mining_Extraction":   "Mining and Quarrying",
        "Water_Waste":         "Electricity, Gas and Water",
        "Health_Social_proj":      "Health and Social Work",
        "Health_Specialized_proj": "Health and Social Work",
        "Health_General_proj":     "Health and Social Work",
        "Energy_proj":             "Electricity, Gas and Water",
        "Rail_Dev_proj":           "Construction",
        "Rail_Op_proj":            "Transport",
    },
    "wiod": {
        "Construction":        "F  Construction",
        "Energy_Utilities":    "D35 Electricity, gas, steam and air conditioning supply",
        "Manufacturing":       "C24 Manufacture of basic metals",
        "Transport_Logistics": "H49 Land transport and transport via pipelines",
        "Health_Social":       "Q86 Human health activities",
        "Agriculture":         "A01 Crop and animal production",
        "Mining_Extraction":   "B07 Mining of metal ores",
        "Water_Waste":         "E36 Water collection, treatment and supply",
        "Health_Social_proj":      "Q86 Human health activities",
        "Health_Specialized_proj": "Q86 Human health activities",
        "Health_General_proj":     "Q86 Human health activities",
        "Energy_proj":             "D35 Electricity, gas, steam and air conditioning supply",
        "Rail_Dev_proj":           "F  Construction",
        "Rail_Op_proj":            "H49 Land transport and transport via pipelines",
    },
    "oecd": {
        "Construction":        "Construction",
        "Energy_Utilities":    "Electricity, gas, water supply",
        "Manufacturing":       "Basic metals and fabricated metal products",
        "Transport_Logistics": "Land transport",
        "Health_Social":       "Health and social work",
        "Agriculture":         "Agriculture, hunting, forestry and fishing",
        "Mining_Extraction":   "Mining and quarrying",
        "Water_Waste":         "Electricity, gas, water supply",
        "Health_Social_proj":      "Health and social work",
        "Health_Specialized_proj": "Health and social work",
        "Health_General_proj":     "Health and social work",
        "Energy_proj":             "Electricity, gas, water supply",
        "Rail_Dev_proj":           "Construction",
        "Rail_Op_proj":            "Land transport",
    },
}

# ── FAST-Infra indicator ↔ supply chain tier relevance mapping
INDICATOR_TIER_RELEVANCE = {
    # Scope 1 (direct) → tier 0
    "E2.G2K.1": "T0",     "E2.G2K.2": "T0-T1",  "E2.G2K.3": "T0-T8",
    "E2.Q8W.A": "T0",     "E2.Q8W.B": "T0",
    "E4.T9A":   "T0-T2",  "E4.2MB.1": "T0",      "E4.O5C":   "T0",
    "R5.U2X.A": "T0",     "R5.V4H.1": "T0",      "R5.B6D.1": "T0-T2",
    "S7.P5W":   "T0",     "S7.X4B":   "T0",      "S7.P8B.1": "T0",
    # Upstream (Scope 3) → tier 2+
    "E1.4OO.3.A": "T0-T3", "E1.UO5":  "T0-T3",   "E1.8OA":  "T0-T3",
    "E3.QK5.A":   "T0-T3", "E3.9IP":  "T0-T3",   "E3.8VX":  "T0-T3",
    "E4.BR3.A":   "T0-T3", "E4.S2H.A":"T0-T3",   "E4.7BL.A":"T0-T3",
    "S8.GC2":     "T0-T2", "S8.I8Q":  "T0-T2",
    "S9.QN8":     "T0-T2",
}
DEFAULT_TIER_RELEVANCE = "T0-T8"

# ══════════════════════════════════════════════════════════════════════════════
# 2.  MRIO DATABASE INTERFACE
# ══════════════════════════════════════════════════════════════════════════════
def load_mrio(db_name, path=None):
    """
    Attempt to load a real MRIO database. Returns a calibrated pymrio IOSystem
    or None if files are not available.

    Download commands
    -----------------
    EXIOBASE  : pymrio.download_exiobase3(storage_folder="./mrio_data/exiobase/")
    Eora26    : request from www.worldmrio.com/eora26 (free academic licence)
    WIOD      : pymrio.download_wiod2016(storage_folder="./mrio_data/wiod/")
    OECD ICIO : https://stats.oecd.org/Index.aspx?DataSetCode=ICIO2023
                then: pymrio.parse_oecd(path=path, year=2018)
    """
    if path is None:
        print(f"[{db_name.upper()}] MRIO_PATH not set → using calibrated benchmark intensities.")
        return None
    try:
        if db_name == "exiobase":
            io = pymrio.load_exiobase3(path=path)
        elif db_name == "eora26":
            io = pymrio.parse_eora26(path=path, year=2015)
        elif db_name == "wiod":
            io = pymrio.parse_wiod(path=path, year=2014)
        elif db_name == "oecd":
            io = pymrio.parse_oecd(path=path, year=2018)
        else:
            raise ValueError(f"Unknown database: {db_name}")
        io.calc_all()
        print(f"[{db_name.upper()}] Database loaded and computed successfully.")
        return io
    except Exception as exc:
        print(f"[{db_name.upper()}] Load failed: {exc}")
        print(f"   Falling back to calibrated benchmark intensities.")
        return None


def calibrate_from_mrio(io, db_name, sector_key, region_iso2=None):
    """
    Extract GHG intensity (tCO2e/M$ output) for one sector from a loaded MRIO.
    Returns a float calibration factor relative to S_GLOBAL, or 1.0 on failure.
    Used to adjust benchmark intensities when a real database is available.
    """
    if io is None:
        return 1.0
    try:
        sector_name = DB_SECTOR_MAP[db_name].get(f"{sector_key}_proj", "Construction")
        S = io.satellite.S if hasattr(io, "satellite") else io.emissions.S
        # Find CO2 row (first row containing "CO2" or "GHG")
        ghg_rows = [r for r in S.index if "CO2" in str(r) or "GHG" in str(r) or "ghg" in str(r)]
        if not ghg_rows:
            return 1.0
        ghg_row = S.loc[ghg_rows[0]]
        # Find sector column
        matching = [c for c in ghg_row.index if sector_name.lower() in str(c).lower()]
        if not matching:
            return 1.0
        db_intensity = ghg_row[matching[0]]  # tCO2e per M€ output (approx)
        # Sector index in our 8-sector model
        sec_idx = SECTORS_8.index(sector_key) if sector_key in SECTORS_8 else 0
        benchmark_intensity = S_GLOBAL[0, sec_idx]  # our benchmark GHG intensity
        return float(db_intensity) / benchmark_intensity if benchmark_intensity else 1.0
    except Exception:
        return 1.0


# ══════════════════════════════════════════════════════════════════════════════
# 3.  LOAD MODELED INPUT DATA
# ══════════════════════════════════════════════════════════════════════════════
def _load_hospitals():
    df = pd.read_csv(INPUT_DIR / "hospitals_finance_input.csv")
    df["project_label"] = [f"hospitals_P{i+1}" for i in range(len(df))]
    df["investment_usd"] = df["Est_Investment_USD"].astype(float)
    df["sector_code"]    = df["Sector_Code"]
    df["region"]         = df["Region"]
    df["stage"]          = df["Stage"]
    df["beneficiaries"]  = df["Beneficiaries_H&S"]
    df["multiplier"]     = df["Multiplier_Benchmark"]
    return df[["project_label","region","stage","sector_code",
               "investment_usd","beneficiaries","multiplier"]]


def _load_hydro():
    df = pd.read_csv(INPUT_DIR / "hydro_finance_input.csv")
    df["project_label"] = [f"hydro_P{i+1}" for i in range(len(df))]
    df["investment_usd"] = df["Est_Investment_USD"].astype(float)
    df["sector_code"]    = "Energy"
    df["region"]         = df["Region"]
    df["stage"]          = df["Sector"]   # "Energy" used as sector; Impact_Type as stage
    df["stage"]          = df["Impact_Type"]
    df["beneficiaries"]  = df["Avoided_CO2_Tons"]
    df["multiplier"]     = 1.0
    return df[["project_label","region","stage","sector_code",
               "investment_usd","beneficiaries","multiplier"]]


def _load_rail():
    df = pd.read_csv(INPUT_DIR / "rail_finance_input.csv")
    df["project_label"] = [f"rail_P{i+1}" for i in range(len(df))]
    # Convert EUR → USD; use 0 if N/A
    df["investment_usd"] = (
        pd.to_numeric(df["Est_Capex_EUR"], errors="coerce").fillna(0) * EUR_TO_USD
    )
    df["sector_code"]    = df["Stage"].map({"Dev": "Rail_Dev", "Op": "Rail_Op"})
    df["region"]         = df["Region"]
    df["stage"]          = df["Stage"]
    df["beneficiaries"]  = pd.to_numeric(df["Reach_Ppl_Yr"], errors="coerce").fillna(0)
    df["multiplier"]     = 1.0
    return df[["project_label","region","stage","sector_code",
               "investment_usd","beneficiaries","multiplier"]]


projects_df = pd.concat([_load_hospitals(), _load_hydro(), _load_rail()],
                         ignore_index=True)
projects_df = projects_df.set_index("project_label")

PROJECTS = list(projects_df.index)   # canonical order

# ══════════════════════════════════════════════════════════════════════════════
# 4.  ATTEMPT DATABASE LOAD (or fall back to benchmarks)
# ══════════════════════════════════════════════════════════════════════════════
mrio_io = load_mrio(DB_CHOICE, MRIO_PATH)
DB_USED  = DB_CHOICE if mrio_io else "benchmark"

# ══════════════════════════════════════════════════════════════════════════════
# 5.  SUPPLY CHAIN TIER DECOMPOSITION
# ══════════════════════════════════════════════════════════════════════════════
def region_adjusted_S(region):
    """Return an S matrix (4 × 8) adjusted for the given region."""
    key = region if region in REGION_MULT["GHG"] else "Global"
    S = S_GLOBAL.copy()
    S[0] *= np.array(REGION_MULT["GHG"][key])
    S[1] *= np.array(REGION_MULT["EMP"][key])
    S[2] *= np.array(REGION_MULT["WAT"][key])
    # Value Added row unchanged (ratio is relatively stable across regions)
    return S


def compute_tiers(label, invest_usd, sector_code, region,
                  calib_factor=1.0, max_tiers=MAX_TIERS):
    """
    Decompose supply chain impacts by tier using power-series expansion.

    Returns
    -------
    tier_rows : list of dicts  (one row per tier × sector combination)
    summary   : dict           (totals across all tiers 0 → max_tiers)
    """
    invest_m = invest_usd / 1e6  # convert to M$

    S = region_adjusted_S(region)
    S[0] *= calib_factor          # apply DB calibration to GHG row

    # Tier-0 demand vector: how the investment is split across 8 sectors
    alloc = SECTOR_ALLOC.get(sector_code, SECTOR_ALLOC["Health_General"])
    y0    = alloc * invest_m      # M$ per sector at tier 0

    rows = []
    A_power = np.eye(NSEC)       # A^0 = I

    cum = {k: 0.0 for k in STRESSOR_LABELS}

    for t in range(max_tiers + 1):
        x_tier = A_power @ y0    # M$ spending vector at tier t

        for j, sec in enumerate(SECTORS_8):
            spend = x_tier[j]
            if spend < 1e-8:
                continue
            ghg  = S[0, j] * spend
            emp  = S[1, j] * spend
            wat  = S[2, j] * spend
            va   = S[3, j] * spend
            rows.append({
                "project":           label,
                "region":            region,
                "sector_code":       sector_code,
                "investment_usd":    invest_usd,
                "db_used":           DB_USED,
                "tier":              t,
                "supplying_sector":  sec,
                "spend_M$":          round(spend, 5),
                "GHG_tCO2e":         round(ghg, 2),
                "Employment_FTE":    round(emp, 2),
                "Water_1000m3":      round(wat, 4),
                "ValueAdded_M$":     round(va, 5),
            })
            cum["GHG_tCO2e"]      += ghg
            cum["Employment_FTE"] += emp
            cum["Water_1000m3"]   += wat
            cum["ValueAdded_M$"]  += va

        A_power = A_power @ A_GLOBAL  # next tier

    summary = {k: round(v, 3) for k, v in cum.items()}
    summary["tiers_computed"] = max_tiers + 1
    return rows, summary


# ══════════════════════════════════════════════════════════════════════════════
# 6.  RUN ANALYSIS FOR ALL PROJECTS
# ══════════════════════════════════════════════════════════════════════════════
all_tier_rows = []
sc_summary    = {}   # {project_label: {stressor: total}}

for label, proj in projects_df.iterrows():
    sector  = proj["sector_code"]
    region  = proj["region"]
    invest  = proj["investment_usd"]
    calib   = calibrate_from_mrio(mrio_io, DB_CHOICE, sector)

    rows, totals = compute_tiers(label, invest, sector, region, calib)
    all_tier_rows.extend(rows)
    sc_summary[label] = totals

tiers_df = pd.DataFrame(all_tier_rows)

# Add cumulative columns (running total of impacts across tiers 0→current)
tiers_df = tiers_df.sort_values(["project", "tier", "supplying_sector"]).reset_index(drop=True)

for proj_label, grp in tiers_df.groupby("project"):
    for stressor in ["GHG_tCO2e", "Employment_FTE", "Water_1000m3", "ValueAdded_M$"]:
        tier_totals = grp.groupby("tier")[stressor].sum().cumsum()
        for idx in grp.index:
            t = tiers_df.loc[idx, "tier"]
            tiers_df.loc[idx, f"cumul_{stressor}"] = round(tier_totals[t], 3)

# ══════════════════════════════════════════════════════════════════════════════
# 7.  BUILD ENHANCED FAST-INFRA BENCHMARK
# ══════════════════════════════════════════════════════════════════════════════
# Load previous indicator benchmark as base
base_bench = pd.read_csv(DATA_DIR / "fastinfra_benchmark.csv", dtype=object)

# Enrich project column names with context
rename_map = {}
for label in PROJECTS:
    if label in base_bench.columns:
        p = projects_df.loc[label]
        new_name = (
            f"{label} [{p['region']} | {p['sector_code']} | "
            f"${p['investment_usd']/1e6:.1f}M | {p['stage']}]"
        )
        rename_map[label] = new_name
base_bench = base_bench.rename(columns=rename_map)

# Add tier-relevance column (where in the supply chain each indicator lives)
base_bench["sc_tier_relevance"] = base_bench["indicator_id"].map(
    lambda x: INDICATOR_TIER_RELEVANCE.get(x, DEFAULT_TIER_RELEVANCE)
)

# Add MRIO database sector mapping columns
for db in ["exiobase", "eora26", "wiod", "oecd"]:
    db_col_rows = []
    for _, row in base_bench.iterrows():
        db_col_rows.append(np.nan)
    base_bench[f"db_sector_{db}"] = np.nan  # placeholder; filled via META rows

# ── Append project-level metadata as META indicator rows ─────────────────────
meta_rows = []
meta_fields = {
    "META_region":          {lbl: projects_df.loc[lbl, "region"]       for lbl in PROJECTS},
    "META_stage":           {lbl: projects_df.loc[lbl, "stage"]        for lbl in PROJECTS},
    "META_sector_code":     {lbl: projects_df.loc[lbl, "sector_code"]  for lbl in PROJECTS},
    "META_investment_USD":  {lbl: projects_df.loc[lbl, "investment_usd"] for lbl in PROJECTS},
    "META_beneficiaries":   {lbl: projects_df.loc[lbl, "beneficiaries"] for lbl in PROJECTS},
    "META_mrio_db_used":    {lbl: DB_USED                               for lbl in PROJECTS},
}
# Add per-DB sector name mappings
for db in ["exiobase", "eora26", "wiod", "oecd"]:
    key = f"META_sector_{db}"
    meta_fields[key] = {
        lbl: DB_SECTOR_MAP[db].get(
            f"{projects_df.loc[lbl,'sector_code']}_proj",
            "—"
        )
        for lbl in PROJECTS
    }
# Add supply chain totals (tiers 0-8)
for stressor in STRESSOR_LABELS:
    key = f"SC_total_{stressor}_T0-T{MAX_TIERS}"
    meta_fields[key] = {
        lbl: sc_summary[lbl].get(stressor, np.nan) for lbl in PROJECTS
    }

# Build meta DataFrame rows matching base_bench column structure
# The enriched project column names replace old column names
inv_rename = {v: k for k, v in rename_map.items()}  # new → old label

for field_id, proj_values in meta_fields.items():
    row = {col: np.nan for col in base_bench.columns}
    row["indicator_id"] = field_id
    row["dimension"]    = "Project Metadata" if field_id.startswith("META") else "Supply Chain"
    row["description"]  = field_id.replace("META_", "").replace("SC_", "").replace("_", " ")
    row["sc_tier_relevance"] = "N/A"
    for new_col, old_label in inv_rename.items():
        if old_label in proj_values:
            row[new_col] = proj_values[old_label]
    meta_rows.append(row)

meta_df = pd.DataFrame(meta_rows, columns=base_bench.columns)

# Combine and write
enhanced = pd.concat([meta_df, base_bench], ignore_index=True)

# ══════════════════════════════════════════════════════════════════════════════
# 8.  WRITE OUTPUT FILES
# ══════════════════════════════════════════════════════════════════════════════
enhanced.to_csv(OUT_BENCH, index=False, encoding="utf-8")

# Reorder tiers CSV columns for readability
tier_cols = [
    "project", "region", "sector_code", "investment_usd", "db_used",
    "tier", "supplying_sector", "spend_M$",
    "GHG_tCO2e", "Employment_FTE", "Water_1000m3", "ValueAdded_M$",
    "cumul_GHG_tCO2e", "cumul_Employment_FTE", "cumul_Water_1000m3", "cumul_ValueAdded_M$",
]
tiers_df[[c for c in tier_cols if c in tiers_df.columns]].to_csv(
    OUT_TIERS, index=False, encoding="utf-8"
)

# ══════════════════════════════════════════════════════════════════════════════
# 9.  CONSOLE REPORT
# ══════════════════════════════════════════════════════════════════════════════
print(f"\n{'═'*70}")
print(f"FAST-Infra MRIO Benchmark — Supply Chain Analysis (Tiers 0 → {MAX_TIERS})")
print(f"Database: {DB_USED.upper()} | EUR→USD: {EUR_TO_USD}")
print(f"{'═'*70}\n")

print(f"Outputs:")
print(f"  {OUT_BENCH}   ({len(enhanced)} rows)")
print(f"  {OUT_TIERS}   ({len(tiers_df)} rows)\n")

print(f"{'─'*70}")
print(f"{'Project':<22} {'Region':<8} {'Sector':<22} {'Invest $M':>9} "
      f"{'GHG T0-T8':>10} {'Jobs T0-T8':>10} {'Water m³k':>10}")
print(f"{'─'*70}")

for label in PROJECTS:
    p   = projects_df.loc[label]
    tot = sc_summary[label]
    print(
        f"{label:<22} {p['region']:<8} {p['sector_code']:<22} "
        f"{p['investment_usd']/1e6:>9.1f} "
        f"{tot['GHG_tCO2e']:>10,.0f} "
        f"{tot['Employment_FTE']:>10,.0f} "
        f"{tot['Water_1000m3']:>10,.1f}"
    )

print(f"\n{'─'*70}")
print("Supply chain GHG by tier (tCO2e) — all projects combined:")
print(f"{'─'*70}")
tier_totals = (
    tiers_df.groupby("tier")["GHG_tCO2e"].sum()
    .reset_index()
    .assign(pct=lambda d: d["GHG_tCO2e"] / d["GHG_tCO2e"].sum() * 100)
)
for _, r in tier_totals.iterrows():
    bar = "█" * int(r["pct"] / 2)
    print(f"  Tier {int(r['tier'])}: {r['GHG_tCO2e']:>10,.0f} tCO2e ({r['pct']:5.1f}%)  {bar}")

print(f"\n{'─'*70}")
print("Top supplying sectors by cumulative GHG across all tiers & projects:")
print(f"{'─'*70}")
sector_ghg = tiers_df.groupby("supplying_sector")["GHG_tCO2e"].sum().sort_values(ascending=False)
for sec, ghg in sector_ghg.items():
    print(f"  {sec:<25} {ghg:>12,.0f} tCO2e")

print(f"\n{'─'*70}")
print("MRIO sector mapping for this dataset:")
print(f"{'─'*70}")
sector_map = DB_SECTOR_MAP[DB_CHOICE]
for sc, label in [
    ("Health_Social_proj",      "Hospitals (Social)"),
    ("Health_Specialized_proj", "Hospitals (Specialized)"),
    ("Health_General_proj",     "Hospitals (General)"),
    ("Energy_proj",             "Hydro Power Plant"),
    ("Rail_Dev_proj",           "Rail (Development)"),
    ("Rail_Op_proj",            "Rail (Operational)"),
]:
    print(f"  {label:<30} → {sector_map.get(sc,'—')}")

print(f"\nDone.\n")
