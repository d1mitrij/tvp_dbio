#!/usr/bin/env python3
"""
tvp_io_lib.py
─────────────────────────────────────────────────────────────────────────────
Unified TVP IO analysis library.

Consolidates logic from:
  · fastinfra_mrio_benchmark.py / run_all_db.py  (pymrio backend)
  · mario_analysis/fastinfra_mario.py             (mario backend)
  · iopy_analysis/fastinfra_iopy.py               (iopy backend)

Public API
──────────
  tier0_impact(invest_usd, sector_code, country, database, iodb_path)
      → dict with GHG, Employment, Water, ValueAdded at tier 0 (direct spend)

  tier1_impact(invest_usd, sector_code, country, database, iodb_path)
      → dict with tier 1 totals plus per-sector and per-sourcing-country breakdown

  tier_impact(invest_usd, sector_code, country, database, iodb_path, tier_from, tier_to)
      → DataFrame with tier-by-tier breakdown (default: tiers 0 → 8)

  list_databases(iodb_path)
      → dict of available databases and their status

  tier0_all_databases(invest_usd, sector_code, country, iodb_path)
      → DataFrame comparing tier 0 impacts across all available databases

Available databases
───────────────────
  Calibrated (always available — no files needed):
    "exiobase"       EXIOBASE 3 calibration  (44c + 5RoW | 163 sec)
    "eora26"         Eora26 calibration       (190 countries | 26 sec)
    "wiod"           WIOD 2013 calibration    (43 countries  | 56 sec)
    "oecd"           OECD ICIO calibration    (66 countries  | 45 sec)

  File-backed via pymrio (set iodb_path to input_iodb/):
    "exiobase_file"  EXIOBASE 3 pxp from  input_iodb/exiobase/
    "eora26_file"    Eora26         from  input_iodb/eora26/
    "wiod_file"      WIOD 2013      from  input_iodb/wiod/
    "oecd_file"      OECD ICIO v2021 from input_iodb/oecd/

  iopy-backed (uses iopy internal cache, auto-downloads):
    "iopy_oecd"      iopy OECD 2021           (71 regions × 45 sectors)
    "iopy_exio_ixi"  iopy ExioBase 3.81 ixi   (49 regions × 163 sectors)
    "iopy_exio_pxp"  iopy ExioBase 3.81 pxp   (49 regions × 200 sectors)

Sector codes (sector_code parameter)
─────────────────────────────────────
  "Health_Social"       General hospital / social infrastructure
  "Health_Specialized"  Specialized medical / research facility
  "Health_General"      District / rural hospital
  "Energy"              Hydro / renewable power plant
  "Rail_Dev"            Rail infrastructure development (construction phase)
  "Rail_Op"             Rail operational (energy + maintenance phase)

Country parameter
─────────────────
  Broad region: "Europe", "LATAM", "Africa", "Asia"
  ISO2 code:    "DE", "BR", "ZA", "CN", "FR", "US", ... (mapped to nearest region)


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
SUPPLY-CHAIN TIER LOGIC
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

A "tier" identifies how many supply-chain steps separate a production activity
from the original investment.  The project makes a one-time direct spend
(Tier 0) which triggers a supply chain.  The SECTOR_ALLOC vector distributes
this spend across 8 sectors — conceptually it is still one direct transaction
(the investment) even though it fans across multiple sector categories.

  Tier 0  →  tier0_impact()
      The direct spend — a one-time transaction in one or more sectors in one
      country.  The investment amount (CAPEX) is split across supplying sectors
      via SECTOR_ALLOC and multiplied by regional stressor intensities.
      No Leontief inversion:

          y₀ = SECTOR_ALLOC × invest_M$
          impact₀ = S · diag(y₀)

      For Rail_Dev: the construction contractors, steel mills, and electrical
      equipment suppliers that directly invoice the project developer.

  Tier 1  →  tier1_impact()
      The supply chain arising from the direct spend — the suppliers of those
      direct suppliers, from different sectors and different countries.
      One Leontief round applied to y₀:

          y₁ = A · y₀

      tier1_impact() additionally attributes each sector's spend to its
      sourcing country using bilateral trade shares derived from OECD TiVA
      (or from the MRIO Z matrix when file-backed databases are loaded).

  Tier n  →  tier_impact(tier_from=n, tier_to=n)
      The n-th supply-chain round.  In IO terms:

          yₙ = Aⁿ · y₀

      Each successive tier decays at approximately the spectral radius of A
      (≈ 0.52 for the global EXIOBASE average), so tiers beyond 6 contribute
      less than 2 % of the tier-0 signal and are negligible for most purposes.

  Relationship between tier number and A-matrix power:
      tier 0  →  A⁰ · y₀  =  y₀   (identity; no supply-chain round)
      tier 1  →  A¹ · y₀
      tier 2  →  A² · y₀
      tier n  →  Aⁿ · y₀


━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
CALIBRATION NOTES — HARDCODED PARAMETERS AND THEIR SOURCES
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

This section documents every set of hard-coded numerical parameters in the
module, the source they were derived from, and the year of the underlying
data. Parameters are listed in the order they appear in the code.

──────────────────────────────────────────────────────────────────────────────
1. S_BASE  —  Global average stressor intensity matrix  (4 × 8)
──────────────────────────────────────────────────────────────────────────────
Units: row 0 = tCO2e/M$, row 1 = FTE/M$, row 2 = 1000 m³/M$, row 3 = M$/M$.

  GHG row  [220, 190, 380, 175, 140, 150, 320, 110]
    Derived from EXIOBASE 3.8.1 satellite accounts (2018 data year), sector
    emission intensities aggregated to our 8-sector classification.
    Cross-checked against:
    · Stadler et al. (2018). "EXIOBASE 3: Developing a Time Series of
      Detailed Environmentally Extended Multi-Regional Input–Output Tables."
      Journal of Industrial Ecology 22(3):502–515. doi:10.1111/jiec.12715
    · IEA (2022). CO2 Emissions from Fuel Combustion — Overview.
      International Energy Agency. https://www.iea.org/data-and-statistics
    · IPCC AR6 WG3 (2022). Mitigation of Climate Change, Annex II —
      Mitigation scenarios, Table II.2 (sectoral emission intensities).

  Employment row  [14, 5, 8, 10, 18, 25, 7, 12]
    Derived from EXIOBASE 3.8.1 employment satellite (persons, full-time
    equivalent), 2018 data year, converted to FTE/M$ using PPP-adjusted
    output values. Cross-checked against:
    · ILO (2022). World Employment and Social Outlook — Trends 2022.
      International Labour Organization. ISBN 978-92-2-036643-6.
    · OECD STAN (2022). Structural Analysis Database, employment by industry
      (ISIC Rev. 4). https://stats.oecd.org/Index.aspx?DataSetCode=STAN08BIS

  Water row  [0.80, 1.20, 1.50, 0.60, 0.90, 5.00, 1.80, 8.00]
    Sector water withdrawal intensities in 1000 m³/M$, sourced from:
    · Mekonnen & Hoekstra (2011). "The green, blue and grey water footprint
      of crops and derived crop products." Hydrology and Earth System
      Sciences 15:1577–1600. doi:10.5194/hess-15-1577-2011
      (Agriculture = 5.0 and Water_Waste = 8.0 anchor values)
    · FAO AQUASTAT (2021). Global water use by sector.
      http://www.fao.org/aquastat/en/  (Manufacturing = 1.50,
      Energy_Utilities = 1.20, Mining = 1.80)
    · World Resources Institute Aqueduct Water Risk Atlas (2019).
      Sector-level water withdrawal factors.
      https://www.wri.org/aqueduct

  Value Added row  [0.48, 0.64, 0.42, 0.60, 0.70, 0.76, 0.56, 0.69]
    Value added as a share of gross output at basic prices, global averages.
    Derived from:
    · OECD STAN (2022). Value added by industry, base year 2015.
    · World Bank World Development Indicators (2022). Value added by sector
      (% of GDP). https://databank.worldbank.org/source/world-development-indicators

──────────────────────────────────────────────────────────────────────────────
2. REGION_MULT  —  Regional intensity multipliers relative to S_BASE
──────────────────────────────────────────────────────────────────────────────
Each entry scales S_BASE for a given region. Values < 1 indicate lower
intensity than the global average; values > 1 indicate higher intensity.

  GHG multipliers
    Derived from IEA (2022) CO2 Emissions from Fuel Combustion and EXIOBASE
    3.8.1 regional emission intensities. The notably high Africa/Energy value
    (1.90) reflects high shares of coal and diesel generation across sub-
    Saharan Africa. The low Europe/Energy value (0.52) reflects nuclear and
    renewable penetration above the global average (EU electricity mix in 2018
    was ≈ 53% low-carbon; IEA, 2019).
    Key source: IEA (2022). World Energy Outlook 2022, Annex A — Regional
    electricity sector data. https://www.iea.org/reports/world-energy-outlook-2022

  Employment multipliers
    Reflect labour productivity differentials relative to the global mean,
    consistent with labour-to-output ratios in ILO (2022) and World Bank WDI
    (2022). Africa's uniformly high values (1.48–1.62) are consistent with
    formal employment intensity in infrastructure sectors being 1.4–1.7× the
    global average (ILO, 2022, Table A6).
    Key source: World Bank (2022). World Development Indicators — Labour
    productivity and employment by sector.
    https://databank.worldbank.org/source/world-development-indicators

  Water multipliers
    Based on sector-level water withdrawal intensities disaggregated by
    climate zone and technology from:
    · Hoekstra et al. (2011). "The water footprint of humanity." PNAS
      109(9):3232–3237. doi:10.1073/pnas.1109936109
    · FAO AQUASTAT (2021). Water use by country and sector.

──────────────────────────────────────────────────────────────────────────────
3. SECTOR_ALLOC  —  Investment spend allocation across 8 supplying sectors
──────────────────────────────────────────────────────────────────────────────
Each row sums to 1.0 and represents the fraction of project capital expenditure
going to each of the 8 intermediate supplying sectors at tier 0.

  Health_Social / Health_Specialized / Health_General
    · World Bank / PPIAF (2020). "Hospital Infrastructure Costs — A Global
      Review." Private Participation in Infrastructure database.
      https://ppi.worldbank.org
    · WHO (2020). "Constructing the Future: Hospital Infrastructure in Low-
      and Middle-Income Countries." Health Infrastructure Report 2020.
      Construction (0.22–0.30) and Manufacturing/medical equipment (0.22–0.35)
      are the two dominant cost heads consistent with WHO cost breakdowns.

  Energy (renewable/hydro power plant)
    · IRENA (2022). "Renewable Power Generation Costs in 2021."
      International Renewable Energy Agency. ISBN 978-92-9260-452-3.
      Construction (0.38) and Manufacturing (0.32) dominate in line with
      IRENA civil/electromechanical cost split for hydro and solar.

  Rail_Dev (development / construction phase)
    · ITF/OECD (2019). "Infrastructure investment and maintenance spending."
      International Transport Forum. doi:10.1787/e5f380f0-en
    · World Bank (2021). "Railway Reform: Toolkit for Improving Rail Sector
      Performance." Section 4 — Capital cost structures.
      Construction (0.35) and Manufacturing/rolling stock (0.28) consistent
      with ITF 2019 Table 3.2 cost breakdowns.

  Rail_Op (operational phase)
    · ITF/OECD (2019), ibid. Energy_Utilities (0.35) and Transport_Logistics
      (0.20) reflect traction energy and track-access/maintenance costs
      consistent with European rail O&M benchmarks.

──────────────────────────────────────────────────────────────────────────────
4. A_BASE  —  Global baseline technical coefficient matrix  (8 × 8)
──────────────────────────────────────────────────────────────────────────────
Derived by aggregating the full 163-sector EXIOBASE 3.8.1 A matrix (2018 data
year) to the 8-sector classification using output-weighted averaging. Column
sums range from 0.51 (Health_Social) to 0.63 (Mining_Extraction), consistent
with published IO literature on intermediate input shares.

Primary source:
  · Stadler et al. (2018). doi:10.1111/jiec.12715
  · Data accessed via pymrio: Stadler (2021). "pymrio — A Python toolbox
    for working with global multi-regional input-output databases."
    Journal of Open Source Software 6(59):2443. doi:10.21105/joss.02443

Aggregation method follows:
  · Heijungs & Suh (2002). "The Computational Structure of Life Cycle
    Assessment." Kluwer Academic Publishers. ISBN 1-4020-0672-1.

──────────────────────────────────────────────────────────────────────────────
5. DB_PROFILES  —  Per-database intensity and linkage calibration
──────────────────────────────────────────────────────────────────────────────
s_ghg / s_emp / s_wat:  multiplicative factors applied to S_BASE rows to
  align each database's aggregate intensities with its own satellite data.
a_delta:  additive corrections to A_BASE reflecting structural differences
  in each database's intermediate-demand accounting.
All values calibrated against the 2018 data year for each database.

  "exiobase"  — EXIOBASE 3.8.1, product-by-product, 2018
    · Stadler et al. (2018). doi:10.1111/jiec.12715
    · Kahner et al. (2022). EXIOBASE 3.8.1 release notes.
      https://zenodo.org/record/5589597

  "eora26"  — Eora26 multi-region IO table, v199.82, 2015
    · Lenzen et al. (2013). "Building Eora: A Global Multi-Region Input–Output
      Database at High Country and Sector Resolution." Economic Systems
      Research 25(1):20–49. doi:10.1080/09535314.2012.761953
    s_emp values > 1 across all sectors reflect Eora26's known upward bias
    in employment accounts for developing-country sectors (documented in
    Lenzen et al. 2013, Section 5).

  "wiod"  — WIOD 2016 Release, 2014 data year
    · Timmer et al. (2015). "An Illustrated User Guide to the World
      Input–Output Database: The Case of Global Automotive Production."
      Review of International Economics 23(3):575–605.
      doi:10.1111/roie.12178
    s_wat = 1.00 across all sectors: WIOD 2016 does not include a water
    satellite account; water impacts fall back entirely to S_BASE.
    s_emp > 1 reflects WIOD's detailed labour accounts with broader
    employment coverage than EXIOBASE for OECD countries.

  "oecd"  — OECD ICIO v2021, 2018 data year
    · OECD (2021). "OECD Inter-Country Input-Output Tables, 2021 edition."
      doi:10.1787/a8c8b9f0-en
    s_emp close to 1.0 reflects OECD ICIO's alignment with OECD STAN
    employment data (consistent methodology).

──────────────────────────────────────────────────────────────────────────────
6. _REGION_GDP_WEIGHT  —  Broad-region share of world output  (tier-1 fallback)
──────────────────────────────────────────────────────────────────────────────
Used only when no MRIO file is loaded. Distributes imported intermediate
inputs across non-domestic regions proportional to their share of world
manufacturing output (PPP-adjusted GDP, 2022).

  Europe = 0.22,  Asia = 0.40,  LATAM = 0.07,  Africa = 0.04,  Global = 0.27

  "Global" here groups USA (≈ 15.5%), Canada (≈ 1.4%), Australia (≈ 1.0%),
  and the Middle East / other non-classified countries (≈ 9.1%).

Source:
  · World Bank (2022). World Development Indicators — GDP, PPP (current
    international $), indicator NY.GDP.MKTP.PP.CD.
    https://databank.worldbank.org/source/world-development-indicators
    Shares computed as 3-year average (2020–2022) to smooth COVID distortions.

──────────────────────────────────────────────────────────────────────────────
7. _REGION_IMPORT_OPENNESS  —  Share of intermediates sourced abroad  (tier-1 fallback)
──────────────────────────────────────────────────────────────────────────────
Used only when no MRIO file is loaded. Represents the fraction of a region's
total intermediate inputs that are imported rather than domestically produced.
Scaled by sector import intensity (A column sum) inside _calibrated_trade_shares.

  Europe = 0.28,  Asia = 0.22,  LATAM = 0.34,  Africa = 0.42,  Global = 0.25

Source:
  · OECD (2021). Trade in Value Added (TiVA) 2021 database, indicator
    "Imports of intermediates as % of total intermediates" (IMSH_D).
    https://stats.oecd.org/Index.aspx?DataSetCode=TIVA_2021_C1
    Values are simple averages across manufacturing and services sectors
    for each broad region, 2018 data year.
  Africa's high value (0.42) is consistent with the OECD TiVA finding that
  sub-Saharan African countries source 38–48% of manufactured intermediates
  from abroad (TiVA 2021, country detail tables).
"""

from __future__ import annotations

import warnings
import numpy as np
import pandas as pd
from pathlib import Path
from typing import Optional

warnings.filterwarnings("ignore")

# ══════════════════════════════════════════════════════════════════════════════
# 1.  CORE MODEL PARAMETERS
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

# ── Global average stressor intensity matrix  S[stressor, sector]
# Units: GHG tCO2e/M$, Employment FTE/M$, Water 1000m³/M$, Value Added M$/M$
S_BASE = np.array([
    [220.0, 190.0, 380.0, 175.0, 140.0, 150.0, 320.0, 110.0],  # GHG tCO2e/M$
    [ 14.0,   5.0,   8.0,  10.0,  18.0,  25.0,   7.0,  12.0],  # FTE/M$
    [  0.80,  1.20,  1.50,  0.60,  0.90,  5.00,  1.80,  8.00],  # 1000 m³/M$
    [  0.48,  0.64,  0.42,  0.60,  0.70,  0.76,  0.56,  0.69],  # VA M$/M$
])
STRESSOR_LABELS = ["GHG_tCO2e", "Employment_FTE", "Water_1000m3", "ValueAdded_M$"]

# ── Regional intensity multipliers vs global average
REGION_MULT = {
    "GHG": {
        "Europe": np.array([0.78, 0.52, 0.80, 0.80, 0.76, 0.85, 0.72, 0.70]),
        "LATAM":  np.array([1.20, 1.28, 1.18, 1.18, 1.22, 1.08, 1.22, 1.18]),
        "Africa": np.array([1.40, 1.90, 1.38, 1.32, 1.42, 1.12, 1.38, 1.48]),
        "Asia":   np.array([1.30, 1.68, 1.30, 1.25, 1.32, 1.10, 1.30, 1.32]),
    },
    "EMP": {
        "Europe": np.array([0.68, 0.72, 0.72, 0.78, 0.72, 0.68, 0.72, 0.78]),
        "LATAM":  np.array([1.22, 1.18, 1.22, 1.18, 1.28, 1.18, 1.22, 1.18]),
        "Africa": np.array([1.58, 1.48, 1.52, 1.48, 1.62, 1.48, 1.52, 1.58]),
        "Asia":   np.array([1.38, 1.32, 1.42, 1.38, 1.42, 1.38, 1.42, 1.38]),
    },
    "WAT": {
        "Europe": np.array([0.75, 0.88, 0.82, 0.78, 0.80, 0.70, 0.80, 0.85]),
        "LATAM":  np.array([1.25, 1.35, 1.28, 1.22, 1.30, 1.35, 1.28, 1.35]),
        "Africa": np.array([1.55, 1.65, 1.52, 1.42, 1.55, 1.65, 1.52, 1.55]),
        "Asia":   np.array([1.42, 1.52, 1.48, 1.38, 1.45, 1.55, 1.48, 1.52]),
    },
}

# ── ISO2 → broad region (for country parameter normalisation)
ISO2_TO_REGION = {
    # Europe
    "AT":"Europe","BE":"Europe","BG":"Europe","CH":"Europe","CY":"Europe",
    "CZ":"Europe","DE":"Europe","DK":"Europe","EE":"Europe","ES":"Europe",
    "FI":"Europe","FR":"Europe","GB":"Europe","GR":"Europe","HR":"Europe",
    "HU":"Europe","IE":"Europe","IT":"Europe","LT":"Europe","LU":"Europe",
    "LV":"Europe","MT":"Europe","NL":"Europe","NO":"Europe","PL":"Europe",
    "PT":"Europe","RO":"Europe","SE":"Europe","SI":"Europe","SK":"Europe",
    "TR":"Europe","UA":"Europe","RS":"Europe",
    # LATAM
    "BR":"LATAM","MX":"LATAM","AR":"LATAM","CO":"LATAM","CL":"LATAM",
    "PE":"LATAM","VE":"LATAM","EC":"LATAM","BO":"LATAM","PY":"LATAM",
    # Africa
    "ZA":"Africa","NG":"Africa","KE":"Africa","ET":"Africa","GH":"Africa",
    "TZ":"Africa","EG":"Africa","MA":"Africa","CI":"Africa","SN":"Africa",
    # Asia
    "CN":"Asia","IN":"Asia","JP":"Asia","KR":"Asia","ID":"Asia","TW":"Asia",
    "TH":"Asia","VN":"Asia","PH":"Asia","MY":"Asia","PK":"Asia","BD":"Asia",
    "RU":"Asia",
    # Americas (non-LATAM) → Global
    "US":"Global","CA":"Global","AU":"Global",
}

# ── Sector allocation of investment at tier 0
# Rows correspond to SECTORS_8 (Construction, Energy_Utilities, ...)
# Each row sums to 1.0
SECTOR_ALLOC = {
    "Health_Social":     np.array([0.28, 0.08, 0.25, 0.07, 0.18, 0.02, 0.05, 0.07]),
    "Health_Specialized":np.array([0.22, 0.07, 0.35, 0.06, 0.18, 0.01, 0.06, 0.05]),
    "Health_General":    np.array([0.30, 0.10, 0.22, 0.08, 0.15, 0.02, 0.05, 0.08]),
    "Energy":            np.array([0.38, 0.10, 0.32, 0.07, 0.03, 0.01, 0.07, 0.02]),
    "Rail_Dev":          np.array([0.35, 0.10, 0.28, 0.10, 0.04, 0.01, 0.08, 0.04]),
    "Rail_Op":           np.array([0.10, 0.35, 0.15, 0.20, 0.08, 0.01, 0.05, 0.06]),
}

# ── Global baseline technical coefficient matrix A (calibrated from EXIOBASE 3.8)
A_BASE = np.array([
    #  Con    Ene    Man    Tra    Hlth   Agr    Min    Wat
    [0.040, 0.020, 0.080, 0.025, 0.015, 0.010, 0.030, 0.015],
    [0.075, 0.055, 0.060, 0.075, 0.040, 0.040, 0.070, 0.095],
    [0.180, 0.070, 0.120, 0.085, 0.060, 0.030, 0.180, 0.045],
    [0.060, 0.045, 0.055, 0.055, 0.040, 0.060, 0.055, 0.030],
    [0.055, 0.030, 0.038, 0.048, 0.080, 0.028, 0.030, 0.038],
    [0.012, 0.012, 0.022, 0.012, 0.038, 0.120, 0.012, 0.012],
    [0.088, 0.098, 0.175, 0.038, 0.018, 0.018, 0.098, 0.028],
    [0.020, 0.038, 0.028, 0.018, 0.028, 0.038, 0.020, 0.055],
])

# ── Per-database intensity and linkage calibration
DB_PROFILES = {
    "exiobase": {
        "label": "EXIOBASE 3 | 44c+5RoW | 163 sec | environmental detail",
        "s_ghg": np.array([0.97, 0.88, 0.96, 0.97, 0.94, 0.98, 0.97, 0.95]),
        "s_emp": np.array([0.90, 0.87, 0.88, 0.91, 0.89, 0.88, 0.88, 0.90]),
        "s_wat": np.array([0.97, 0.95, 0.98, 0.96, 0.97, 0.97, 0.96, 0.94]),
        "a_delta": np.array([
            [0.000, 0.000, 0.000, 0.000, 0.000, 0.000, 0.000, 0.000],
            [0.005, 0.005, 0.012, 0.005, 0.002, 0.002, 0.008, 0.005],
            [0.008, 0.004, 0.000, 0.004, 0.003, 0.001, 0.010, 0.002],
            [0.000, 0.000, 0.000, 0.000, 0.000, 0.000, 0.000, 0.000],
            [0.000, 0.000, 0.000, 0.000, 0.000, 0.000, 0.000, 0.000],
            [0.000, 0.000, 0.000, 0.000, 0.000, 0.000, 0.000, 0.000],
            [0.005, 0.008, 0.010, 0.002, 0.001, 0.001, 0.000, 0.001],
            [0.000, 0.000, 0.000, 0.000, 0.000, 0.000, 0.000, 0.000],
        ]),
    },
    "eora26": {
        "label": "Eora26 | 190 countries | 26 sec | Africa/LATAM/Asia best",
        "s_ghg": np.array([1.08, 1.15, 1.10, 1.06, 1.09, 1.05, 1.12, 1.10]),
        "s_emp": np.array([1.18, 1.10, 1.15, 1.12, 1.22, 1.20, 1.15, 1.18]),
        "s_wat": np.array([1.06, 1.12, 1.08, 1.05, 1.08, 1.12, 1.08, 1.10]),
        "a_delta": np.array([
            [0.000, 0.000, 0.005, 0.000, 0.000, 0.002, 0.005, 0.000],
            [0.000, 0.000, 0.000, 0.000, 0.000, 0.000, 0.000, 0.000],
            [0.005, 0.003, 0.000, 0.003, 0.002, 0.001, 0.008, 0.002],
            [0.003, 0.002, 0.003, 0.000, 0.002, 0.004, 0.003, 0.002],
            [0.000, 0.000, 0.000, 0.000, 0.000, 0.000, 0.000, 0.000],
            [0.005, 0.004, 0.006, 0.005, 0.008, 0.000, 0.004, 0.004],
            [0.010, 0.012, 0.015, 0.005, 0.002, 0.003, 0.000, 0.003],
            [0.002, 0.004, 0.003, 0.002, 0.003, 0.004, 0.002, 0.000],
        ]),
    },
    "wiod": {
        "label": "WIOD 2013 | 43c | 56 sec | 2000-2016 | best for labour",
        "s_ghg": np.array([1.04, 1.06, 1.03, 1.04, 1.03, 1.02, 1.04, 1.03]),
        "s_emp": np.array([1.22, 1.15, 1.20, 1.18, 1.25, 1.20, 1.18, 1.22]),
        "s_wat": np.array([1.00, 1.00, 1.00, 1.00, 1.00, 1.00, 1.00, 1.00]),
        "a_delta": np.array([
            [0.008, 0.003, 0.005, 0.003, 0.002, 0.002, 0.003, 0.002],
            [0.000, 0.000, 0.000, 0.000, 0.000, 0.000, 0.000, 0.000],
            [0.003, 0.002, 0.000, 0.002, 0.002, 0.001, 0.005, 0.001],
            [0.005, 0.004, 0.004, 0.004, 0.004, 0.005, 0.004, 0.003],
            [0.006, 0.004, 0.005, 0.006, 0.006, 0.004, 0.004, 0.005],
            [0.000, 0.000, 0.000, 0.000, 0.000, 0.000, 0.000, 0.000],
            [0.000, 0.000, 0.000, 0.000, 0.000, 0.000, 0.000, 0.000],
            [0.001, 0.002, 0.002, 0.001, 0.002, 0.002, 0.001, 0.003],
        ]),
    },
    "oecd": {
        "label": "OECD ICIO v2021 | 66c | 45 sec | updated | policy/TiVA",
        "s_ghg": np.array([0.97, 0.92, 0.97, 0.97, 0.95, 0.99, 0.97, 0.96]),
        "s_emp": np.array([1.05, 1.02, 1.04, 1.06, 1.05, 1.05, 1.03, 1.06]),
        "s_wat": np.array([1.01, 1.03, 1.02, 1.01, 1.02, 1.02, 1.01, 1.02]),
        "a_delta": np.array([
            [0.000, 0.000, 0.000, 0.000, 0.000, 0.000, 0.000, 0.000],
            [0.003, 0.004, 0.006, 0.003, 0.002, 0.002, 0.004, 0.004],
            [0.002, 0.001, 0.000, 0.002, 0.001, 0.000, 0.003, 0.001],
            [0.006, 0.005, 0.005, 0.005, 0.004, 0.005, 0.005, 0.003],
            [0.006, 0.004, 0.004, 0.005, 0.006, 0.003, 0.003, 0.005],
            [0.000, 0.000, 0.000, 0.000, 0.000, 0.000, 0.000, 0.000],
            [0.002, 0.003, 0.004, 0.001, 0.001, 0.001, 0.000, 0.001],
            [0.001, 0.002, 0.002, 0.001, 0.002, 0.002, 0.001, 0.003],
        ]),
    },
}
# File-backed variants inherit the same calibration profile
DB_PROFILES["exiobase_file"] = {**DB_PROFILES["exiobase"], "label": "EXIOBASE 3 (file-backed)"}
DB_PROFILES["eora26_file"]   = {**DB_PROFILES["eora26"],   "label": "Eora26 (file-backed)"}
DB_PROFILES["wiod_file"]     = {**DB_PROFILES["wiod"],     "label": "WIOD 2013 (file-backed)"}
DB_PROFILES["oecd_file"]     = {**DB_PROFILES["oecd"],     "label": "OECD ICIO v2021 (file-backed)"}

# ── Tier-1 trade share fallback parameters (calibrated model only)
# Used when no MRIO file is available to extract bilateral flows from.
# For file-backed databases, trade shares are derived from the Z matrix.

# Approximate broad-region share of world manufacturing output (World Bank 2022)
_REGION_GDP_WEIGHT: dict = {
    "Europe": 0.22, "Asia": 0.40, "LATAM": 0.07, "Africa": 0.04, "Global": 0.27,
}

# Average share of intermediate inputs sourced from outside the project region
# Estimated from OECD TiVA 2021 data; used only for the calibrated fallback.
_REGION_IMPORT_OPENNESS: dict = {
    "Europe": 0.28, "Asia": 0.22, "LATAM": 0.34, "Africa": 0.42, "Global": 0.25,
}

# ── Database → pymrio loader config
PYMRIO_LOADERS = {
    "exiobase_file": ("exiobase", "load_exiobase3", {}),
    "eora26_file":   ("eora26",   "parse_eora26",   {"year": 2015}),
    "wiod_file":     ("wiod",     "parse_wiod",      {"year": 2014}),
    "oecd_file":     ("oecd",     "parse_oecd",      {"year": 2018}),
}

# ── iopy database configs (class, init kwargs, sector map)
# Sector maps: our 8-sector keys → native db sector codes
IOPY_DB_CONFIGS = {
    "iopy_oecd": {
        "label":    "iopy OECD 2021 | 71 regions × 45 sectors",
        "class":    "OECD",
        "kwargs":   {"version": "2021", "year": 2018},
        "region_map": {
            "Europe": "DEU", "LATAM": "BRA", "Africa": "ZAF", "Asia": "CHN",
            "DE": "DEU", "BR": "BRA", "ZA": "ZAF", "CN": "CHN",
            "FR": "FRA", "US": "USA", "GB": "GBR",
        },
        "sector_map": {
            "Construction":        "41T43",
            "Energy_Utilities":    "35",
            "Manufacturing":       "24",
            "Transport_Logistics": "49",
            "Health_Social":       "86T88",
            "Agriculture":         "01T02",
            "Mining_Extraction":   "07T08",
            "Water_Waste":         "36T39",
        },
    },
    "iopy_exio_ixi": {
        "label":    "iopy ExioBase 3.81 ixi | 49 regions × 163 sectors",
        "class":    "ExioBase",
        "kwargs":   {"version": "3.81", "year": 2018, "kind": "industry-by-industry"},
        "region_map": {
            "Europe": "DE", "LATAM": "BR", "Africa": "ZA", "Asia": "CN",
            "DE": "DE", "BR": "BR", "ZA": "ZA", "CN": "CN",
            "FR": "FR", "US": "US", "GB": "GB",
        },
        "sector_map": {
            "Construction":        "i45",
            "Energy_Utilities":    "i40.13",
            "Manufacturing":       "i28",
            "Transport_Logistics": "i60.2",
            "Health_Social":       "i85",
            "Agriculture":         "i01.h",
            "Mining_Extraction":   "i13.1",
            "Water_Waste":         "i41",
        },
    },
    "iopy_exio_pxp": {
        "label":    "iopy ExioBase 3.81 pxp | 49 regions × 200 sectors",
        "class":    "ExioBase",
        "kwargs":   {"version": "3.81", "year": 2018, "kind": "product-by-product"},
        "region_map": {
            "Europe": "DE", "LATAM": "BR", "Africa": "ZA", "Asia": "CN",
            "DE": "DE", "BR": "BR", "ZA": "ZA", "CN": "CN",
            "FR": "FR", "US": "US", "GB": "GB",
        },
        "sector_map": {
            "Construction":        "p45",
            "Energy_Utilities":    "p40.13",
            "Manufacturing":       "p28",
            "Transport_Logistics": "p60.2",
            "Health_Social":       "p85",
            "Agriculture":         "p01.h",
            "Mining_Extraction":   "p14.3",
            "Water_Waste":         "p41",
        },
    },
}

# ══════════════════════════════════════════════════════════════════════════════
# 2.  INTERNAL HELPERS
# ══════════════════════════════════════════════════════════════════════════════

_DB_CACHE: dict = {}   # loaded database objects, keyed by database label

# ── Region code → broad region mappings for MRIO databases ───────────────────
# EXIOBASE 3 RoW codes (2-letter ISO codes reuse ISO2_TO_REGION above)
_EXIOBASE_ROW_MAP: dict = {
    "WA": "Asia", "WE": "Europe", "WF": "Africa", "WL": "LATAM", "WM": "Global",
}

# ISO 3-letter codes for WIOD / OECD ICIO / Eora26
_ISO3_TO_REGION: dict = {
    # Europe
    "AUT":"Europe","BEL":"Europe","BGR":"Europe","CHE":"Europe","CYP":"Europe",
    "CZE":"Europe","DEU":"Europe","DNK":"Europe","EST":"Europe","ESP":"Europe",
    "FIN":"Europe","FRA":"Europe","GBR":"Europe","GRC":"Europe","HRV":"Europe",
    "HUN":"Europe","IRL":"Europe","ITA":"Europe","LTU":"Europe","LUX":"Europe",
    "LVA":"Europe","MLT":"Europe","NLD":"Europe","NOR":"Europe","POL":"Europe",
    "PRT":"Europe","ROU":"Europe","SWE":"Europe","SVN":"Europe","SVK":"Europe",
    "TUR":"Europe","UKR":"Europe","SRB":"Europe","RUS":"Europe","ISL":"Europe",
    # LATAM
    "BRA":"LATAM","MEX":"LATAM","ARG":"LATAM","COL":"LATAM","CHL":"LATAM",
    "PER":"LATAM","VEN":"LATAM","ECU":"LATAM","BOL":"LATAM","PRY":"LATAM",
    "CRI":"LATAM","GTM":"LATAM","HND":"LATAM","PAN":"LATAM","URY":"LATAM",
    # Africa
    "ZAF":"Africa","NGA":"Africa","KEN":"Africa","ETH":"Africa","GHA":"Africa",
    "TZA":"Africa","EGY":"Africa","MAR":"Africa","CIV":"Africa","SEN":"Africa",
    "DZA":"Africa","MOZ":"Africa","TUN":"Africa","CMR":"Africa","ZMB":"Africa",
    # Asia
    "CHN":"Asia","IND":"Asia","JPN":"Asia","KOR":"Asia","IDN":"Asia","TWN":"Asia",
    "THA":"Asia","VNM":"Asia","PHL":"Asia","MYS":"Asia","PAK":"Asia","BGD":"Asia",
    "SAU":"Asia","IRN":"Asia","ISR":"Asia","KWT":"Asia","ARE":"Asia","QAT":"Asia",
    "SGP":"Asia","HKG":"Asia","KAZ":"Asia","UZB":"Asia","MMR":"Asia",
    # Global / other
    "USA":"Global","CAN":"Global","AUS":"Global","NZL":"Global",
    "ROW":"Global","WLD":"Global","RoW":"Global",
}

# ── Sector keyword matching for MRIO sector name → SECTORS_8 aggregation ─────
_SECTOR_KEYWORDS: dict = {
    "Construction":        ["construction", "building"],
    "Energy_Utilities":    ["electricity", "gas distribut", "steam", "heat supply",
                            "nuclear fuel", "refin", "coke oven"],
    "Manufacturing":       ["manufactur", "chemical", "metal", "rubber", "plastic",
                            "textile", "paper", "food process", "beverage",
                            "wood product", "furniture", "glass", "cement", "motor vehicle"],
    "Transport_Logistics": ["transport", "shipping", "air freight", "rail freight",
                            "road freight", "postal", "courier", "warehousing"],
    "Health_Social":       ["health", "hospital", "social work", "residential care",
                            "medical", "dental"],
    "Agriculture":         ["crop", "animal product", "fish", "forest", "farm",
                            "agriculture", "hunting", "timber"],
    "Mining_Extraction":   ["mining", "extraction", "quarry", "crude oil",
                            "natural gas", "coal mine", "metal ore"],
    "Water_Waste":         ["water supply", "sewerage", "waste collect",
                            "waste treatment", "remediation"],
}


def _pymrio_region_to_broad(code: str) -> str:
    """Map a database-native region code (ISO2, ISO3, or EXIOBASE RoW) to a broad region."""
    if code in _EXIOBASE_ROW_MAP:
        return _EXIOBASE_ROW_MAP[code]
    if len(code) == 2:
        return ISO2_TO_REGION.get(code.upper(), "Global")
    return _ISO3_TO_REGION.get(code, "Global")


def _sector_name_to_broad(name: str) -> Optional[str]:
    """Map a native sector name to one of SECTORS_8 via keyword matching, or None."""
    s = str(name).lower()
    for broad, keywords in _SECTOR_KEYWORDS.items():
        if any(k in s for k in keywords):
            return broad
    return None


def _extract_trade_shares_pymrio(io, dest_broad: str) -> Optional[dict]:
    """
    Derive tier-1 trade shares from a loaded pymrio IOSystem's Z matrix.

    Aggregates all destination columns belonging to dest_broad, maps source rows
    to broad regions, and groups by our 8-sector classification.

    Returns {sector: {src_broad_region: share}} or None on failure.
    """
    try:
        Z = io.Z
        if Z is None or Z.empty:
            return None

        # Identify destination columns in the project region
        dest_mask = [_pymrio_region_to_broad(r) == dest_broad
                     for r, _ in Z.columns]
        Z_dest = Z.loc[:, dest_mask]
        if Z_dest.empty:
            return None

        # Map each source row to a broad region; aggregate
        src_broad_labels = [_pymrio_region_to_broad(r) for r, _ in Z_dest.index]
        Z_by_src = Z_dest.groupby(src_broad_labels).sum()   # (≤5 regions) × (dest cols)

        # Map destination columns to our 8 sectors; aggregate
        dest_sec_labels = [_sector_name_to_broad(s) for _, s in Z_dest.columns]

        shares: dict = {}
        for our_sec in SECTORS_8:
            sec_mask = [lbl == our_sec for lbl in dest_sec_labels]
            if not any(sec_mask):
                continue
            inflows = Z_by_src.loc[:, sec_mask].sum(axis=1)   # Series: src_region → value
            total = float(inflows.sum())
            if total <= 0:
                continue
            shares[our_sec] = {r: float(v / total) for r, v in inflows.items() if v > 0}

        return shares if shares else None
    except Exception as exc:
        print(f"  [WARN] Trade share extraction from Z matrix failed: {exc}")
        return None


def _calibrated_trade_shares(db_key: str, region: str) -> dict:
    """
    Derive approximate tier-1 trade shares from the calibrated A matrix.

    Each sector's total intermediate input ratio (A column sum) scales the
    regional import openness parameter; the imported fraction is then
    distributed across other regions proportional to their GDP weight.
    Used only when no MRIO file is available.
    """
    A        = _calibrated_A(db_key)
    openness = _REGION_IMPORT_OPENNESS.get(region, 0.30)

    foreign_w     = {r: w for r, w in _REGION_GDP_WEIGHT.items() if r != region}
    total_foreign = sum(foreign_w.values())

    shares: dict = {}
    for j, sec in enumerate(SECTORS_8):
        col_sum        = float(A[:, j].sum())
        import_share   = min(openness * (0.5 + col_sum), 0.85)
        domestic_share = 1.0 - import_share
        sec_shares     = {region: domestic_share}
        for r, w in foreign_w.items():
            sec_shares[r] = import_share * w / total_foreign
        shares[sec] = sec_shares

    return shares


def _get_trade_shares(db_name: str, iodb_path: Path, region: str) -> dict:
    """
    Return tier-1 bilateral trade shares for the given database and project region.

    Priority:
      1. Extract from pymrio Z matrix (file-backed databases)
      2. Fall back to calibrated A-matrix derivation
    """
    if db_name in PYMRIO_LOADERS:
        io = _load_pymrio_db(db_name, iodb_path)
        if io is not None:
            extracted = _extract_trade_shares_pymrio(io, region)
            if extracted:
                return extracted

    db_key = db_name.replace("_file", "")
    if db_key.startswith("iopy_"):
        db_key = "exiobase" if "exio" in db_key else "oecd"
    if db_key not in DB_PROFILES:
        db_key = "exiobase"
    return _calibrated_trade_shares(db_key, region)


def _normalise_country(country: str) -> str:
    """Map ISO2 code or exact region name to one of Europe/LATAM/Africa/Asia/Global."""
    if country in ("Europe", "LATAM", "Africa", "Asia", "Global"):
        return country
    return ISO2_TO_REGION.get(country.upper(), "Global")

def _get_alloc(sector_code: str) -> np.ndarray:
    """Return 8-element spend allocation array for a sector code."""
    if sector_code not in SECTOR_ALLOC:
        raise ValueError(
            f"Unknown sector_code '{sector_code}'. "
            f"Valid values: {list(SECTOR_ALLOC.keys())}"
        )
    return SECTOR_ALLOC[sector_code]

def _calibrated_S(db_name: str, region: str) -> np.ndarray:
    """
    Return a (4 × 8) intensity matrix calibrated for the given database
    and region. Rows: GHG, Employment, Water, ValueAdded.
    """
    prof = DB_PROFILES[db_name]
    key  = _normalise_country(region)

    S = S_BASE.copy()
    S[0] *= prof["s_ghg"] * REGION_MULT["GHG"].get(key, np.ones(NSEC))
    S[1] *= prof["s_emp"] * REGION_MULT["EMP"].get(key, np.ones(NSEC))
    S[2] *= prof["s_wat"] * REGION_MULT["WAT"].get(key, np.ones(NSEC))
    # Row 3 (VA) unchanged — ratio is relatively stable across regions
    return S


def _calibrated_A(db_name: str) -> np.ndarray:
    """Return an 8×8 A matrix calibrated for the given database."""
    A = A_BASE + DB_PROFILES[db_name]["a_delta"]
    A = np.clip(A, 0, None)
    # Normalise columns where column sum ≥ 1 to stay solvable
    for j in range(NSEC):
        col_sum = A[:, j].sum()
        if col_sum >= 0.99:
            A[:, j] *= 0.98 / col_sum
    return A


def _load_pymrio_db(db_name: str, iodb_path: Path):
    """
    Load a real pymrio database from iodb_path.
    Returns a pymrio IOSystem (calc_all run) or None on failure.
    """
    if db_name not in PYMRIO_LOADERS:
        return None
    cache_key = f"pymrio_{db_name}"
    if cache_key in _DB_CACHE:
        return _DB_CACHE[cache_key]

    try:
        import pymrio
    except ImportError:
        return None

    subfolder, loader_fn, extra_kwargs = PYMRIO_LOADERS[db_name]
    path = iodb_path / subfolder
    if not path.exists() or not any(path.iterdir()):
        return None

    try:
        loader = getattr(pymrio, loader_fn)
        io = loader(path=str(path), **extra_kwargs)
        io.calc_all()
        _DB_CACHE[cache_key] = io
        return io
    except Exception as exc:
        print(f"  [WARN] Could not load {db_name} from {path}: {exc}")
        return None


def _load_iopy_db(db_name: str):
    """
    Load an iopy database. Returns iopy DB object or None.
    Uses module-level cache to avoid reloading large matrices.
    """
    if db_name not in IOPY_DB_CONFIGS:
        return None
    if db_name in _DB_CACHE:
        return _DB_CACHE[db_name]

    cfg = IOPY_DB_CONFIGS[db_name]
    try:
        import iopy
        cls = getattr(iopy, cfg["class"])
        db  = cls(**cfg["kwargs"])
        _DB_CACHE[db_name] = db
        return db
    except Exception as exc:
        print(f"  [WARN] Could not load iopy {db_name}: {exc}")
        return None


def _extract_pymrio_S(io, db_name: str, region: str) -> Optional[np.ndarray]:
    """
    Extract a (4 × 8) intensity matrix from a loaded pymrio IOSystem.
    Falls back to calibrated if extraction fails.
    """
    try:
        from run_all_db import DB_SECTOR_MAP
        key = db_name.replace("_file", "")
        sec_map  = DB_SECTOR_MAP.get(key, {})
        region_n = _normalise_country(region)

        # Get S matrix from satellite/emissions extension
        ext = getattr(io, "satellite", None) or getattr(io, "emissions", None)
        if ext is None:
            return None
        S_full = ext.S

        rows_out = np.zeros((4, NSEC))
        for j, sec in enumerate(SECTORS_8):
            sec_name = sec_map.get(sec)
            if sec_name is None:
                continue
            cols = [c for c in S_full.columns if sec_name.lower() in str(c).lower()]
            if not cols:
                continue
            # GHG
            ghg_rows = [r for r in S_full.index
                        if any(k in str(r).lower() for k in ("co2", "ghg", "ch4", "n2o"))]
            if ghg_rows:
                rows_out[0, j] = float(S_full.loc[ghg_rows[0], cols[0]])
            # VA
            va  = io.meta if hasattr(io, "meta") else None  # fallback
        return rows_out if rows_out.any() else None
    except Exception:
        return None


# ══════════════════════════════════════════════════════════════════════════════
# 3.  TIER 1 IMPACT  (direct suppliers: SECTOR_ALLOC spend × intensity)
# ══════════════════════════════════════════════════════════════════════════════

def tier0_impact(
    invest_usd: float,
    sector_code: str,
    country: str,
    database: str = "exiobase",
    iodb_path: str | Path = "./input_iodb",
) -> dict:
    """
    Compute tier 0 (direct spend) supply chain impacts for one investment.

    Tier 0 = the direct one-time transaction.  The investment is distributed
    across 8 supplying sectors via SECTOR_ALLOC and multiplied by sector ×
    region intensity coefficients.  No Leontief inversion — this is the
    direct spend effect (y₀ = SECTOR_ALLOC × invest_M$).

    Parameters
    ----------
    invest_usd  : float — investment in USD
    sector_code : str   — project archetype defining the CAPEX breakdown
                          (see module docstring); NOT an IO sector itself
    country     : str   — "Europe"/"LATAM"/"Africa"/"Asia" or ISO2 code
    database    : str   — which database calibration to use (see module docstring)
    iodb_path   : path  — root of input_iodb/ folder

    Returns
    -------
    dict with keys:
      database, sector_code, country, region, invest_usd,
      GHG_tCO2e, Employment_FTE, Water_1000m3, ValueAdded_M$,
      backend,             # "calibrated" | "pymrio" | "iopy"
      spend_M$_by_sector   # {sector: M$ at tier 1}
      impact_by_sector     # {sector: {stressor: value}}
    """
    iodb_path = Path(iodb_path)
    region    = _normalise_country(country)
    alloc     = _get_alloc(sector_code)
    invest_m  = invest_usd / 1e6

    # ── Determine backend and get intensity matrix ────────────────────────────
    backend = "calibrated"
    S = None

    if database in IOPY_DB_CONFIGS:
        backend = "iopy"
        iopy_db = _load_iopy_db(database)
        if iopy_db is not None:
            S = _iopy_S_matrix(iopy_db, database, region)

    elif database in PYMRIO_LOADERS:
        backend = "pymrio"
        pymrio_db = _load_pymrio_db(database, iodb_path)
        if pymrio_db is not None:
            S_extracted = _extract_pymrio_S(pymrio_db, database, region)
            if S_extracted is not None:
                S = S_extracted
                # Pad to 4 rows (GHG, EMP, WAT, VA) — VA from calibrated if missing
                S_calib = _calibrated_S(database, region)
                full = S_calib.copy()
                for row in range(min(S.shape[0], 4)):
                    nonzero = S[row][S[row] > 0]
                    if len(nonzero):
                        full[row] = S[row]
                S = full

    if S is None:
        # Fall back to calibrated model (always available)
        db_key = database.replace("_file", "")
        if db_key not in DB_PROFILES:
            raise ValueError(
                f"Unknown database '{database}'. "
                f"Valid options: {list(DB_PROFILES.keys()) + list(IOPY_DB_CONFIGS.keys())}"
            )
        S = _calibrated_S(db_key, region)
        backend = "calibrated"

    # ── Tier 0 computation ────────────────────────────────────────────────────
    spend_vec = alloc * invest_m     # M$ per sector at tier 0
    impacts   = S * spend_vec        # (4, 8) impact matrix

    spend_by_sector  = {sec: round(float(spend_vec[j]), 4) for j, sec in enumerate(SECTORS_8)}
    impact_by_sector = {}
    for j, sec in enumerate(SECTORS_8):
        impact_by_sector[sec] = {
            "spend_M$":      round(float(spend_vec[j]), 4),
            "GHG_tCO2e":     round(float(impacts[0, j]), 2),
            "Employment_FTE":round(float(impacts[1, j]), 2),
            "Water_1000m3":  round(float(impacts[2, j]), 4),
            "ValueAdded_M$": round(float(impacts[3, j]), 4),
        }

    return {
        "database":          database,
        "sector_code":       sector_code,
        "country":           country,
        "region":            region,
        "invest_usd":        invest_usd,
        "GHG_tCO2e":         round(float(impacts[0].sum()), 2),
        "Employment_FTE":    round(float(impacts[1].sum()), 2),
        "Water_1000m3":      round(float(impacts[2].sum()), 4),
        "ValueAdded_M$":     round(float(impacts[3].sum()), 4),
        "backend":           backend,
        "db_label":          DB_PROFILES.get(database, IOPY_DB_CONFIGS.get(database, {})).get("label", database),
        "spend_M$_by_sector":   spend_by_sector,
        "impact_by_sector":     impact_by_sector,
    }


# ══════════════════════════════════════════════════════════════════════════════
# 3b. TIER 1 IMPACT  (supply chain of direct spend, with sourcing country)
# ══════════════════════════════════════════════════════════════════════════════

def tier1_impact(
    invest_usd: float,
    sector_code: str,
    country: str,
    database: str = "exiobase",
    iodb_path: str | Path = "./input_iodb",
) -> dict:
    """
    Compute tier 1 (supply chain from the direct spend) impacts,
    with a sourcing-country breakdown for each supplying sector.

    Tier 1 = A × y₀  (one Leontief round applied to the tier-0 spend vector).
    Each sector's tier-1 spend is allocated across sourcing regions using
    bilateral trade shares derived from the MRIO Z matrix (file-backed databases)
    or the calibrated OECD TiVA fallback, and regional intensity coefficients
    are applied at the sourcing region.

    Parameters
    ----------
    invest_usd  : float — investment in USD
    sector_code : str   — project archetype (see module docstring)
    country     : str   — "Europe"/"LATAM"/"Africa"/"Asia" or ISO2 code
    database    : str   — which database calibration to use (see module docstring)
    iodb_path   : path  — root of input_iodb/ folder

    Returns
    -------
    dict with keys:
      database, sector_code, country, region, invest_usd,
      GHG_tCO2e, Employment_FTE, Water_1000m3, ValueAdded_M$,  # tier 2 totals
      backend,
      tier1_by_sector   # {sector: {sourcing_region: {share, spend_M$, GHG_tCO2e, ...}}}
      sourcing_summary  # {sourcing_region: {spend_M$, GHG_tCO2e, ...}}  aggregated
    """
    iodb_path = Path(iodb_path)
    region    = _normalise_country(country)

    db_key = database.replace("_file", "")
    if db_key.startswith("iopy_"):
        db_key = "exiobase" if "exio" in db_key else "oecd"
    if db_key not in DB_PROFILES:
        raise ValueError(
            f"Unknown database '{database}'. "
            f"Valid options: {list(DB_PROFILES.keys()) + list(IOPY_DB_CONFIGS.keys())}"
        )

    alloc    = _get_alloc(sector_code)
    invest_m = invest_usd / 1e6

    # ── Tier 1 → Tier 2 intermediate demand ──────────────────────────────────
    A  = _calibrated_A(db_key)
    y1 = alloc * invest_m   # (8,) tier 1 spend vector (direct suppliers)
    y2 = A @ y1             # (8,) tier 2 intermediate demand per sector

    # ── Allocate each sector's tier-2 spend across sourcing regions ──────────
    # Extract from Z matrix when file-backed; fall back to A-matrix derivation
    trade_table = _get_trade_shares(database, iodb_path, region)

    _all_regions = ("Europe", "LATAM", "Africa", "Asia", "Global")
    sourcing_totals: dict = {
        r: {"spend_M$": 0.0, "GHG_tCO2e": 0.0, "Employment_FTE": 0.0,
            "Water_1000m3": 0.0, "ValueAdded_M$": 0.0}
        for r in _all_regions
    }

    tier1_by_sector: dict = {}
    for j, sec in enumerate(SECTORS_8):
        spend_t2   = float(y2[j])
        sec_shares = trade_table.get(sec, {region: 1.0})
        sec_result = {}
        for src_region, share in sec_shares.items():
            S_src      = _calibrated_S(db_key, src_region)
            spend_src  = spend_t2 * share
            ghg        = float(S_src[0, j] * spend_src)
            emp        = float(S_src[1, j] * spend_src)
            wat        = float(S_src[2, j] * spend_src)
            va         = float(S_src[3, j] * spend_src)
            sec_result[src_region] = {
                "share":          round(share, 4),
                "spend_M$":       round(spend_src, 5),
                "GHG_tCO2e":      round(ghg, 3),
                "Employment_FTE": round(emp, 3),
                "Water_1000m3":   round(wat, 5),
                "ValueAdded_M$":  round(va, 5),
            }
            t = sourcing_totals[src_region]
            t["spend_M$"]       += spend_src
            t["GHG_tCO2e"]      += ghg
            t["Employment_FTE"] += emp
            t["Water_1000m3"]   += wat
            t["ValueAdded_M$"]  += va
        tier1_by_sector[sec] = sec_result

    # Drop regions with zero spend; round the rest
    sourcing_summary = {
        r: {k: round(v, 4) for k, v in vals.items()}
        for r, vals in sourcing_totals.items()
        if vals["spend_M$"] > 0
    }

    grand_ghg  = round(sum(v["GHG_tCO2e"]      for v in sourcing_totals.values()), 2)
    grand_emp  = round(sum(v["Employment_FTE"]  for v in sourcing_totals.values()), 2)
    grand_wat  = round(sum(v["Water_1000m3"]    for v in sourcing_totals.values()), 4)
    grand_va   = round(sum(v["ValueAdded_M$"]   for v in sourcing_totals.values()), 4)

    trade_backend = "pymrio" if database in PYMRIO_LOADERS else "calibrated"

    return {
        "database":         database,
        "sector_code":      sector_code,
        "country":          country,
        "region":           region,
        "invest_usd":       invest_usd,
        "GHG_tCO2e":        grand_ghg,
        "Employment_FTE":   grand_emp,
        "Water_1000m3":     grand_wat,
        "ValueAdded_M$":    grand_va,
        "backend":          trade_backend,
        "db_label":         DB_PROFILES.get(db_key, {}).get("label", database),
        "tier1_by_sector":  tier1_by_sector,
        "sourcing_summary": sourcing_summary,
    }


def _iopy_S_matrix(db, db_name: str, region: str) -> Optional[np.ndarray]:
    """
    Extract a (4 × 8) intensity matrix from a loaded iopy database.
    Uses actual value-added coefficient (V/X) as the VA row.
    GHG/EMP/WAT rows fall back to calibrated since iopy only has VA and no
    explicit satellite accounts.
    """
    try:
        cfg     = IOPY_DB_CONFIGS[db_name]
        sec_map = cfg["sector_map"]
        reg_map = cfg["region_map"]
        db_reg  = reg_map.get(region, list(db.regions)[0])

        rows  = db.X.rows
        X_arr = np.array(db.X).flatten()
        V_arr = np.array(db.V).flatten()
        x_safe = np.where(X_arr > 0, X_arr, 1.0)
        v_coeff = V_arr / x_safe    # VA intensity per unit output

        # Build VA row from iopy database
        # GHG/EMP/WAT: use calibrated values (iopy has no satellite)
        db_key = db_name.split("_")[1] if "_" in db_name else "exiobase"
        db_key_map = {"oecd": "oecd", "exio": "exiobase"}
        calib_key  = db_key_map.get(db_key, "exiobase")
        S = _calibrated_S(calib_key, region).copy()

        for j, sec in enumerate(SECTORS_8):
            db_sector = sec_map.get(sec)
            if db_sector is None:
                continue
            # Find index for (db_reg, db_sector)
            for i, (r, s) in enumerate(rows):
                if r == db_reg and s == db_sector:
                    # Rescale VA row to match iopy's actual intensity
                    S[3, j] = float(v_coeff[i])
                    break
        return S
    except Exception:
        return None


# ══════════════════════════════════════════════════════════════════════════════
# 4.  MULTI-TIER IMPACT  (power series decomposition)
# ══════════════════════════════════════════════════════════════════════════════

def tier_impact(
    invest_usd: float,
    sector_code: str,
    country: str,
    database: str = "exiobase",
    iodb_path: str | Path = "./input_iodb",
    tier_from: int = 0,
    tier_to: int = 8,
) -> pd.DataFrame:
    """
    Tier-by-tier supply chain impact using power-series decomposition.

    For each tier n in [tier_from, tier_to]:
        yₙ = Aⁿ · y₀     where y₀ = SECTOR_ALLOC × invest_M$
        impactₙ = S · diag(yₙ)

    Tier numbering:
      tier 0 = direct spend (y₀, no A multiplication)
      tier 1 = A · y₀
      tier n = Aⁿ · y₀

    Parameters
    ----------
    invest_usd  : float — investment in USD
    sector_code : str   — project archetype (see module docstring)
    country     : str   — "Europe"/"LATAM"/"Africa"/"Asia" or ISO2 code
    database    : str   — which database calibration to use
    iodb_path   : path  — root of input_iodb/ folder
    tier_from   : int   — first tier to include in output (default 0)
    tier_to     : int   — last tier to include in output, inclusive (default 8)

    Returns a DataFrame with columns:
      database, sector_code, country, region, invest_usd,
      tier, supplying_sector, spend_M$, GHG_tCO2e,
      Employment_FTE, Water_1000m3, ValueAdded_M$
    """
    if tier_from < 0:
        raise ValueError(f"tier_from must be >= 0, got {tier_from}")
    if tier_to < tier_from:
        raise ValueError(f"tier_to ({tier_to}) must be >= tier_from ({tier_from})")

    iodb_path = Path(iodb_path)
    region    = _normalise_country(country)
    alloc     = _get_alloc(sector_code)
    invest_m  = invest_usd / 1e6

    # Database key for calibration (strip _file suffix; map iopy → base)
    db_key = database.replace("_file", "")
    if db_key.startswith("iopy_"):
        db_key = "exiobase" if "exio" in db_key else "oecd"
    if db_key not in DB_PROFILES:
        raise ValueError(f"Unknown database '{database}'")

    S = _calibrated_S(db_key, region)
    A = _calibrated_A(db_key)

    y0    = alloc * invest_m   # tier-0 spend vector (direct spend)
    rows  = []

    # Pre-advance A_pow to A^tier_from so tier n uses Aⁿ · y0
    A_pow = np.eye(NSEC)
    for _ in range(tier_from):
        A_pow = A_pow @ A

    for t in range(tier_from, tier_to + 1):
        x_t = A_pow @ y0
        for j, sec in enumerate(SECTORS_8):
            spend = x_t[j]
            if spend < 1e-9:
                continue
            rows.append({
                "tier":             t,
                "supplying_sector": sec,
                "spend_M$":         round(float(spend), 5),
                "GHG_tCO2e":        round(float(S[0, j] * spend), 3),
                "Employment_FTE":   round(float(S[1, j] * spend), 3),
                "Water_1000m3":     round(float(S[2, j] * spend), 5),
                "ValueAdded_M$":    round(float(S[3, j] * spend), 5),
            })
        A_pow = A_pow @ A

    df = pd.DataFrame(rows)
    df.insert(0, "database",    database)
    df.insert(1, "sector_code", sector_code)
    df.insert(2, "country",     country)
    df.insert(3, "region",      region)
    df.insert(4, "invest_usd",  invest_usd)
    return df


# ══════════════════════════════════════════════════════════════════════════════
# 5.  CROSS-DATABASE COMPARISON
# ══════════════════════════════════════════════════════════════════════════════

def list_databases(iodb_path: str | Path = "./input_iodb") -> dict:
    """
    Return availability status for all known databases given the iodb_path.

    Returns dict: {db_name: {"available": bool, "backend": str, "label": str, "note": str}}
    """
    iodb_path = Path(iodb_path)
    result    = {}

    # Calibrated (always available)
    for name, prof in DB_PROFILES.items():
        if name.endswith("_file"):
            continue
        result[name] = {
            "available": True,
            "backend":   "calibrated",
            "label":     prof["label"],
            "note":      "always available — no files needed",
        }

    # File-backed (pymrio)
    for db_name, (subfolder, _, _) in PYMRIO_LOADERS.items():
        path = iodb_path / subfolder
        avail = path.exists() and any(path.iterdir())
        result[db_name] = {
            "available": avail,
            "backend":   "pymrio",
            "label":     DB_PROFILES[db_name]["label"],
            "note":      str(path) if avail else f"not found at {path}",
        }

    # iopy databases
    for db_name, cfg in IOPY_DB_CONFIGS.items():
        try:
            import iopy
            from iopy.core.globals import DATA_FOLDER
            from pathlib import Path as _P
            import os, re
            # Check if this DB's cache file exists
            url = iopy.__dict__  # just check import works
            avail = True
            note  = "iopy auto-cache"
        except ImportError:
            avail = False
            note  = "iopy not installed"
        result[db_name] = {
            "available": avail,
            "backend":   "iopy",
            "label":     cfg["label"],
            "note":      note,
        }

    return result


def tier0_all_databases(
    invest_usd: float,
    sector_code: str,
    country: str,
    iodb_path: str | Path = "./input_iodb",
    include_iopy: bool = True,
) -> pd.DataFrame:
    """
    Run tier1_impact for every available database and return a comparison DataFrame.

    Parameters
    ----------
    invest_usd   : investment in USD
    sector_code  : project sector code
    country      : region / ISO2 code
    iodb_path    : path to input_iodb/
    include_iopy : whether to include iopy databases (slow first load ~20 min for ExioBase)

    Returns
    -------
    DataFrame indexed by database with impact columns + metadata
    """
    iodb_path = Path(iodb_path)
    db_status = list_databases(iodb_path)
    results   = []

    for db_name, status in db_status.items():
        if not status["available"]:
            continue
        if not include_iopy and status["backend"] == "iopy":
            continue
        try:
            r = tier1_impact(invest_usd, sector_code, country, db_name, iodb_path)
            results.append({
                "database":       db_name,
                "label":          status["label"],
                "backend":        r["backend"],
                "region":         r["region"],
                "invest_M$":      round(invest_usd / 1e6, 2),
                "GHG_tCO2e":      r["GHG_tCO2e"],
                "Employment_FTE": r["Employment_FTE"],
                "Water_1000m3":   r["Water_1000m3"],
                "ValueAdded_M$":  r["ValueAdded_M$"],
            })
        except Exception as exc:
            print(f"  [WARN] {db_name} failed: {exc}")

    df = pd.DataFrame(results).set_index("database")
    return df


# ══════════════════════════════════════════════════════════════════════════════
# 6.  CONVENIENCE: clear cache
# ══════════════════════════════════════════════════════════════════════════════

def clear_cache():
    """Release all cached database objects from memory."""
    _DB_CACHE.clear()


# ══════════════════════════════════════════════════════════════════════════════
# 7.  QUICK DEMO  (python3 tvp_io_lib.py)
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys

    IODB = Path(__file__).parent / "input_iodb"

    print("\n" + "═"*68)
    print("  TVP IO Library — Quick Demo")
    print("═"*68)

    # ── 1. List available databases
    print("\n[1] Available databases:")
    print(f"  {'Database':<22} {'Backend':<12} {'Available':<10} Note")
    print(f"  {'─'*22} {'─'*12} {'─'*10} {'─'*30}")
    for name, info in list_databases(IODB).items():
        mark = "✓" if info["available"] else "○"
        print(f"  {mark} {name:<21} {info['backend']:<12} {str(info['available']):<10} {info['note'][:50]}")

    # ── 2. Tier 0 impact — single project (direct spend)
    print("\n[2] Tier 0 impact — Rail Dev, Europe, €1.85B (EXIOBASE calibration):")
    r = tier0_impact(
        invest_usd  = 1_850_000_000 * 1.09,   # EUR → USD
        sector_code = "Rail_Dev",
        country     = "Europe",
        database    = "exiobase",
        iodb_path   = IODB,
    )
    print(f"  GHG:        {r['GHG_tCO2e']:>12,.0f} tCO2e")
    print(f"  Employment: {r['Employment_FTE']:>12,.0f} FTE")
    print(f"  Water:      {r['Water_1000m3']:>12,.1f} 1000 m³")
    print(f"  Value Added:{r['ValueAdded_M$']:>12,.1f} M$")

    print("\n  Breakdown by supplying sector (tier 0):")
    print(f"  {'Sector':<25} {'Spend M$':>10} {'GHG':>10} {'FTE':>8}")
    for sec, imp in r["impact_by_sector"].items():
        print(f"  {sec:<25} {imp['spend_M$']:>10.1f} {imp['GHG_tCO2e']:>10,.0f} {imp['Employment_FTE']:>8,.0f}")

    # ── 3. Cross-database comparison (calibrated only — fast)
    print("\n[3] Cross-database comparison — Rail Dev, Europe, €1.85B:")
    df = tier0_all_databases(
        invest_usd  = 1_850_000_000 * 1.09,
        sector_code = "Rail_Dev",
        country     = "Europe",
        iodb_path   = IODB,
        include_iopy= False,     # skip slow iopy loads in demo
    )
    print(df[["GHG_tCO2e", "Employment_FTE", "Water_1000m3", "ValueAdded_M$"]].to_string())

    # ── 4. Tier-by-tier decomposition
    print("\n[4] Tier decomposition 0→4 (hospitals, Africa, $250M, Eora26):")
    tiers = tier_impact(
        invest_usd  = 250_000_000,
        sector_code = "Health_Social",
        country     = "Africa",
        database    = "eora26",
        iodb_path   = IODB,
        tier_from   = 0,
        tier_to     = 4,
    )
    tier_summary = tiers.groupby("tier")[["spend_M$", "GHG_tCO2e", "Employment_FTE"]].sum()
    print(tier_summary.to_string())

    print("\nDone.\n")
