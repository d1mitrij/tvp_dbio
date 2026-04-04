# tvp_dbio — Indicators and IO Databases: Reference Guide

*Last updated: 2026-04-04*

---

## Contents

1. [Indicator Framework](#1-indicator-framework)
2. [Core Indicators (all databases)](#2-core-indicators-all-databases)
   - 2.1 GHG Emissions
   - 2.2 Employment
   - 2.3 Water Use
   - 2.4 Value Added
3. [Optional Indicators (database-specific)](#3-optional-indicators-database-specific)
   - 3.1 Energy Use (EXIOBASE)
   - 3.2 NOx Emissions (EXIOBASE)
   - 3.3 Skill-Level Employment (WIOD)
   - 3.4 Labour Income (Eora26 / OECD)
4. [IO Database Overview](#4-io-database-overview)
5. [Database Detail: EXIOBASE 3](#5-database-detail-exiobase-3)
6. [Database Detail: Eora26](#6-database-detail-eora26)
7. [Database Detail: WIOD 2016](#7-database-detail-wiod-2016)
8. [Database Detail: OECD ICIO v2021](#8-database-detail-oecd-icio-v2021)
9. [Database Detail: iopy backends](#9-database-detail-iopy-backends)
10. [Database Comparison Matrix](#10-database-comparison-matrix)
11. [Choosing a Database](#11-choosing-a-database)

---

## 1. Indicator Framework

### Tier convention

| Tier | Who | What |
|------|-----|------|
| **Tier 0** | Asset owner / investor | One-time CAPEX transaction to acquire or commission the asset (railroad, hospital, power plant). The investment flows to the investor's **direct procurement categories** — construction contractors, equipment manufacturers (rolling stock, medical devices, turbines) contracted directly, energy utilities. These are the entities that directly invoice the project owner. |
| **Tier 1** | Tier 0 contractors | The supply chain triggered by the Tier 0 payment. Tier 0 contractors procure inputs from their own upstream suppliers — steel mills supply the civil works contractor, semiconductor fabs supply the signalling manufacturer, copper mines supply the turbine winder. |
| **Tier n** | Suppliers n steps upstream | Each successive Leontief round: the suppliers of the suppliers of … the investor's direct contractors. |

The `SECTOR_ALLOC` vector distributes the investor's CAPEX across the direct procurement
categories at Tier 0. For Rail_Dev this is: civil works 35%, directly contracted equipment
manufacturers 28%, energy utilities 10%, logistics 10%, quarry/ballast 8%, and minor shares
for safety, waste, and vegetation management.

### How indicators are computed

The library uses a **Leontief power-series decomposition** over a calibrated 8-sector global
model. For a given tier `n`, the supply-chain output vector is:

```
yₙ = Aⁿ · y₀          (M$ of gross output per sector at tier n)
y₀ = SECTOR_ALLOC × invest_M$     ← Tier 0: investor's direct CAPEX
```

Stressor impacts at tier `n` are then:

```
impact_n = S · diag(yₙ)        (stressor × sector impact matrix)
```

where `S` is a **(rows: stressors) × (cols: 8 sectors)** intensity matrix giving the
stressor per M$ of gross output in each sector. For each indicator, a row of `S` is
multiplied element-wise by `yₙ` and summed across sectors.

### Two layers of intensity calibration

Every intensity value passes through two multiplicative calibration layers before use:

1. **Database calibration** (`DB_PROFILES[db]["s_ghg"]` etc.) — aligns the global average
   intensity with the methodological conventions of the specific database.
2. **Regional multiplier** (`REGION_MULT[region]`) — scales to the regional technology and
   economic structure (e.g., grid carbon intensity, labour productivity).

Value Added is not regionally multiplied; its ratio to gross output is relatively stable
across regions in the 8-sector model.

### Eight supplying sectors

| Code | Sector | Typical infrastructure role |
|------|--------|----------------------------|
| `Construction` | Building and civil works | Dominant for any CAPEX-heavy project |
| `Energy_Utilities` | Electricity, gas, heat, refining | Dominates Rail_Op; significant for hydro |
| `Manufacturing` | Metals, chemicals, electronics, machinery | Turbines, rolling stock, medical equipment |
| `Transport_Logistics` | Freight, shipping, warehousing | Materials delivery, logistics |
| `Health_Social` | Hospitals, social care, medical services | Dominant for health projects |
| `Agriculture` | Crops, livestock, fisheries, forestry | Minor; food supply chains |
| `Mining_Extraction` | Metals, coal, oil, quarrying | Iron ore, copper, coking coal for manufacturing |
| `Water_Waste` | Water supply, sewerage, waste treatment | Minor for most projects |

---

## 2. Core Indicators (all databases)

These four indicators are computed for every database, calibrated or file-backed.

---

### 2.1  GHG Emissions — `GHG_tCO2e`

**Unit:** tonnes CO₂-equivalent per M$ gross output (at tier 0); cumulative over tiers.

**Definition:**
Total greenhouse gas emissions associated with a unit of gross output in the supplying
sector, expressed in CO₂-equivalent using **IPCC AR6 GWP100** factors:

| Gas | GWP100 (AR6) |
|-----|-------------|
| CO₂ | 1 |
| CH₄ | 27.9 |
| N₂O | 273 |
| HFCs / PFCs | 100–12,400 (sector-average) |

**Scope coverage by tier:**

| Tier | GHG scope | Description |
|------|-----------|-------------|
| Tier 0 | Scope 3 (Category 1) | Investor's CAPEX to direct procurement categories (contractors, equipment manufacturers, utilities) |
| Tier 1 | Scope 3 (Category 1) | Upstream suppliers to Tier 0 contractors (steel mills, cement plants, raw materials) |
| Tiers 2+ | Scope 3 deep chain | Upstream supply chain convergence |
| Sum T0–T10 | Full upstream embodied | Converges to full Leontief inverse |

**Calibrated base intensities** (`S_BASE` row 0, global average):

| Sector | GHG (tCO2e/M$) | Calibration notes |
|--------|---------------:|---|
| Construction | 220 | Cement, steel, on-site diesel |
| Energy_Utilities | 190 | Mix of generation types; includes fuel refining |
| Manufacturing | 380 | Highest — steel smelting, chemicals, cement inputs |
| Transport_Logistics | 175 | Road freight diesel dominated |
| Health_Social | 140 | Building operations, medical supply chains |
| Agriculture | 150 | Land use change, fertiliser N₂O |
| Mining_Extraction | 320 | Ore processing, explosives, diesel |
| Water_Waste | 110 | Pumping, treatment chemicals |

Primary source: EXIOBASE 3.8.1 emission satellite accounts, 2018 data year.
Cross-checked against IPCC AR6 WG3 Annex II sector emission intensities.

**Regional multipliers** (relative to global average):

| Region | Construction | Energy | Manufacturing | Transport | Note |
|--------|:-----------:|:------:|:-------------:|:---------:|------|
| Europe | 0.78 | **0.52** | 0.80 | 0.80 | Low-carbon grid (53% low-C in 2018, IEA) |
| LATAM | 1.20 | 1.28 | 1.18 | 1.18 | Brazil hydro offsets; Mexico/Argentina fossil |
| Africa | 1.40 | **1.90** | 1.38 | 1.32 | High coal/diesel share; sub-Saharan grid |
| Asia | 1.30 | 1.68 | 1.30 | 1.25 | China coal dominance; mixed Southeast Asia |

The Africa Energy multiplier of **1.90×** is the highest in the model and reflects
sub-Saharan Africa's heavy reliance on coal and diesel for generation.

**Limitations:**
- Calibrated model uses 2018 data; does not reflect post-2020 grid decarbonisation.
- 8-sector aggregation masks technology heterogeneity within sectors (e.g., electric vs diesel
  vehicles within Transport_Logistics).
- Does not capture project-specific technology choices (e.g., a low-carbon concrete specification).
- Scope 1 direct on-site emissions are not separately identified; they are embedded in the
  tier-0 sector intensities.
- Land-use-change GHG is included in the Agriculture sector average but not tracked separately.

---

### 2.2  Employment — `Employment_FTE`

**Unit:** full-time equivalent jobs per M$ gross output.

**Definition:**
Total employment in the supplying sector expressed in FTE (full-time equivalent), covering
all persons directly employed in the production of the output — including part-time and
seasonal workers converted to FTE at 2000 hours/year.

**Calibrated base intensities** (`S_BASE` row 1, global average):

| Sector | FTE/M$ | Notes |
|--------|-------:|------|
| Construction | 14 | Labour-intensive; highest among non-agricultural sectors |
| Energy_Utilities | 5 | Capital-intensive; few direct workers per M$ |
| Manufacturing | 8 | Varies widely (auto ~5, electronics ~12) |
| Transport_Logistics | 10 | Mix of road, rail, warehousing |
| Health_Social | 18 | Service sector; very high labour intensity |
| Agriculture | 25 | Highest FTE/M$; subsistence farming drives global average |
| Mining_Extraction | 7 | Capital-intensive extraction |
| Water_Waste | 12 | Operation and maintenance-heavy |

Primary source: EXIOBASE 3.8.1 employment satellite (persons FTE), 2018 data year.
Converted using PPP-adjusted gross output values. Cross-checked: ILO (2022)
World Employment and Social Outlook; OECD STAN employment by industry.

**Regional multipliers** (relative to global average):

| Region | Construction | Agriculture | Health | Note |
|--------|:-----------:|:-----------:|:------:|------|
| Europe | 0.68 | 0.68 | 0.72 | High productivity; capital substitutes labour |
| LATAM | 1.22 | 1.18 | 1.28 | Middle-income productivity; some informality |
| Africa | 1.58 | 1.48 | 1.62 | Labour-intensive; ILO benchmark ±1.4–1.7× global |
| Asia | 1.38 | 1.38 | 1.42 | China/India manufacturing scale |

**Limitations:**
- FTE counts employment *induced* by the investment; does not measure net new jobs (gross
  versus displacement effects are not modelled).
- Does not distinguish between permanent, temporary, and seasonal employment.
- Informal sector employment is **not** separately tracked in the core indicator
  (captured approximately by the Eora26 optional indicator; see §3.4).
- No gender disaggregation.
- The Africa/Asia multipliers reflect average sector intensities; specific project contexts
  (e.g., mechanised mining vs artisanal) can differ substantially.

---

### 2.3  Water Use — `Water_1000m3`

**Unit:** 1000 m³ of water withdrawal per M$ gross output.

**Definition:**
Total water *withdrawal* (blue + grey water; green water excluded) associated with
production in the supplying sector. Includes direct operational water and upstream water
embedded in purchased intermediates at the modelled tier.

**Calibrated base intensities** (`S_BASE` row 2, global average):

| Sector | 1000 m³/M$ | Notes |
|--------|----------:|------|
| Construction | 0.80 | Concrete mixing, dust suppression |
| Energy_Utilities | 1.20 | Cooling water for thermal generation |
| Manufacturing | 1.50 | Industrial processes, cleaning |
| Transport_Logistics | 0.60 | Relatively low direct water use |
| Health_Social | 0.90 | Sanitation, sterilisation |
| Agriculture | **5.00** | Dominant water user globally; irrigation |
| Mining_Extraction | 1.80 | Ore processing, slurry transport |
| Water_Waste | **8.00** | Water treatment throughput (operational) |

Sources: Mekonnen & Hoekstra (2011) doi:10.5194/hess-15-1577-2011; FAO AQUASTAT (2021);
WRI Aqueduct Water Risk Atlas (2019).

**Regional multipliers:**

| Region | Agriculture | Energy | Manufacturing | Note |
|--------|:-----------:|:------:|:-------------:|------|
| Europe | 0.70 | 0.88 | 0.82 | Water-efficient practices; EU regulations |
| LATAM | 1.35 | 1.35 | 1.28 | High irrigation demand; tropical processes |
| Africa | 1.65 | 1.65 | 1.52 | Arid zones; inefficient infrastructure |
| Asia | 1.55 | 1.52 | 1.48 | Rice cultivation; high thermal cooling |

**Important note — WIOD:**
WIOD 2016 does not include a water satellite account. When `database="wiod"` or
`"wiod_file"`, the `s_wat` calibration factor is **1.00** for all sectors — meaning the
water result falls back entirely to the regional-scaled `S_BASE` without any WIOD-specific
adjustment. This is acknowledged in the `DB_PROFILES` comments.

**Limitations:**
- Withdrawal ≠ consumption. The model measures total water withdrawn, not the portion
  permanently removed from the local water cycle (consumption).
- No water stress dimension — 1000 m³ withdrawn in a water-scarce basin is not weighted
  differently from the same volume in a water-rich region.
- No water quality dimension (pollutant load, thermal discharge).
- Agriculture sector water use is dominated by irrigation; supply-chain agriculture content
  for construction/manufacturing is relatively small.
- WIOD water falls back to global calibration (see above).

---

### 2.4  Value Added — `ValueAdded_M$`

**Unit:** M$ value added per M$ gross output (dimensionless ratio; also reported as M$
total for the investment).

**Definition:**
Value added is the GDP contribution of the supplying sector: gross output minus the cost of
intermediate inputs. In the context of a supply-chain analysis, value added at each tier
represents the income (wages + profits + taxes + capital consumption) generated by
producing the required intermediates.

```
VA = gross_output × (1 − column_sum_of_A)    approximately
```

**Calibrated base intensities** (`S_BASE` row 3, global average ratio):

| Sector | VA/GO ratio | Notes |
|--------|:-----------:|------|
| Construction | 0.48 | Moderate; high intermediate input share |
| Energy_Utilities | **0.64** | Capital-intensive; high profit component |
| Manufacturing | 0.42 | Lowest; extensive intermediate inputs |
| Transport_Logistics | 0.60 | Service-labour heavy |
| Health_Social | **0.70** | High; dominated by wages |
| Agriculture | **0.76** | High in developing countries; low intermediate inputs |
| Mining_Extraction | 0.56 | Moderate; high capital consumption |
| Water_Waste | 0.69 | Mainly operational labour |

Sources: OECD STAN value added by industry (2022); World Bank WDI value added by sector.

**No regional multiplier** is applied to value added. The VA/GO ratio is relatively stable
across regions (within ±5%) compared to GHG or employment intensities (±30–90%).
Regional wage differences affect the *distribution* of value added between labour and capital
but not the total.

**Limitations:**
- Reports producer-price value added; does not net out taxes and subsidies at the
  investor's perspective.
- No income-distribution dimension — VA to labour vs capital is not separated in the
  core indicator (see `LaborIncome_M$` optional indicator for Eora26/OECD).
- For file-backed databases (pymrio), value added is extracted from the calibrated model,
  not directly from the database's national accounts, because not all pymrio databases
  include a consistent value-added extension row.

---

## 3. Optional Indicators (database-specific)

Optional indicators extend the standard four when a database provides the relevant
satellite account. They are returned in `tier0_impact()` as `optional_indicators: dict`
and as additional columns in the `tier_impact()` DataFrame.

---

### 3.1  Energy Use — `Energy_TJ`
*(available for: `exiobase`, `exiobase_file`, `iopy_exio_ixi`, `iopy_exio_pxp`)*

**Unit:** terajoules (TJ) of total energy input per M$ gross output.

**Definition:**
Total primary and secondary energy input to the supplying sector's production process,
covering fuel combustion, electricity use, heat purchases, and feedstock use. This is a
*production-system* energy total (not final-demand energy).

**Calibrated base intensities:**

| Sector | TJ/M$ | Key drivers |
|--------|------:|-------------|
| Construction | 1.80 | Diesel equipment, cement kiln |
| Energy_Utilities | **8.50** | Self-consumption in electricity/gas production |
| Manufacturing | 3.20 | Industrial furnaces, process heat |
| Transport_Logistics | 2.10 | Traction fuel, refrigerated warehouses |
| Health_Social | 0.90 | Building HVAC |
| Agriculture | 2.80 | Machinery fuel, irrigation pumps |
| Mining_Extraction | 4.50 | Ore crushing, ventilation, diesel |
| Water_Waste | 1.20 | Pumping stations, treatment |

Source: EXIOBASE 3.8.1 energy extension (F matrix, rows "Energy carrier supply");
output-weighted aggregate to 8 sectors. Cross-checked: IEA (2022) energy intensities
by sector (TJ/M$ value added converted via VA/GO ratios from OECD STAN 2022).

**Regional multipliers:**

| Region | Multiplier | Rationale |
|--------|:---------:|---------|
| Europe | 0.72 | High energy efficiency; EU ETS and regulation |
| LATAM | 1.15 | Older industrial stock; partially inefficient grid |
| Africa | 1.35 | High energy losses; older capital equipment |
| Asia | 1.25 | China heavy-industry energy intensity |

**Interpretation:** The `Energy_TJ` indicator is most useful for identifying supply-chain
segments where energy transition policies will have the greatest procurement-side impact.
High `Energy_TJ` × high fossil-fuel grid intensity → high future GHG exposure as carbon
prices rise.

**Limitations:**
- Total energy, not disaggregated by carrier (electricity, gas, oil, coal). Cannot
  distinguish renewable from fossil without additional data.
- Does not account for energy efficiency improvements within the 2018–present period.
- EXIOBASE energy extension covers direct + indirect energy use at sector level; project-
  specific technology choices (e.g., electric excavators) are not reflected.

---

### 3.2  NOx Emissions — `NOx_t`
*(available for: `exiobase`, `exiobase_file`, `iopy_exio_ixi`, `iopy_exio_pxp`)*

**Unit:** tonnes of nitrogen oxides (NOx = NO + NO₂, expressed as NO₂-equivalent)
per M$ gross output.

**Definition:**
NOx is a primary air pollutant generated by high-temperature combustion (road transport,
industrial boilers, power generation). It contributes to ground-level ozone, particulate
matter (PM₂.₅ via secondary formation), and acid deposition. In the supply-chain context,
`NOx_t` captures combustion-sourced NOx at each upstream tier.

**Calibrated base intensities:**

| Sector | t NOx/M$ | Key drivers |
|--------|--------:|-------------|
| Construction | 0.80 | Non-road diesel machinery (excavators, cranes) |
| Energy_Utilities | 1.50 | Fossil fuel combustion in power generation |
| Manufacturing | 1.20 | Industrial boilers, cement kilns |
| Transport_Logistics | **2.80** | Road freight diesel; highest among sectors |
| Health_Social | 0.40 | Low-intensity building and supply operations |
| Agriculture | 0.90 | Machinery fuel combustion |
| Mining_Extraction | 1.40 | Explosives, haul-truck diesel |
| Water_Waste | 0.50 | Pumping and treatment |

Source: EXIOBASE 3.8.1 air emission extension (F matrix, row "Nitrogen oxides").
Cross-checked: EEA (2022) National Emissions reported to UNFCCC and CLRTAP, sector data.

**Regional multipliers:**

| Region | Multiplier | Rationale |
|--------|:---------:|---------|
| Europe | 0.68 | Euro VI emission standards; catalytic converters; clean grid |
| LATAM | 1.30 | Older fleet; Euro II–IV standards; less-clean power mix |
| Africa | 1.45 | Aged diesel fleet; minimal emission controls |
| Asia | 1.35 | China National VI (recent); South/SE Asia older fleets |

**Interpretation:** `NOx_t` is particularly relevant for projects in urban or near-urban
settings where air quality is a health co-benefit criterion (e.g., FAST-Infra E4 Air Quality
positive contribution). High `NOx_t` in Transport_Logistics tiers signals procurement
measures (e.g., Euro VI freight requirements) that could reduce air quality impacts.

**Limitations:**
- NOx ≠ PM₂.₅; secondary PM formation from NOx depends on atmospheric conditions not
  modelled here.
- Does not capture non-combustion NOx (e.g., fertiliser application) — Agriculture NOx
  in this model is combustion-only; actual agricultural NOx (including N₂O → NOx) is
  substantially higher.
- Unit is a mass metric, not a health impact metric (DALY, YOLL); spatial context is needed
  to translate NOx tonnes into health outcomes.

---

### 3.3  Skill-Level Employment — `Emp_HighSkill_FTE`, `Emp_MedSkill_FTE`, `Emp_LowSkill_FTE`
*(available for: `wiod`, `wiod_file`)*

**Unit:** FTE per M$ gross output, by education level (ISCED 1997).

**Definition:**
The total employment indicator (`Employment_FTE`) is disaggregated into three skill bands
based on educational attainment of the workforce:

| Key | ISCED level | Education |
|-----|------------|-----------|
| `Emp_HighSkill_FTE` | ISCED 5–6 | Tertiary: university, vocational college |
| `Emp_MedSkill_FTE` | ISCED 3–4 | Upper-secondary and post-secondary non-tertiary |
| `Emp_LowSkill_FTE` | ISCED 0–2 | Primary and lower-secondary |

The three bands sum to the total `Employment_FTE` global average
(before regional multipliers are applied).

**Calibrated base intensities:**

| Sector | High FTE/M$ | Med FTE/M$ | Low FTE/M$ | Total |
|--------|:----------:|:---------:|:----------:|------:|
| Construction | 2.80 | 5.60 | 5.60 | 14.00 |
| Energy_Utilities | 2.00 | 2.25 | 0.75 | 5.00 |
| Manufacturing | 2.00 | 3.60 | 2.40 | 8.00 |
| Transport_Logistics | 2.00 | 5.00 | 3.00 | 10.00 |
| Health_Social | **8.10** | 7.20 | 2.70 | 18.00 |
| Agriculture | 2.50 | 5.00 | **17.50** | 25.00 |
| Mining_Extraction | 2.45 | 2.80 | 1.75 | 7.00 |
| Water_Waste | 3.00 | 5.40 | 3.60 | 12.00 |

Skill shares calibrated from WIOD 2016 SEA (Timmer et al. 2015
doi:10.1111/roie.12178) and ILO ILOSTAT employment by education
and economic activity (2020 update).

**Note on regional scaling:** No regional multipliers are applied to the skill-level
indicators. The WIOD SEA absolute values are already representative of the WIOD-covered
economies (43 countries, predominantly OECD). Using a global average is preferable to
regional scaling, which would mix wage-substitution effects with employment-structure effects.

**Interpretation:**
- **Health_Social** projects show the highest high-skill employment intensity (8.10 FTE/M$)
  — driven by medical professionals, therapists, and clinical researchers.
- **Agriculture** supply chains generate the highest low-skill employment (17.50 FTE/M$)
  — relevant for food supply chains embedded in any large infrastructure project.
- High-skill employment intensity can be used as a proxy for the degree to which a project's
  supply chain requires technology transfer and local workforce development.

**Limitations:**
- Based on 2014 data (WIOD 2016 release). Structural change in skill demand (digitalisation,
  automation) is not reflected.
- OECD-country skill definitions; skill categories may not map consistently onto
  developing-country education systems.
- No regional adjustment — the same global-average skill split is applied regardless of
  whether the project is in Europe or Africa (where skill composition differs substantially).
- Ignores skill mismatches (a graduate employed as a labourer would be counted as high-skill).

---

### 3.4  Labour Income — `LaborIncome_M$`
*(available for: `eora26`, `eora26_file`, `oecd`, `oecd_file`)*

**Unit:** M$ wages and salaries per M$ gross output.

**Definition:**
Labour income (compensation of employees) is the share of gross output that accrues to
workers as wages, salaries, and non-wage benefits (social contributions paid by employer).
It is a sub-component of value added:

```
ValueAdded_M$ = LaborIncome_M$ + CapitalIncome_M$ + TaxesLessSubsidies_M$
```

**Calibrated base intensities:**

| Source | Sector | Labor/GO | VA/GO | Labor/VA share |
|--------|--------|:--------:|:-----:|:--------------:|
| Eora26 | Construction | 0.26 | 0.48 | 54% |
| Eora26 | Energy_Util | 0.22 | 0.64 | 34% |
| Eora26 | Manufacturing | 0.18 | 0.42 | 43% |
| Eora26 | Transport | 0.30 | 0.60 | 50% |
| Eora26 | Health_Social | 0.42 | 0.70 | 60% |
| Eora26 | Agriculture | 0.28 | 0.76 | 37% |
| Eora26 | Mining | 0.22 | 0.56 | 39% |
| Eora26 | Water_Waste | 0.32 | 0.69 | 46% |
| OECD | Construction | 0.28 | 0.48 | 58% |
| OECD | Energy_Util | 0.24 | 0.64 | 38% |
| OECD | Manufacturing | 0.20 | 0.42 | 48% |
| OECD | Transport | 0.32 | 0.60 | 53% |
| OECD | Health_Social | 0.44 | 0.70 | 63% |

The Eora26 values are slightly lower across sectors because Eora26 covers more developing
countries where capital's share of VA is higher (World Bank WDI).

Sources: OECD STAN (2022) labour compensation by industry; World Bank (2022) WDI labour
income share (SL.GDP.PCAP.EM.KD); Eora26 value-added satellite.

**Regional multipliers:**

| Region | Multiplier | Rationale |
|--------|:---------:|---------|
| Europe | 1.20 | High minimum wages; strong collective bargaining |
| LATAM | 0.85 | Middle-income wages; informal sector depresses average |
| Africa | 0.65 | Low formal wages; high capital/subsistence share |
| Asia | 0.80 | Wide range (Japan/Korea high; South/SE Asia low) |

**Interpretation:**
`LaborIncome_M$` is the most direct supply-chain indicator of workers' welfare impact.
Combined with `Employment_FTE`, it allows computation of the **average wage per FTE**
generated by the supply chain:

```
average_wage_kUSD = (LaborIncome_M$ / Employment_FTE) × 1000
```

For example, a Health_Social project in Europe generating 0.44 M$ labour income and
18 FTE per M$ output implies an average supply-chain wage of ~$24,000/FTE/year.

**Limitations:**
- Includes employer social contributions, not just take-home wages.
- Does not distinguish between wage levels by occupation or gender.
- Eora26 labour income for developing-country sectors carries higher uncertainty than OECD
  (less consistent national account data; see Lenzen et al. 2013 §5).
- The model does not capture labour income generated outside the 8-sector IO framework
  (e.g., civil society, informal micro-enterprises).

---

## 4. IO Database Overview

| Database | Countries | Sectors | Year | Backend(s) | Best for |
|----------|:---------:|:-------:|:----:|-----------|---------|
| EXIOBASE 3 | 44+5RoW | 163 | 2018 | calibrated, pymrio, iopy | Environmental detail; European projects |
| Eora26 | 190 | 26 | 2015 | calibrated, pymrio | Global South; broad country coverage |
| WIOD 2016 | 43 | 56 | 2014 | calibrated, pymrio | Labour analysis; historical trends |
| OECD ICIO v2021 | 66 | 45 | 2018 | calibrated, pymrio, iopy | Policy; TiVA; most current |

All four databases are available as **calibrated** backends (pure numpy, no files needed).
The calibrated model applies database-specific bias corrections and regional multipliers
to a single 8-sector global A matrix and S matrix.

---

## 5. Database Detail: EXIOBASE 3

**Key identifiers in the library:** `"exiobase"` (calibrated), `"exiobase_file"` (pymrio),
`"iopy_exio_ixi"`, `"iopy_exio_pxp"` (iopy).

### Overview

EXIOBASE 3 is a multi-regional supply-use and input-output database with a particular focus
on environmental extensions. It covers 44 countries (all EU members + 12 major non-EU
economies) plus 5 rest-of-world aggregates, with 163 product categories or 163 industry
categories depending on the variant (pxp = product-by-product; ixi = industry-by-industry).

**Current release:** 3.8.1 (Zenodo doi:10.5281/zenodo.5589597), 2018 data year.
**Earlier versions:** 3.1 (2011), 3.3.17 (2011–2016), 3.8 (2011–2022 time series).

The library uses the **2018 data year** for consistency across all databases.

**Satellite accounts (F matrix):**
EXIOBASE provides one of the richest environmental satellite extensions in any public MRIO:
- GHG: CO₂, CH₄, N₂O, SF₆, HFCs, PFCs (6 gases; AR5 and AR6 GWP available)
- Air emissions: NOx, SOx, NH₃, NMVOC, CO, PM2.5, PM10
- Energy use: by carrier (coal, oil, gas, nuclear, hydro, other renewable, electricity)
- Land use: cropland, grassland, forest, built-up
- Water use: blue, green, grey
- Material extraction: biomass, fossil fuels, metals, non-metallic minerals
- Employment: total persons, FTE

### Advantages

1. **Richest environmental satellite.** 48+ satellite rows enable comprehensive footprint
   analysis beyond GHG — energy, water, land, materials, and air emissions in one table.

2. **Best GHG detail.** Individual gas-level GHG accounting allows applying different GWP
   vintages (AR5 vs AR6) and identifying sector-specific gas composition (e.g., N₂O from
   agriculture, CH₄ from energy).

3. **EU-centric accuracy.** All EU member states are individually represented. Supply chains
   within the EU are modelled with the highest granularity of any public MRIO.

4. **163-sector resolution.** Allows precise mapping from procurement categories (e.g.,
   "hot-rolled steel" or "cement clinker") to IO sectors — minimising aggregation bias.

5. **Long time series (3.8 release).** 2011–2022 annual tables allow trend analysis and
   green-transition tracking.

6. **Freely available.** Licensed under CC BY 4.0; downloadable via Zenodo and pymrio.

### Limitations

1. **EU-centric.** Supply chains in Africa, LATAM, and parts of Asia are represented only at
   the rest-of-world aggregate level (WF = Africa RoW, WL = LATAM RoW, etc.). This means
   project-specific country sourcing in those regions cannot be captured at the transaction level.

2. **Large file size.** The pxp version is ~600 MB zipped (~2 GB loaded). Loading time can
   be several minutes; memory footprint is high (4–8 GB RAM for calc_all).

3. **2018 data year.** Does not reflect post-2020 green transition, COVID supply-chain
   disruptions, or energy price shocks of 2021–2022.

4. **Calibrated-model limitation.** The calibrated `"exiobase"` backend uses a condensed
   8-sector A matrix; the rich 163-sector detail is not available unless `"exiobase_file"`
   or an iopy backend is used.

5. **iopy EXIOBASE limitation.** When accessed via iopy, GHG/Employment/Water rows fall
   back to the calibrated model because iopy does not expose the full satellite matrix via
   its API; only value-added and the A matrix are accessible.

### Calibration factors in the library

```
s_ghg = [0.97, 0.88, 0.96, 0.97, 0.94, 0.98, 0.97, 0.95]  # slightly below global average
s_emp = [0.90, 0.87, 0.88, 0.91, 0.89, 0.88, 0.88, 0.90]  # EU high-productivity bias
s_wat = [0.97, 0.95, 0.98, 0.96, 0.97, 0.97, 0.96, 0.94]  # EU water efficiency
```

The EXIOBASE calibration factors are consistently below 1.0 across all rows, reflecting that
EXIOBASE's EU-weighted global average is more technologically efficient than the true global
average (which includes high-intensity developing-country sectors).

---

## 6. Database Detail: Eora26

**Key identifiers in the library:** `"eora26"` (calibrated), `"eora26_file"` (pymrio).

### Overview

Eora26 is a high-country-resolution MRIO built at the University of Sydney. With **190
individual countries** and 26 sectors, it is the only global MRIO with individual coverage
for small and low-income economies. It is particularly strong for Africa, LATAM, and
South/Southeast Asia.

**Current data year used:** 2015 (most recent stable release in the library).
**Sectors:** 26 (International Standard Industrial Classification Rev. 3 aggregate).

### Advantages

1. **190-country coverage.** No other public MRIO provides individual-country supply-chain
   data for economies such as Mozambique, Bolivia, or Cambodia. This is essential for
   projects in the Global South.

2. **Informal economy representation.** Eora26's supply-use construction methodology
   incorporates satellite data from national statistical offices and international
   organisations, capturing informal sector activity to a greater degree than EXIOBASE
   or OECD.

3. **Compact sector structure.** 26 sectors is computationally fast and requires a smaller
   file than EXIOBASE (Eora26 full is ~230 MB).

4. **Free with registration.** Available at worldmrio.com; academic/non-commercial
   licence is free.

5. **Developing-country calibration in the library.** The `s_emp > 1.0` calibration
   factors (1.10–1.22×) correct for Eora26's known upward bias in employment accounts
   for formal-sector sectors in developing countries (documented in Lenzen et al. 2013 §5).

### Limitations

1. **Coarse 26-sector aggregation.** Manufacturing, for example, is a single sector
   covering everything from food processing to steel smelting. Aggregation bias is
   significant for projects with sector-specific supply chains.

2. **2015 data year.** The oldest data year among the four databases. Does not reflect
   post-2015 structural change (Paris Agreement implementation, Chinese supply chain
   reorientation, COVID-19 effects).

3. **Known employment upward bias.** Eora26's national account reconciliation for
   developing-country formal sectors produces employment figures higher than ILO household
   surveys. The library applies a 10–22% downward correction (`s_emp = 1.10–1.22`) to
   partially offset this.

4. **Eora26 satellite accounts.** GHG and water extensions in Eora26 are less comprehensive
   than EXIOBASE. The calibrated backend uses the Eora26 DB profile adjustments but does not
   extract satellite data from the file (the pymrio extraction uses calibrated fallback).

5. **Registration required.** Unlike EXIOBASE (public) and OECD (government), Eora26 requires
   a user account at worldmrio.com. The download script (`download_iodb.py`) handles this
   via `--eora-email` and `--eora-password`.

### Calibration factors in the library

```
s_ghg = [1.08, 1.15, 1.10, 1.06, 1.09, 1.05, 1.12, 1.10]  # developing-country higher intensity
s_emp = [1.18, 1.10, 1.15, 1.12, 1.22, 1.20, 1.15, 1.18]  # known upward bias correction
s_wat = [1.06, 1.12, 1.08, 1.05, 1.08, 1.12, 1.08, 1.10]  # tropical/arid water intensity
```

All Eora26 calibration factors are **above 1.0**, consistently reflecting that developing-country
supply chains embedded in Eora26's global average are more resource-intensive than the
EXIOBASE-anchored S_BASE global average.

---

## 7. Database Detail: WIOD 2016

**Key identifiers in the library:** `"wiod"` (calibrated), `"wiod_file"` (pymrio).

### Overview

The World Input-Output Database (WIOD) is produced by a consortium of European universities
and research institutes. The 2016 release covers **43 countries** (EU28 + 15 major economies)
and 56 industries, with annual tables from **2000 to 2014**. Its primary strength is the
Socio-Economic Accounts (SEA), which provide the most detailed public labour-market satellite
accounts of any MRIO.

**Data year used in the library:** 2014 (most recent in the 2016 release).
**Sectors:** 56 (ISIC Rev. 4 aggregate).

### Advantages

1. **Most detailed labour accounts.** WIOD's SEA satellite provides employment by industry
   and education level (ISCED 1997 high/medium/low skill), as well as labour compensation,
   capital compensation, and mixed income. This enables the `Emp_HighSkill_FTE`,
   `Emp_MedSkill_FTE`, `Emp_LowSkill_FTE` optional indicators.

2. **Time series 2000–2014.** Allows tracking of structural change and GHG decoupling
   over a 15-year period. Useful for benchmarking historical supply-chain footprints.

3. **56-sector resolution.** More granular than Eora26's 26 sectors; allows distinguishing
   within broad categories (e.g., road transport vs water transport, or basic metals vs
   fabricated metal products).

4. **Consistent national accounts integration.** WIOD is anchored to Eurostat and UN
   national accounts with strong consistency across countries.

5. **Compact file.** ~350 MB; loads faster than EXIOBASE.

### Limitations

1. **No water satellite account.** WIOD 2016 does not include a water extension. The
   library's `s_wat = [1.00, ...]` means the water indicator for WIOD falls back to the
   global-average S_BASE without any WIOD-specific calibration.

2. **Pre-Paris data.** The most recent year (2014) predates the Paris Agreement (2015) and
   does not reflect the accelerated renewable energy deployment of 2016–2024. GHG intensities
   are likely overstated for energy-sector supply chains in current analyses.

3. **43-country coverage.** Only 15 non-EU countries are individually represented. Africa
   and LATAM are entirely in the "RoW" aggregate, making WIOD unsuitable as the primary
   database for developing-country projects.

4. **No official update since 2016.** The WIOD consortium released the 2016 edition but
   has not released a 2020+ update (as of 2026). OECD ICIO is generally preferred for
   current-year analyses.

5. **Skill split regional limitation.** The SEA skill-level employment is provided for
   WIOD-covered economies; applying global-average skill splits to Africa or LATAM projects
   introduces significant uncertainty.

### Calibration factors in the library

```
s_ghg = [1.04, 1.06, 1.03, 1.04, 1.03, 1.02, 1.04, 1.03]  # pre-Paris vintage; slightly above S_BASE
s_emp = [1.22, 1.15, 1.20, 1.18, 1.25, 1.20, 1.18, 1.22]  # SEA broader coverage than EXIOBASE
s_wat = [1.00, 1.00, 1.00, 1.00, 1.00, 1.00, 1.00, 1.00]  # no water satellite; pure S_BASE fallback
```

The WIOD GHG calibration is slightly above 1.0 because the 2014 data year predates grid
decarbonisation; EXIOBASE (also 2018) is the reference and is more carbon-efficient by 2018.

---

## 8. Database Detail: OECD ICIO v2021

**Key identifiers in the library:** `"oecd"` (calibrated), `"oecd_file"` (pymrio),
`"iopy_oecd"` (iopy).

### Overview

The OECD Inter-Country Input-Output (ICIO) database is produced by the OECD Directorate for
Science, Technology and Innovation. The 2021 edition covers **66 countries** (all OECD plus
major emerging economies) and 45 industries, with annual tables from **1995 to 2018**. It is
designed for **trade-in-value-added (TiVA)** analysis and is deeply integrated with OECD's
national accounts.

**Data year used in the library:** 2018 (most recent, consistent with EXIOBASE).
**Sectors:** 45 (ISIC Rev. 4 aggregate).

### Advantages

1. **Most current data.** The 2018 data year is the most recent among the four databases
   and reflects post-2015 supply-chain structures, renewable energy penetration, and digital
   service trade patterns.

2. **66-country coverage.** Adds 22 countries beyond WIOD (e.g., Saudi Arabia, South Africa,
   Vietnam, Argentina), improving coverage for middle-income emerging markets.

3. **TiVA integration.** Directly linked to OECD's Trade in Value Added (TiVA) database.
   Enables decomposition of gross trade into domestic and foreign value-added content —
   relevant for assessing import leakage in infrastructure procurement.

4. **Consistent policy linkages.** Used by OECD, IMF, and WTO for policy analysis; aligned
   with the SNA 2008 national accounts standard. Results are directly comparable to OECD
   published industry statistics.

5. **iopy backend available.** Accessible via iopy as `"iopy_oecd"` (71 regions × 45
   sectors, auto-download ~95 MB).

### Limitations

1. **No environmental satellite in the standard release.** Unlike EXIOBASE, OECD ICIO does
   not include GHG or air emission extensions. GHG, employment, and water indicators fall
   back to the calibrated model; only value added can be extracted from the file.

2. **45-sector resolution.** Less granular than EXIOBASE (163 sectors) and comparable to
   WIOD (56 sectors); some sector-specific supply chains cannot be resolved.

3. **OECD-country bias.** Despite covering 66 countries, the methodology is most reliable
   for OECD members. Non-OECD coverage depends on partner-country data quality.

4. **Proprietary distribution.** Available for non-commercial use, but redistribution is
   restricted. The download script uses the OECD public download API.

### Calibration factors in the library

```
s_ghg = [0.97, 0.92, 0.97, 0.97, 0.95, 0.99, 0.97, 0.96]  # 2018 post-Paris efficiency gains
s_emp = [1.05, 1.02, 1.04, 1.06, 1.05, 1.05, 1.03, 1.06]  # aligned with OECD STAN employment
s_wat = [1.01, 1.03, 1.02, 1.01, 1.02, 1.02, 1.01, 1.02]  # near S_BASE; no OECD water satellite
```

OECD calibration factors are close to 1.0 across all rows, reflecting OECD ICIO's alignment
with the global-average S_BASE which was itself partially anchored to OECD STAN employment data.

---

## 9. Database Detail: iopy backends

**Key identifiers:** `"iopy_oecd"`, `"iopy_exio_ixi"`, `"iopy_exio_pxp"`.

### Overview

The `iopy` library provides Python access to several MRIO databases with an auto-download
mechanism. It loads data from an internal cache folder and exposes the A matrix, L matrix
(Leontief inverse), X (gross output), and V (value added) for each database.

| Key | Database | Regions | Sectors | Download size |
|-----|----------|:-------:|:-------:|:-------------:|
| `iopy_oecd` | OECD 2021 | 71 | 45 | ~95 MB |
| `iopy_exio_ixi` | ExioBase 3.81 ixi | 49 | 163 | ~730 MB |
| `iopy_exio_pxp` | ExioBase 3.81 pxp | 49 | 200 | ~500 MB |

### Advantages

1. **Automatic download and cache.** No manual download required; first use triggers an
   automatic download to a fixed internal cache path.

2. **ExioBase 3.81** (vs 3.8.1 via pymrio). iopy targets the minor revision 3.81, which
   includes small corrections to the 2018 satellite data.

3. **iopy_oecd provides actual VA.** The library extracts the real V/X (value-added per
   gross output) ratio from the iopy OECD database, giving more precise value-added results
   than the calibrated fallback.

4. **No file path management.** Unlike pymrio file-backed databases, iopy uses its own cache
   and does not require `input_iodb/` to be populated.

### Limitations

1. **No satellite extraction via iopy API.** The iopy library exposes A, L, X, V but not the
   F (satellite) matrix. As a result, GHG, employment, and water indicators for all iopy
   backends fall back to the calibrated model. Only the VA row uses real database data.

2. **Slow first load.** ExioBase ixi/pxp downloads are 500–730 MB and can take 5–20 minutes.
   After the first load, the database is cached in memory for the session.

3. **iopy version dependency.** iopy ≥ 0.2.4 is required. Earlier versions had broken
   OECD 2022-small URLs; the library pins to OECD 2021 (`version="2021"`) to avoid this.

4. **Mapping dependency.** Sector codes in iopy OECD are ISIC strings (e.g., `"41T43"`) and
   in iopy ExioBase are short `CodeNr` strings (e.g., `"i45"` for construction ixi,
   `"p45"` for pxp). These mappings are hardcoded in `IOPY_DB_CONFIGS` and must be
   updated if iopy changes its sector encoding.

5. **Optional indicators limited.** For iopy OECD, no optional indicators are currently
   exposed (VA row is extracted but LaborIncome requires the labour compensation row which
   iopy does not provide). For iopy ExioBase, the EXIOBASE optional indicators
   (Energy_TJ, NOx_t) are available through the calibrated model rather than the actual
   satellite.

---

## 10. Database Comparison Matrix

| Criterion | EXIOBASE 3 | Eora26 | WIOD 2016 | OECD ICIO |
|-----------|:----------:|:------:|:---------:|:---------:|
| **Country coverage** | 44+5RoW | **190** | 43 | 66 |
| **Sector resolution** | **163** | 26 | 56 | 45 |
| **Data year** | **2018** | 2015 | 2014 | **2018** |
| **GHG satellite** | ★★★★★ | ★★☆☆☆ | ★★☆☆☆ | ★★☆☆☆ |
| **Employment satellite** | ★★★☆☆ | ★★★★☆ | ★★★★★ | ★★★☆☆ |
| **Water satellite** | ★★★★☆ | ★★★☆☆ | ★☆☆☆☆ | ★★☆☆☆ |
| **Air emissions (NOx/SOx)** | ★★★★★ | ☆☆☆☆☆ | ☆☆☆☆☆ | ☆☆☆☆☆ |
| **Skill-level employment** | ★★★☆☆ | ★★☆☆☆ | ★★★★★ | ★★★☆☆ |
| **Labour income** | ★★★☆☆ | ★★★★☆ | ★★★★☆ | ★★★★★ |
| **Global South accuracy** | ★★☆☆☆ | ★★★★★ | ★☆☆☆☆ | ★★★☆☆ |
| **EU/OECD accuracy** | ★★★★★ | ★★★☆☆ | ★★★★☆ | ★★★★★ |
| **File size** | ~600 MB | ~230 MB | ~350 MB | ~200 MB |
| **Availability** | Free (CC BY 4.0) | Free (registration) | Free | Free (non-commercial) |
| **Optional indicators** | Energy, NOx | LaborIncome | Skill employment | LaborIncome |

★★★★★ = best among the four | ☆☆☆☆☆ = not available / very limited

---

## 11. Choosing a Database

### Decision guide

```
What is the primary use of the results?

├── Environmental impact assessment (GHG, energy, air quality)
│   └── Use EXIOBASE — richest satellite, best GHG detail
│       └── For file-backed accuracy: "exiobase_file"
│
├── Employment and social analysis
│   ├── Is the project in Europe / OECD country?
│   │   └── Use WIOD — best skill-level labour accounts
│   ├── Is the project in Africa / LATAM / South Asia?
│   │   └── Use Eora26 — only MRIO with individual country data
│   └── Need skill breakdown in non-OECD context?
│       └── Use WIOD base + Eora26 regional multipliers (not directly supported)
│
├── Policy analysis / trade-in-value-added
│   └── Use OECD ICIO — TiVA-linked, most current, 66 countries
│
├── Cross-database robustness check
│   └── Run tier0_all_databases() — returns all four calibrated backends
│       as a comparison DataFrame; the spread gives uncertainty bounds
│
└── No preference / quick screening
    └── Use EXIOBASE (default) — best-documented, most widely cited
```

### Uncertainty ranges

Running `tier0_all_databases()` for the same investment typically produces:

- **GHG spread:** ±7–12% across calibrated databases (EXIOBASE as reference)
- **Employment spread:** ±15–25% (largest for WIOD high-skill vs Eora26 total)
- **Water spread:** ±8–15% (largest for WIOD which lacks satellite)
- **Value Added spread:** ±3–5% (most stable across databases)

These spreads represent irreducible methodological uncertainty in global MRIO and should
be reported as confidence intervals rather than point estimates in any formal assessment.

### Recommended database by project type

| Project type | Primary | Secondary check | Rationale |
|---|---|---|---|
| European rail infrastructure | EXIOBASE | OECD | EU supply chains; GHG and TiVA important |
| Health facility in Africa | Eora26 | EXIOBASE | Country specificity; employment paramount |
| Hydro power in Asia | OECD or Eora26 | EXIOBASE | Mixed: country coverage vs. environmental detail |
| Health facility in Europe | EXIOBASE | WIOD | GHG + skill employment |
| Policy brief (any sector) | OECD | EXIOBASE | TiVA linkage; current data; OECD comparability |

---

*Sources cited throughout this document:*

- Stadler et al. (2018). EXIOBASE 3. *Journal of Industrial Ecology* 22(3):502–515. doi:10.1111/jiec.12715
- Lenzen et al. (2013). Building Eora. *Economic Systems Research* 25(1):20–49. doi:10.1080/09535314.2012.761953
- Timmer et al. (2015). WIOD. *Review of International Economics* 23(3):575–605. doi:10.1111/roie.12178
- OECD (2021). OECD ICIO Tables 2021 Edition. doi:10.1787/a8c8b9f0-en
- IEA (2022). World Energy Outlook 2022. International Energy Agency.
- ILO (2022). World Employment and Social Outlook 2022. ISBN 978-92-2-036643-6.
- Mekonnen & Hoekstra (2011). Water footprint of crops. *Hydrology and Earth System Sciences* 15:1577–1600.
- World Bank (2022). World Development Indicators. https://databank.worldbank.org
- IPCC AR6 WG3 (2022). Mitigation of Climate Change. Cambridge University Press.
