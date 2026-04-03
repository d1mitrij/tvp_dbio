#!/usr/bin/env python3
"""
fastinfra_benchmark.py
─────────────────────────────────────────────────────────────────────────────
Benchmarks hospitals, hydro_power_plant and rail self-assessment CSVs against
the complete FAST-Infra Sustainable Infrastructure Label indicator set using
pymrio's Extension / IOSystem framework.

Output: data/fastinfra_benchmark.csv
Rows  : every FAST-Infra indicator (registry + CSV-observed extras)
Cols  : metadata · one column per project · coverage stats · numeric aggregates
"""

import re
import warnings
import numpy as np
import pandas as pd
import pymrio
from pathlib import Path

warnings.filterwarnings("ignore")

DATA_DIR   = Path("data")
OUTPUT_CSV = DATA_DIR / "fastinfra_benchmark.csv"

# ══════════════════════════════════════════════════════════════════════════════
# 1.  COMPLETE FAST-INFRA INDICATOR REGISTRY
#     4 dimensions · 14 criteria · Minimum Safeguards · ~220 indicators
# ══════════════════════════════════════════════════════════════════════════════
_C = ["dimension", "criterion", "criterion_name",
      "indicator_id", "ind_type", "description", "unit"]

_ROWS = [
    # ── MINIMUM SAFEGUARDS ────────────────────────────────────────────────────
    ("Safeguards","MS","Minimum Safeguards","MS.YP8","MS",
     "ESIA by qualified independent firm","Yes/No"),
    ("Safeguards","MS","Minimum Safeguards","MS.1PV","MS",
     "Climate Risk & Resilience Assessment (TCFD-aligned)","Yes/No"),
    ("Safeguards","MS","Minimum Safeguards","MS.5BA","MS",
     "Sustainability and Mitigation Action Plan","Yes/No"),
    ("Safeguards","MS","Minimum Safeguards","MS.H4C","MS",
     "Stakeholder Engagement Plan","Yes/No"),
    ("Safeguards","MS","Minimum Safeguards","MS.TT5","MS",
     "ESMS reviewed externally","Yes/No"),
    ("Safeguards","MS","Minimum Safeguards","MS.W4A","MS",
     "IFC Performance Standards / Equator Principles alignment","Yes/No"),
    ("Safeguards","MS","Minimum Safeguards","MS.MW7","MS",
     "ESAP disclosed (optional)","Yes/No"),
    ("Safeguards","MS","Minimum Safeguards","MS.X4Y","MS",
     "Risk assessment on Associated Facilities (if applicable)","Yes/No"),
    # ── E1 — BIODIVERSITY AND ECOSYSTEM SERVICES ──────────────────────────────
    ("Environmental","E1","Biodiversity and Ecosystem Services","E1.FG3","Baseline",
     "Compliance with national/subnational biodiversity strategies","Yes/No"),
    ("Environmental","E1","Biodiversity and Ecosystem Services","E1.1YN","Baseline",
     "Avoidance of impacts on critical habitats and ecological corridors","Yes/No/Committed"),
    ("Environmental","E1","Biodiversity and Ecosystem Services","E1.8BR","Baseline",
     "Net gains (critical) or no net loss (non-critical habitats) demonstrated","Yes/No"),
    ("Environmental","E1","Biodiversity and Ecosystem Services","E1.UM1","Baseline",
     "Biodiversity and ecosystem service risk assessment conducted","Yes/No/Committed"),
    ("Environmental","E1","Biodiversity and Ecosystem Services","E1.QW4","Baseline",
     "Cumulative impacts assessed at landscape/seascape level","Yes/No/Committed"),
    ("Environmental","E1","Biodiversity and Ecosystem Services","E1.I5G","Baseline",
     "Long-term biodiversity monitoring scheme in place","Yes/No/Committed"),
    ("Environmental","E1","Biodiversity and Ecosystem Services","E1.IO1","Baseline",
     "Nature-Based Solutions incorporated (IUCN definition)","Yes/No"),
    ("Environmental","E1","Biodiversity and Ecosystem Services","E1.4OO.3.A","PC",
     "Expenditure on biodiversity measures — prior year","USD"),
    ("Environmental","E1","Biodiversity and Ecosystem Services","E1.4OO.3.B","PC",
     "Expenditure on biodiversity measures — forecasted","USD/year"),
    ("Environmental","E1","Biodiversity and Ecosystem Services","E1.UO5","PC",
     "Increase in area of natural habitats","ha"),
    ("Environmental","E1","Biodiversity and Ecosystem Services","E1.8OA","PC",
     "Decrease of degraded habitats","ha"),
    # ── E2 — CLIMATE CHANGE MITIGATION ───────────────────────────────────────
    ("Environmental","E2","Climate Change Mitigation","E2.D9U","Baseline",
     "Compliance with national/sub-national climate targets","Yes/No"),
    ("Environmental","E2","Climate Change Mitigation","E2.G2K","Baseline",
     "Scope 1, 2, 3 emissions monitored with mitigation measures","Yes — scope 1+2 / Yes — 1+2+3"),
    ("Environmental","E2","Climate Change Mitigation","E2.G2K.1","Baseline",
     "Scope 1 absolute emissions, prior calendar year","tCO2e"),
    ("Environmental","E2","Climate Change Mitigation","E2.G2K.2","Baseline",
     "Scope 2 absolute emissions, prior calendar year","tCO2e"),
    ("Environmental","E2","Climate Change Mitigation","E2.G2K.3","Baseline",
     "Scope 3 absolute emissions, prior calendar year","tCO2e"),
    ("Environmental","E2","Climate Change Mitigation","E2.O9E","Baseline",
     "Carbon management governance structure in place","Yes/No/Committed"),
    ("Environmental","E2","Climate Change Mitigation","E2.2SK","Baseline",
     "GHG reduction targets set (incl. Net Zero)","Yes/No/Committed"),
    ("Environmental","E2","Climate Change Mitigation","E2.TR9","Baseline",
     "GHG mitigation measures implemented","Yes/No/Committed"),
    ("Environmental","E2","Climate Change Mitigation","E2.ZT8","Baseline",
     "Climate mitigation requirements included in tendering","Yes/No/Committed"),
    ("Environmental","E2","Climate Change Mitigation","E2.C4U","Baseline",
     "Decommissioning plan includes emissions minimisation","Yes/No/Committed"),
    ("Environmental","E2","Climate Change Mitigation","E2.Q8W.A","PC",
     "Avoided GHG emissions — prior year","tCO2e"),
    ("Environmental","E2","Climate Change Mitigation","E2.Q8W.B","PC",
     "Avoided GHG emissions — forecasted","tCO2e/year"),
    # ── E3 — CIRCULAR ECONOMY AND RESOURCE EFFICIENCY ────────────────────────
    ("Environmental","E3","Circular Economy and Resource Efficiency","E3.8TF","Baseline",
     "Compliance with circular economy / waste / resource-efficiency policies","Yes/No"),
    ("Environmental","E3","Circular Economy and Resource Efficiency","E3.9NH","Baseline",
     "Life Cycle Assessment on material footprint conducted","Yes/No/Committed"),
    ("Environmental","E3","Circular Economy and Resource Efficiency","E3.TY9","Baseline",
     "Critical raw materials limited; recycled content maximised","Yes/No/Committed"),
    ("Environmental","E3","Circular Economy and Resource Efficiency","E3.8IE","Baseline",
     "Waste management plan developed","Yes/No/Committed"),
    ("Environmental","E3","Circular Economy and Resource Efficiency","E3.A4I","Baseline",
     "Strategies to reduce freshwater consumption","Yes/No/Committed"),
    ("Environmental","E3","Circular Economy and Resource Efficiency","E3.6XO.1","Baseline",
     "Decommissioning costs included in financial model","Yes/No"),
    ("Environmental","E3","Circular Economy and Resource Efficiency","E3.QK5.A","PC",
     "Water usage at project level — prior year","m3/year"),
    ("Environmental","E3","Circular Economy and Resource Efficiency","E3.QK5.B","PC",
     "Water usage at project level — forecasted","m3/year"),
    ("Environmental","E3","Circular Economy and Resource Efficiency","E3.6TP.3.A","PC",
     "Expenditure on circular economy measures — prior year","USD"),
    ("Environmental","E3","Circular Economy and Resource Efficiency","E3.6TP.3.B","PC",
     "Expenditure on circular economy measures — forecasted","USD/year"),
    ("Environmental","E3","Circular Economy and Resource Efficiency","E3.8VX","PC",
     "Tons of waste diverted from landfill","t"),
    ("Environmental","E3","Circular Economy and Resource Efficiency","E3.9IP","PC",
     "Reduction in water withdrawal","m3/year"),
    # ── E4 — POLLUTION PREVENTION AND CONTROL ────────────────────────────────
    ("Environmental","E4","Pollution Prevention and Control","E4.KV9","Baseline",
     "Compliance with pollution prevention and control policies","Yes/No"),
    ("Environmental","E4","Pollution Prevention and Control","E4.8KU","Baseline",
     "Pollution management system implemented","Yes/No/Committed"),
    ("Environmental","E4","Pollution Prevention and Control","E4.QN9","Baseline",
     "Hazardous waste generated and managed","Yes/No"),
    ("Environmental","E4","Pollution Prevention and Control","E4.V9T","Baseline",
     "Wastewater discharge norms met","Yes/No/Committed"),
    ("Environmental","E4","Pollution Prevention and Control","E4.HB5","Baseline",
     "Air emissions within national norms / WHO guidelines","Yes/No/Committed"),
    ("Environmental","E4","Pollution Prevention and Control","E4.6BK","Baseline",
     "Disturbance (noise, vibration, light, radiation) minimised","Yes/No/Committed"),
    ("Environmental","E4","Pollution Prevention and Control","E4.9LA","Baseline",
     "Cumulative Impact Assessment undertaken","Yes/No/Committed"),
    ("Environmental","E4","Pollution Prevention and Control","E4.O5C","PC",
     "Type of pollution improvement claimed","Category"),
    ("Environmental","E4","Pollution Prevention and Control","E4.2MB.1","PC",
     "Pollution prevention measure as % of CAPEX","%"),
    ("Environmental","E4","Pollution Prevention and Control","E4.2MB.3.A","PC",
     "Expenditure on pollution prevention — prior year","USD"),
    ("Environmental","E4","Pollution Prevention and Control","E4.2MB.3.B","PC",
     "Expenditure on pollution prevention — forecasted","USD/year"),
    ("Environmental","E4","Pollution Prevention and Control","E4.BR3.A","PC",
     "Solid waste safely managed — prior year","t/year"),
    ("Environmental","E4","Pollution Prevention and Control","E4.BR3.B","PC",
     "Solid waste safely managed — forecasted","t/year"),
    ("Environmental","E4","Pollution Prevention and Control","E4.S2H.A","PC",
     "Gaseous pollution safely managed — prior year","m3"),
    ("Environmental","E4","Pollution Prevention and Control","E4.S2H.B","PC",
     "Gaseous pollution safely managed — forecasted","m3/year"),
    ("Environmental","E4","Pollution Prevention and Control","E4.7BL.A","PC",
     "Wastewater safely managed — prior year","m3"),
    ("Environmental","E4","Pollution Prevention and Control","E4.7BL.B","PC",
     "Wastewater safely managed — forecasted","m3/year"),
    ("Environmental","E4","Pollution Prevention and Control","E4.T9A","PC",
     "Total people impacted by pollution prevention contribution","#"),
    # ── R5 — RESILIENCE AND CLIMATE ADAPTATION ────────────────────────────────
    ("Adaptation & Resilience","R5","Resilience and Climate Adaptation","R5.Z5G","Baseline",
     "Compliance with disaster/climate resilience policies","Yes/No"),
    ("Adaptation & Resilience","R5","Resilience and Climate Adaptation","R5.1JT","Baseline",
     "Physical Climate Risk Assessment conducted","Yes/No/Committed"),
    ("Adaptation & Resilience","R5","Resilience and Climate Adaptation","R5.1JT.1","Baseline",
     "Climate scenarios considered (incl. 1.5C, 2C, 4C)","Multi-select"),
    ("Adaptation & Resilience","R5","Resilience and Climate Adaptation","R5.R8S","Baseline",
     "Climate Transition Risk Assessment conducted","Yes/No/Committed"),
    ("Adaptation & Resilience","R5","Resilience and Climate Adaptation","R5.R8S.1","Baseline",
     "Transition scenarios considered","Multi-select"),
    ("Adaptation & Resilience","R5","Resilience and Climate Adaptation","R5.TZ1","Baseline",
     "Risk mitigation targets and programmes set","Yes/No/Committed"),
    ("Adaptation & Resilience","R5","Resilience and Climate Adaptation","R5.8GV","Baseline",
     "Structured governance framework for risk management","Yes/No/Committed"),
    ("Adaptation & Resilience","R5","Resilience and Climate Adaptation","R5.UP6","Baseline",
     "Adaptation measures implemented","Yes/No/Committed"),
    ("Adaptation & Resilience","R5","Resilience and Climate Adaptation","R5.V4H.1","PC",
     "Domain in which resilience is improved","Category"),
    ("Adaptation & Resilience","R5","Resilience and Climate Adaptation","R5.B6D.1","PC",
     "Resilience measure as % of Project CAPEX","%"),
    ("Adaptation & Resilience","R5","Resilience and Climate Adaptation","R5.B6D.3.A","PC",
     "Expenditure on resilience measures — prior year","USD"),
    ("Adaptation & Resilience","R5","Resilience and Climate Adaptation","R5.B6D.3.B","PC",
     "Expenditure on resilience measures — forecasted","USD/year"),
    ("Adaptation & Resilience","R5","Resilience and Climate Adaptation","R5.U2X.A","PC",
     "People positively impacted by resilience — prior year","people"),
    ("Adaptation & Resilience","R5","Resilience and Climate Adaptation","R5.U2X.B","PC",
     "People positively impacted by resilience — forecasted","people/year"),
    # ── S6 — INCLUSIVITY AND GENDER ───────────────────────────────────────────
    ("Social","S6","Inclusivity and Gender","S6.9LA","Baseline",
     "Compliance with gender/inclusion regulations","Yes/No"),
    ("Social","S6","Inclusivity and Gender","S6.LV7","Baseline",
     "GBVH risk assessment and action plan in place","Yes/No/Committed"),
    ("Social","S6","Inclusivity and Gender","S6.Z5Q","Baseline",
     "Gender Action Plan developed and implemented","Yes/No/Committed"),
    ("Social","S6","Inclusivity and Gender","S6.4PQ","Baseline",
     "Gender Action Plan includes measurable targets/KPIs","Yes/No"),
    ("Social","S6","Inclusivity and Gender","S6.LS4.1","Baseline",
     "Female representation on governing board","%"),
    ("Social","S6","Inclusivity and Gender","S6.X1I","Baseline",
     "Wage parity policy for equivalent roles","Yes/No/Committed"),
    ("Social","S6","Inclusivity and Gender","S6.Y7A","Baseline",
     "Zero-tolerance sexual harassment policy","Yes/No/Committed"),
    ("Social","S6","Inclusivity and Gender","S6.3WY.A","PC",
     "Female workers as % of permanent direct workforce — prior year","%"),
    ("Social","S6","Inclusivity and Gender","S6.3WY.B","PC",
     "Female workers as % of permanent direct workforce — forecasted","%"),
    ("Social","S6","Inclusivity and Gender","S6.Q5F.3.A","PC",
     "Expenditure on inclusivity/gender measures — prior year","USD"),
    ("Social","S6","Inclusivity and Gender","S6.Q5F.3.B","PC",
     "Expenditure on inclusivity/gender measures — forecasted","USD/year"),
    ("Social","S6","Inclusivity and Gender","S6.HB5","PC",
     "Number of vulnerable people benefitting","#"),
    ("Social","S6","Inclusivity and Gender","S6.KV9","PC",
     "Number of women benefitting","#"),
    # ── S7 — HEALTH AND SAFETY ────────────────────────────────────────────────
    ("Social","S7","Health and Safety","S7.YD5","Baseline",
     "OHS policy and plan for workers in place","Yes/No/Committed"),
    ("Social","S7","Health and Safety","S7.T8D","Baseline",
     "Designated OHS responsible person","Yes/No"),
    ("Social","S7","Health and Safety","S7.QO5","Baseline",
     "Compliance with OHS national laws and GIIP","Yes/No/Committed"),
    ("Social","S7","Health and Safety","S7.Z9O","Baseline",
     "Occupational accidents and injuries monitored","Yes/No"),
    ("Social","S7","Health and Safety","S7.Z9O.3","Baseline",
     "Non-fatal occupational accident rate","per 100,000 FTEs"),
    ("Social","S7","Health and Safety","S7.8BR","Baseline",
     "Health Impact Assessment conducted","Yes/No/Committed"),
    ("Social","S7","Health and Safety","S7.UM1","Baseline",
     "Community Health and Safety management plan","Yes/No/Committed"),
    # S7 PC indicators (observed in hospitals CSV)
    ("Social","S7","Health and Safety","S7.P5W","PC",
     "Improvement of H&S of surrounding communities","Yes/No"),
    ("Social","S7","Health and Safety","S7.LJ9","PC",
     "Type of H&S contribution claimed","Category"),
    ("Social","S7","Health and Safety","S7.EJ2","PC",
     "Nature-Based Solutions used in H&S contribution","Yes/No"),
    ("Social","S7","Health and Safety","S7.P8B","PC",
     "H&S positive contribution — quantification method","Category"),
    ("Social","S7","Health and Safety","S7.P8B.1","PC",
     "H&S measures as % of CAPEX","%"),
    ("Social","S7","Health and Safety","S7.P8B.3","PC",
     "H&S measures yearly expenditure (observed, no .A/.B suffix)","USD/year"),
    ("Social","S7","Health and Safety","S7.P8B.3.A","PC",
     "Expenditure on H&S measures — prior year","USD"),
    ("Social","S7","Health and Safety","S7.P8B.3.B","PC",
     "Expenditure on H&S measures — forecasted","USD/year"),
    ("Social","S7","Health and Safety","S7.X4B","PC",
     "Number of people with improved H&S","#"),
    # ── S8 — HUMAN RIGHTS AND LABOUR RIGHTS ──────────────────────────────────
    ("Social","S8","Human Rights and Labour Rights","S8.FD1","Baseline",
     "Workers' rights aligned with ILO conventions","Yes/No/Committed"),
    ("Social","S8","Human Rights and Labour Rights","S8.FA6","Baseline",
     "Forced labour and child labour prohibited","Yes/No/Committed"),
    ("Social","S8","Human Rights and Labour Rights","S8.A7L","Baseline",
     "Freedom of association and collective bargaining guaranteed","Yes/No/Committed"),
    ("Social","S8","Human Rights and Labour Rights","S8.U3O","Baseline",
     "Equal opportunity and non-discrimination policy","Yes/No/Committed"),
    ("Social","S8","Human Rights and Labour Rights","S8.YL6","Baseline",
     "No child-labour clause in all contracts","Yes/No/Committed"),
    ("Social","S8","Human Rights and Labour Rights","S8.9BT","Baseline",
     "Human rights due diligence assessment conducted","Yes/No/Committed"),
    ("Social","S8","Human Rights and Labour Rights","S8.9MP","Baseline",
     "Grievance mechanism accessible to all workers","Yes/No/Committed"),
    ("Social","S8","Human Rights and Labour Rights","S8.W7L","Baseline",
     "Remuneration policy meets at least minimum wage","Yes/No/Committed"),
    ("Social","S8","Human Rights and Labour Rights","S8.7ZH.3.A","PC",
     "Expenditure on human/labour rights measures — prior year","USD"),
    ("Social","S8","Human Rights and Labour Rights","S8.7ZH.3.B","PC",
     "Expenditure on human/labour rights measures — forecasted","USD/year"),
    ("Social","S8","Human Rights and Labour Rights","S8.I8Q","PC",
     "Number of people with secure employment created","#"),
    ("Social","S8","Human Rights and Labour Rights","S8.GC2","PC",
     "Percentage of local workforce","%"),
    # ── S9 — LAND ACQUISITION AND RESETTLEMENT ────────────────────────────────
    ("Social","S9","Land Acquisition and Resettlement","S9.O7H","Baseline",
     "Project-related land displacement identified","Yes/No"),
    ("Social","S9","Land Acquisition and Resettlement","S9.9LQ","Baseline",
     "RAP / LRP in place","Yes/No/Committed"),
    ("Social","S9","Land Acquisition and Resettlement","S9.OE1","Baseline",
     "Eligible persons identified via census","Yes/No/Committed"),
    ("Social","S9","Land Acquisition and Resettlement","S9.OE1.1","Baseline",
     "Compensation provided at full replacement cost","Yes/No/Committed"),
    ("Social","S9","Land Acquisition and Resettlement","S9.OE1.3","Baseline",
     "Vulnerable groups met with separately","Yes/No/Committed"),
    ("Social","S9","Land Acquisition and Resettlement","S9.2QR","Baseline",
     "Project design minimises displacement","Yes/No/Committed"),
    ("Social","S9","Land Acquisition and Resettlement","S9.8JJ","Baseline",
     "Resettlement plan addresses climate impacts","Yes/No/Committed"),
    ("Social","S9","Land Acquisition and Resettlement","S9.B1G.3.A","PC",
     "Expenditure on land/resettlement measures — prior year","USD"),
    ("Social","S9","Land Acquisition and Resettlement","S9.B1G.3.B","PC",
     "Expenditure on land/resettlement measures — forecasted","USD/year"),
    ("Social","S9","Land Acquisition and Resettlement","S9.QN8","PC",
     "Number of affected people benefitting","#"),
    # ── S10 — STAKEHOLDER ENGAGEMENT (baseline only) ─────────────────────────
    ("Social","S10","Stakeholder Engagement","S10.HW8","Baseline",
     "Stakeholder mapping undertaken","Yes/No/Committed"),
    ("Social","S10","Stakeholder Engagement","S10.K2M","Baseline",
     "Stakeholder Engagement Plan considers all needs","Yes/No/Committed"),
    ("Social","S10","Stakeholder Engagement","S10.8CC","Baseline",
     "Engagement plan revised annually","Yes/No/Committed"),
    ("Social","S10","Stakeholder Engagement","S10.A7P","Baseline",
     "Number of directly affected people","#"),
    ("Social","S10","Stakeholder Engagement","S10.KW5.1","Baseline",
     "Indigenous peoples consulted","Yes/No"),
    ("Social","S10","Stakeholder Engagement","S10.OF4.2","Baseline",
     "Free, Prior and Informed Consent (FPIC) obtained","Yes/No"),
    ("Social","S10","Stakeholder Engagement","S10.5SI.1","Baseline",
     "Grievances received (count)","#"),
    ("Social","S10","Stakeholder Engagement","S10.5SI.2","Baseline",
     "Grievances addressed/resolved (count)","#"),
    ("Social","S10","Stakeholder Engagement","S10.SO5.A","Baseline",
     "Budget for stakeholder engagement — development/construction","USD"),
    ("Social","S10","Stakeholder Engagement","S10.SO5.B","Baseline",
     "Budget for stakeholder engagement — forecasted","USD"),
    ("Social","S10","Stakeholder Engagement","S10.W5V.A","Baseline",
     "Expenditure on stakeholder engagement — operations, prior year","USD"),
    ("Social","S10","Stakeholder Engagement","S10.W5V.B","Baseline",
     "Expenditure on stakeholder engagement — operations, forecasted","USD/year"),
    # ── G11 — ANTI-CORRUPTION (baseline only) ────────────────────────────────
    ("Governance","G11","Anti-Corruption","G11.PT2","Baseline",
     "Comprehensive anti-corruption management system in place","Yes/No/Committed"),
    ("Governance","G11","Anti-Corruption","G11.N1K","Baseline",
     "Annual reviews of anti-corruption system","Yes/No/Committed"),
    ("Governance","G11","Anti-Corruption","G11.9SA","Baseline",
     "Annual training on code of conduct","Yes/No/Committed"),
    ("Governance","G11","Anti-Corruption","G11.9SA.1","Baseline",
     "Percentage of staff trained on code of conduct","%"),
    ("Governance","G11","Anti-Corruption","G11.4UO","Baseline",
     "Corruption risk assessment conducted","Yes/No/Committed"),
    ("Governance","G11","Anti-Corruption","G11.PV5","Baseline",
     "Internal whistleblowing system in place","Yes/No/Committed"),
    ("Governance","G11","Anti-Corruption","G11.4UQ","Baseline",
     "Confirmed corruption incidents (count)","#"),
    # ── G12 — TRANSPARENCY AND ACCOUNTABILITY (baseline only) ────────────────
    ("Governance","G12","Transparency and Accountability","G12.PQ5","Baseline",
     "Compliance with applicable transparency laws","Yes/No"),
    ("Governance","G12","Transparency and Accountability","G12.YL6.1","Baseline",
     "Tendering conducted according to best practices","Yes/No/Committed"),
    ("Governance","G12","Transparency and Accountability","G12.YL6.2","Baseline",
     "Sustainability criteria included in tender","Yes/No/Committed"),
    ("Governance","G12","Transparency and Accountability","G12.W7L","Baseline",
     "Ultimate beneficial owners disclosed","Yes/No/Committed"),
    ("Governance","G12","Transparency and Accountability","G12.2VU","Baseline",
     "Environmental and social targets/risks disclosed publicly","Yes/No/Committed"),
    ("Governance","G12","Transparency and Accountability","G12.2AG","Baseline",
     "Nature-related impacts disclosed (TNFD-aligned)","Yes/No/Committed"),
    ("Governance","G12","Transparency and Accountability","G12.2AG.1","Baseline",
     "TNFD disclosure pillars covered","Multi-select"),
    # ── G13 — FINANCIAL INTEGRITY AND FISCAL TRANSPARENCY (baseline only) ────
    ("Governance","G13","Financial Integrity and Fiscal Transparency","G13.Q7P","Baseline",
     "Required financial information reported","Yes/No"),
    ("Governance","G13","Financial Integrity and Fiscal Transparency","G13.5UW","Baseline",
     "Tax strategy coherent with business operations","Yes/No/Committed"),
    ("Governance","G13","Financial Integrity and Fiscal Transparency","G13.5PI","Baseline",
     "Located in non-cooperative tax jurisdiction (flag)","Yes/No"),
    ("Governance","G13","Financial Integrity and Fiscal Transparency","G13.RL7","Baseline",
     "Audited financial statements demonstrated","Yes/No/Committed"),
    ("Governance","G13","Financial Integrity and Fiscal Transparency","G13.BH8.1","Baseline",
     "Financial model shared with government","Yes/No/Committed"),
    # ── G14 — SUSTAINABLE REPORTING AND COMPLIANCE ────────────────────────────
    ("Governance","G14","Sustainable Reporting and Compliance","G14.H3V","Baseline",
     "Serious violations in the last five years","Yes/No"),
    ("Governance","G14","Sustainable Reporting and Compliance","G14.IL3","Baseline",
     "Environmental and social actions included in project budget","Yes/No/Committed"),
    ("Governance","G14","Sustainable Reporting and Compliance","G14.N4B.A","PC",
     "E&S actions budget — development/construction","USD"),
    ("Governance","G14","Sustainable Reporting and Compliance","G14.N4B.B","PC",
     "E&S actions budget — forecasted","USD"),
    ("Governance","G14","Sustainable Reporting and Compliance","G14.2AG.A","PC",
     "E&S actions expenditure — operations, prior year","USD"),
    ("Governance","G14","Sustainable Reporting and Compliance","G14.2AG.B","PC",
     "E&S actions expenditure — operations, forecasted","USD/year"),
]

REGISTRY = pd.DataFrame(_ROWS, columns=_C).set_index("indicator_id")


# ══════════════════════════════════════════════════════════════════════════════
# 2.  LOAD CSVs — EXTRACT INDICATOR VALUES PER PROJECT
# ══════════════════════════════════════════════════════════════════════════════
_ID_RE = re.compile(r"\(([A-Za-z0-9]+(?:\.[A-Za-z0-9]+)+)\)$")


def _extract_id(col_name):
    """Return the FAST-Infra indicator ID from 'Desc (X.Y1Z)' or None."""
    m = _ID_RE.search(col_name.strip())
    return m.group(1) if m else None


def _load_sector(path, prefix):
    """Return {indicator_id: {project_label: raw_value}}."""
    df = pd.read_csv(path)
    data = {}
    for col in df.columns:
        ind_id = _extract_id(col)
        if ind_id is None:
            continue
        for _, row in df.iterrows():
            proj = f"{prefix}_P{int(float(row['Project_ID']))}"
            data.setdefault(ind_id, {})[proj] = row[col]
    return data


hosp_vals  = _load_sector(DATA_DIR / "hospitals.csv",        "hospitals")
hydro_vals = _load_sector(DATA_DIR / "hydro_power_plant.csv","hydro")
rail_vals  = _load_sector(DATA_DIR / "rail.csv",             "rail")


# ══════════════════════════════════════════════════════════════════════════════
# 3.  ASSEMBLE MULTI-SECTOR VALUE MATRIX
# ══════════════════════════════════════════════════════════════════════════════
REGIONS   = ["hospitals", "hydro", "rail"]
SECTORS   = ["P1", "P2", "P3"]
PROJECTS  = [f"{r}_{s}" for r in REGIONS for s in SECTORS]

# Union of indicator IDs: registry first (preserves logical order), then CSV extras
all_ids = list(REGISTRY.index)
for src in (hosp_vals, hydro_vals, rail_vals):
    for k in src:
        if k not in all_ids:
            all_ids.append(k)

# Raw value matrix — object dtype preserves original strings and numbers
F_raw = pd.DataFrame(np.nan, index=all_ids, columns=PROJECTS, dtype=object)

for src in (hosp_vals, hydro_vals, rail_vals):
    for ind_id, proj_map in src.items():
        for proj, val in proj_map.items():
            F_raw.loc[ind_id, proj] = val


# ══════════════════════════════════════════════════════════════════════════════
# 4.  NUMERIC ENCODING FOR PYMRIO
#     Yes = 1.0 | No = 0.0 | Committed = 0.5 | numeric strings → float
#     "Not answered", "N/A", "" → NaN (no data)
#     "Not a claimed PC" → NaN in numeric matrix (kept as-is in raw)
# ══════════════════════════════════════════════════════════════════════════════
_NULL_ENCODE  = {"not answered", "n/a", "", "nan", "not a claimed pc"}
_YESNO_MAP    = {"yes": 1.0, "no": 0.0, "committed": 0.5}


def _encode(v):
    """Convert a raw cell value to float for the pymrio Extension F matrix."""
    try:
        if pd.isna(v):
            return np.nan
    except (TypeError, ValueError):
        pass
    if isinstance(v, bool):
        return 1.0 if v else 0.0
    if isinstance(v, (int, float)):
        return float(v)
    s = str(v).strip().lower()
    if s in _NULL_ENCODE:
        return np.nan
    if s in _YESNO_MAP:
        return _YESNO_MAP[s]
    # Attempt numeric parse: handle "5'500'000 ppl/year", "<1%", "250 million", etc.
    first_tok = s.split()[0] if s else ""
    clean = re.sub(r"[^0-9.\-]", "", first_tok)
    try:
        return float(clean) if clean else np.nan
    except ValueError:
        return np.nan


F_encoded = pd.DataFrame(
    {col: F_raw[col].map(_encode) for col in PROJECTS},
    index=all_ids,
    dtype=float,
)


# ══════════════════════════════════════════════════════════════════════════════
# 5.  BUILD PYMRIO IOSystem WITH fastinfra EXTENSION
# ══════════════════════════════════════════════════════════════════════════════
# Multi-index (region, sector) required by pymrio conventions
_pymrio_regions = ["hospitals", "hydro", "rail"]
mi = pd.MultiIndex.from_tuples(
    [(r, s) for r in _pymrio_regions for s in SECTORS],
    names=["region", "sector"],
)

# Placeholder Z and Y — we use pymrio purely for the Extension framework
Z = pd.DataFrame(0.0, index=mi, columns=mi)
Y = pd.DataFrame(
    1.0,
    index=mi,
    columns=pd.MultiIndex.from_product(
        [_pymrio_regions, ["final_demand"]], names=["region", "category"]
    ),
)

# Extension F matrix with MultiIndex columns
F_mi = F_encoded.copy()
F_mi.columns = mi

fastinfra_ext = pymrio.Extension(name="fastinfra", F=F_mi)

io = pymrio.IOSystem(Z=Z, Y=Y)
io.fastinfra = fastinfra_ext


# ══════════════════════════════════════════════════════════════════════════════
# 6.  BUILD BENCHMARK DATAFRAME
# ══════════════════════════════════════════════════════════════════════════════
# Start from the registry; reindex to include CSV-observed extras
bench = REGISTRY.reindex(all_ids).copy()

# Fill metadata gaps for any indicator found in CSVs but absent from registry
bench.loc[bench["dimension"].isna(), "dimension"]             = "Unclassified"
bench.loc[bench["criterion"].isna(), "criterion"]             = "—"
bench.loc[bench["criterion_name"].isna(), "criterion_name"]   = "—"
bench.loc[bench["ind_type"].isna(), "ind_type"]               = "—"
bench.loc[bench["description"].isna(), "description"]         = "—"
bench.loc[bench["unit"].isna(), "unit"]                       = "—"

# Attach raw project values
for proj in PROJECTS:
    bench[proj] = F_raw[proj]

# ── Coverage stats ────────────────────────────────────────────────────────────
_NULL_DISPLAY = {"not answered", "n/a", "", "nan"}


def _is_reported(v):
    """True for any substantive response (including 'Not a claimed PC')."""
    try:
        if pd.isna(v):
            return False
    except (TypeError, ValueError):
        pass
    return str(v).strip().lower() not in _NULL_DISPLAY


bench["n_reported"]   = bench[PROJECTS].apply(
    lambda row: sum(_is_reported(v) for v in row), axis=1
)
bench["coverage_pct"] = (bench["n_reported"] / len(PROJECTS) * 100).round(1)


# ── Numeric aggregates (only when raw values contain at least one real number) ──
def _num_stats(ind_id):
    raw  = F_raw.loc[ind_id]
    vals = F_encoded.loc[ind_id].dropna()
    if vals.empty:
        return np.nan, np.nan, np.nan
    has_num = any(
        isinstance(v, (int, float)) and not (isinstance(v, float) and np.isnan(v))
        for v in raw
    )
    if not has_num:
        return np.nan, np.nan, np.nan
    return round(float(vals.mean()), 2), round(float(vals.min()), 2), round(float(vals.max()), 2)


means, mins, maxs = [], [], []
for iid in bench.index:
    m, lo, hi = _num_stats(iid)
    means.append(m); mins.append(lo); maxs.append(hi)

bench["numeric_mean"] = means
bench["numeric_min"]  = mins
bench["numeric_max"]  = maxs

# ── Final column ordering ─────────────────────────────────────────────────────
META_COLS = ["dimension", "criterion", "criterion_name", "ind_type", "description", "unit"]
STAT_COLS = ["n_reported", "coverage_pct", "numeric_mean", "numeric_min", "numeric_max"]

bench = bench.reset_index().rename(columns={"index": "indicator_id"})
bench = bench[["indicator_id"] + META_COLS + PROJECTS + STAT_COLS]

bench.to_csv(OUTPUT_CSV, index=False, encoding="utf-8")


# ══════════════════════════════════════════════════════════════════════════════
# 7.  SUMMARY REPORT
# ══════════════════════════════════════════════════════════════════════════════
total    = len(bench)
covered  = int((bench["n_reported"] > 0).sum())

print(f"\nBenchmark written  →  {OUTPUT_CSV}")
print(f"  Total indicators  : {total}")
print(f"  With >= 1 data pt : {covered}  ({covered/total*100:.1f}%)")
print(f"  Projects          : {len(PROJECTS)}  "
      f"(hospitals P1-P3 · hydro P1-P3 · rail P1-P3)\n")

print("Coverage by dimension:")
dim_summary = (
    bench.groupby("dimension")
    .agg(
        total_indicators=("indicator_id", "count"),
        indicators_reported=("n_reported", lambda x: (x > 0).sum()),
    )
    .assign(coverage_pct=lambda d: (d["indicators_reported"] / d["total_indicators"] * 100).round(1))
    .sort_values("coverage_pct", ascending=False)
)
print(dim_summary.to_string())

print("\nCoverage by criterion (reported only):")
crit_summary = (
    bench[bench["n_reported"] > 0]
    .groupby(["criterion", "criterion_name"])
    .agg(
        indicators_with_data=("indicator_id", "count"),
        avg_project_coverage=("coverage_pct", "mean"),
    )
    .round(1)
    .sort_values("criterion")
)
print(crit_summary.to_string())
