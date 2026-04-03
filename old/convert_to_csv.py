import csv
import openpyxl
from pathlib import Path

wb = openpyxl.load_workbook("data/20260317_Data_Greenings_clean_NG.xlsx", read_only=True)
ws = wb["Sheet1"]
rows = list(ws.iter_rows(values_only=True))

output_dir = Path("data")

# ── HOSPITALS ────────────────────────────────────────────────────────────────
# Data rows: 9-11 (0-indexed: 8-10)
hospital_headers = [
    "Project_ID",
    "Region",
    "Stage",
    "H&S_Improvement_Communities (S7.P5W)",
    "Contribution_To (S7.LJ9)",
    "Nature_Based_Solutions (S7.EJ2)",
    "Quantification (S7.P8B)",
    "CAPEX_Pct (S7.P8B.1)",
    "Yearly_Expenditure (S7.P8B.3)",
    "People_With_Improved_H&S (S7.X4B)",
    "Comment",
]
hospital_data = [rows[i] for i in range(8, 11)]  # rows 9-11

with open(output_dir / "hospitals.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(hospital_headers)
    for row in hospital_data:
        writer.writerow([row[0], row[1], row[2], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11]])

print("hospitals.csv written")

# ── HYDRO POWER PLANT ────────────────────────────────────────────────────────
# Data rows: 21-23 (0-indexed: 20-22)
hydro_headers = [
    "Project_ID",
    "Region",
    "Stage",
    "Avoided_Emissions_Prior_Year_tCO2e (E2.Q8W.A)",
    "Avoided_Emissions_1st_Year_tCO2e (E2.Q8W.B)",
    "People_With_Improved_H&S (S7.X4B)",
    "Comment",
]
hydro_data = [rows[i] for i in range(20, 23)]  # rows 21-23

with open(output_dir / "hydro_power_plant.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(hydro_headers)
    for row in hydro_data:
        writer.writerow([row[0], row[1], row[2], row[4], row[5], row[6], row[7]])

print("hydro_power_plant.csv written")

# ── RAIL ─────────────────────────────────────────────────────────────────────
# Data rows: 32-34 (0-indexed: 31-33)
rail_headers = [
    "Project_ID",
    "Region",
    "Stage",
    "Avoided_Emissions_Prior_Year_tCO2e (E2.Q8W.A)",
    "Pollution_Improvement_Type (E4.O5C)",
    "Pollution_Measure_CAPEX_Pct (E4.2MB.1)",
    "People_Impacted_Pollution_Reduction (E4.T9A)",
    "Resilience_Domain (R5.V4H.1)",
    "Resilience_Measure_CAPEX_Pct (R5.B6D.1)",
    "People_Impacted_Resilience (R5.U2X.A)",
]
rail_data = [rows[i] for i in range(31, 34)]  # rows 32-34

with open(output_dir / "rail.csv", "w", newline="", encoding="utf-8") as f:
    writer = csv.writer(f)
    writer.writerow(rail_headers)
    for row in rail_data:
        writer.writerow([row[0], row[1], row[2], row[4], row[5], row[6], row[7], row[8], row[9], row[10]])

print("rail.csv written")
print("\nDone.")
