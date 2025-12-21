from __future__ import annotations

import random
from pathlib import Path
from datetime import datetime, timedelta, timezone

import pandas as pd


def rchoice(rng: random.Random, items):
    return items[rng.randrange(len(items))]


def main(seed: int = 42, n_patients: int = 250, n_encounters: int = 1200) -> None:
    rng = random.Random(seed)

    out_dir = Path("data/raw")
    out_dir.mkdir(parents=True, exist_ok=True)

    # --- Reference sets ---
    departments = [
        ("ED", "Emergency"),
        ("MED", "Med/Surg"),
        ("ICU", "ICU"),
        ("LAB", "Laboratory"),
        ("RAD", "Radiology"),
        ("OB", "OB/GYN"),
        ("PT", "Physical Therapy"),
    ]

    encounter_types = ["ED", "Inpatient", "Outpatient", "Observation"]

    providers = []
    for i in range(1, 41):
        dept_id, _ = rchoice(rng, departments)
        providers.append((f"PRV{i:04d}", f"Provider {i:02d}", dept_id))

    # --- Patients ---
    patients = []
    for i in range(1, n_patients + 1):
        patient_id = f"PAT{i:05d}"
        birth_year = rng.randint(1935, 2020)
        sex = rchoice(rng, ["F", "M"])
        patients.append((patient_id, birth_year, sex))
    df_patients = pd.DataFrame(patients, columns=["patient_id", "birth_year", "sex"])

    # --- Encounters ---
    # Generate encounters over last ~18 months
    now = datetime.now(timezone.utc)
    start = now - timedelta(days=540)

    enc_rows = []
    for i in range(1, n_encounters + 1):
        encounter_id = f"ENC{i:06d}"
        patient_id, _, _ = rchoice(rng, patients)

        provider_id, _, dept_id = rchoice(rng, providers) 
        etype = rchoice(rng, encounter_types)

        admit_ts = start + timedelta(minutes=rng.randint(0, 540 * 24 * 60))

        # LOS distribution by type 
        if etype == "Inpatient":
            los_hours = rng.randint(24, 24 * 10)
        elif etype == "Observation": 
            los_hours = rng.randint(8, 36)
        elif etype == "ED":
            los_hours = rng.randint(1, 12)
        else:
            los_hours = rng.randint(1, 6)

        discharge_ts = admit_ts + timedelta(hours=los_hours)

        enc_rows.append(
            (encounter_id, patient_id, provider_id, dept_id, admit_ts.isoformat(), discharge_ts.isoformat(), etype)
        )

    df_enc = pd.DataFrame(
        enc_rows,
        columns=[
            "encounter_id",
            "patient_id", 
            "provider_id",
            "department_id",
            "admit_ts", 
            "discharge_ts",
            "encounter_type", 
        ],
    )

    # --- Charges (financial-ish) ---
    cpt_codes = ["99283", "99284", "99285", "93000", "80053", "85025", "71045", "74177", "36415"]
    charge_rows = []
    charge_id_counter = 1
    for _, row in df_enc.iterrows():
        # number of charge lines
        n_lines = rng.randint(1, 8)
        base_ts = datetime.fromisoformat(row["admit_ts"])
        for _ in range(n_lines):
            charge_id = f"CHG{charge_id_counter:08d}"
            charge_id_counter += 1
            cpt = rchoice(rng, cpt_codes) 
            # amounts: skewed positive; keep realistic-ish
            amount = round(max(5.0, rng.lognormvariate(6.2, 0.6)), 2) 
            posted_ts = base_ts + timedelta(hours=rng.randint(0, 48)) 
            charge_rows.append((charge_id, row["encounter_id"], cpt, amount, posted_ts.isoformat())) 

    df_chg = pd.DataFrame(charge_rows, columns=["charge_id", "encounter_id", "cpt_code", "amount", "posted_ts"])

    # --- Labs (clinical-ish) ---
    loinc = [
        ("718-7", "Hemoglobin", "g/dL"),  
        ("4548-4", "Hematocrit", "%"), 
        ("6690-2", "WBC", "10^3/uL"), 
        ("2951-2", "Sodium", "mmol/L"), 
        ("2823-3", "Potassium", "mmol/L"),
        ("2075-0", "Chloride", "mmol/L"), 
        ("2160-0", "Creatinine", "mg/dL"), 
    ]
    lab_rows = []
    lab_id_counter = 1
    for _, row in df_enc.iterrows():
        # not every encounter has labs
        if rng.random() < 0.55:
            n_labs = rng.randint(1, 6)
            base_ts = datetime.fromisoformat(row["admit_ts"])
            for _ in range(n_labs):
                lab_id = f"LAB{lab_id_counter:08d}"
                lab_id_counter += 1
                loinc_code, _, unit = rchoice(rng, loinc)

                # crude value generation by test
                if loinc_code == "718-7":
                    val = round(rng.uniform(10.0, 17.5), 1)
                elif loinc_code == "4548-4":
                    val = round(rng.uniform(30.0, 52.0), 1)
                elif loinc_code == "6690-2":
                    val = round(rng.uniform(3.5, 17.0), 1)
                elif loinc_code == "2951-2":
                    val = round(rng.uniform(130.0, 150.0), 1)
                elif loinc_code == "2823-3":
                    val = round(rng.uniform(3.0, 5.8), 1)
                elif loinc_code == "2075-0":
                    val = round(rng.uniform(95.0, 110.0), 1)
                else:  # creatinine
                    val = round(rng.uniform(0.5, 2.2), 2)

                result_ts = base_ts + timedelta(hours=rng.randint(1, 24))
                lab_rows.append((lab_id, row["encounter_id"], loinc_code, val, unit, result_ts.isoformat()))

    df_lab = pd.DataFrame(
        lab_rows, columns=["lab_id", "encounter_id", "loinc_code", "result_value", "unit", "result_ts"]
    )

    # --- Staff (HR-ish) ---
    roles = ["RN", "MD", "PA", "NP", "Tech", "Admin"]
    staff_rows = []
    for i in range(1, 61):
        staff_id = f"STF{i:05d}" 
        provider_id, _, _ = rchoice(rng, providers) 
        role = rchoice(rng, roles) 
        hire_date = (now - timedelta(days=rng.randint(30, 3650))).date().isoformat() 
        staff_rows.append((staff_id, provider_id, role, hire_date)) 
    df_staff = pd.DataFrame(staff_rows, columns=["staff_id", "provider_id", "role", "hire_date"])

    # --- Save ---
    df_enc.to_csv(out_dir / "encounters.csv", index=False)
    df_chg.to_csv(out_dir / "charges.csv", index=False)
    df_lab.to_csv(out_dir / "labs.csv", index=False)
    df_staff.to_csv(out_dir / "staff.csv", index=False)
    df_patients.to_csv(out_dir / "patients.csv", index=False)

    print("Wrote:")
    for p in ["patients.csv", "encounters.csv", "charges.csv", "labs.csv", "staff.csv"]:
        print(" -", out_dir / p)


if __name__ == "__main__":
    main()
