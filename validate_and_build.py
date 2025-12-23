from __future__ import annotations

import os
from datetime import datetime
import pandas as pd
import argparse

from dotenv import load_dotenv
from sqlalchemy import create_engine, text


def fail(msg: str):
    raise RuntimeError(f"VALIDATION FAILED: {msg}")

def parse_args():
    parser = argparse.ArgumentParser(description="Validate staging data and build warehouse")
    parser.add_argument(
        "--source",
        choices=["csv", "fhir"],
        default="csv",
        help="Upstream data source to build warehouse from",
    )
    return parser.parse_args()


def main():

    # Args for CSV or FHIR (just the one flag)
    args = parse_args()
    source = args.source

    load_dotenv()
    engine = create_engine(os.getenv("DATABASE_URL"), future=True)

    with engine.begin() as conn:
        run_id = conn.execute(
            text(
                "INSERT INTO etl_run_log(status, notes) "
                "VALUES ('running', 'validate + build warehouse') "
                "RETURNING run_id"
            )
        ).scalar_one()

    # ----------------------------
    # Load staging tables depending on arg
    # ----------------------------
    if source == "csv":
        patients = pd.read_sql("SELECT * FROM stg_patients", engine)
        encounters = pd.read_sql("SELECT * FROM stg_encounters", engine)
        charges = pd.read_sql("SELECT * FROM stg_charges", engine)
        labs = pd.read_sql("SELECT * FROM stg_labs", engine)
        staff = pd.read_sql("SELECT * FROM stg_staff", engine)

    elif source == "fhir":
        patients = pd.read_sql("SELECT * FROM stg_fhir_patient", engine)
        encounters = pd.read_sql("SELECT * FROM stg_fhir_encounter", engine)
        charges = pd.read_sql("SELECT * FROM stg_fhir_chargeitem", engine)
        labs = pd.read_sql("SELECT * FROM stg_fhir_observation", engine)
        staff = pd.DataFrame()  # FHIR bundle doesn't include HR data

    #----------------------------
    # If FHIR arg then we need to normalize tables so we have expected values/names
    #----------------------------
    if source == "fhir":
        # Normalize FHIR patients
        patients = patients.rename(
            columns={
                "birth_date": "birth_year",
                "gender": "sex",
            }
        )

        patients["birth_year"] = (
            pd.to_datetime(patients["birth_year"], errors="coerce").dt.year
        )

        # Standardize sex values to match CSV conventions
        patients["sex"] = patients["sex"].map(
            {
                "male": "M",
                "female": "F",
                "other": "O",
                "unknown": None,
            }
        )

        # Normalize encounters
        encounters = encounters.rename(
            columns={
                "start_ts": "admit_ts",
                "end_ts": "discharge_ts",
                "department": "department_id",
            }
        )

        # Normalize charges
        charges = charges.rename(
            columns={
                "amount": "amount"
            }
        )

        # Normalize labs
        labs = labs.rename(
            columns={
                "loinc_code": "loinc_code",
                "effective_ts": "result_ts",
                "value": "result_value",
            }
        )

        # provider_id doesn't exist in our FHIR staging; create a stable surrogate
        if "provider_id" not in encounters.columns:
            encounters["provider_id"] = encounters.get("provider_name")

        # encounter_type in CSV exists; for FHIR, use class_display (or class_code)
        if "encounter_type" not in encounters.columns:
            encounters["encounter_type"] = encounters.get("class_display").fillna(encounters.get("class_code"))

    # ----------------------------
    # VALIDATION 
    # ----------------------------
    if patients["patient_id"].duplicated().any():
        fail("Duplicate patient_id in stg_patients")

    if encounters["encounter_id"].duplicated().any(): 
        fail("Duplicate encounter_id in stg_encounters")

    if not charges.empty and (charges["amount"] < 0).any():
        fail("Negative charge amounts detected")

    encounters["admit_ts"] = pd.to_datetime(encounters["admit_ts"])
    encounters["discharge_ts"] = pd.to_datetime(encounters["discharge_ts"]) 

    if (encounters["discharge_ts"] < encounters["admit_ts"]).any():
        fail("Discharge before admit detected")

    # Foreign key checks 
    enc_ids = set(encounters["encounter_id"])
    if not set(charges["encounter_id"]).issubset(enc_ids):
        fail("Charges reference missing encounters")

    if not set(labs["encounter_id"]).issubset(enc_ids):
        fail("Labs reference missing encounters") 

    # ----------------------------
    # DIMENSIONS
    # ---------------------------- 
    dim_patient = patients[["patient_id", "birth_year", "sex"]].drop_duplicates()

    dim_department = encounters[["department_id"]].drop_duplicates() 
    dim_department["department_name"] = dim_department["department_id"]

    dim_provider = encounters[["provider_id", "department_id"]].drop_duplicates()
    dim_provider["provider_name"] = dim_provider["provider_id"] 

    # Time dimension
    all_dates = pd.concat( 
        [
            encounters["admit_ts"].dt.date,
            encounters["discharge_ts"].dt.date, 
        ]
    ).drop_duplicates()
 
    dim_time = pd.DataFrame({"date_key": all_dates}) 
    dim_time["year"] = dim_time["date_key"].apply(lambda d: d.year)
    dim_time["month"] = dim_time["date_key"].apply(lambda d: d.month) 
    dim_time["day"] = dim_time["date_key"].apply(lambda d: d.day)
    dim_time["dow"] = dim_time["date_key"].apply(lambda d: d.weekday()) 

    # ----------------------------
    # FACT: encounters
    # ----------------------------
    charges_agg = (
        charges.groupby("encounter_id", as_index=False)["amount"]
        .sum()
        .rename(columns={"amount": "total_charges"})
    )

    fact = encounters.merge(charges_agg, on="encounter_id", how="left")
    fact["total_charges"] = fact["total_charges"].fillna(0)

    fact["length_of_stay_days"] = (
        (fact["discharge_ts"] - fact["admit_ts"]).dt.total_seconds() / 86400
    ).round(2)

    fact = fact[
        [
            "encounter_id",
            "patient_id",
            "provider_id",
            "department_id",
            "admit_ts",
            "discharge_ts",
            "encounter_type",
            "length_of_stay_days",
            "total_charges",
        ]
    ]

    # ----------------------------
    # LOAD WAREHOUSE (preserve FKs)
    # ----------------------------
    with engine.begin() as conn:
        # Clear fact first, then dimensions (order matters with FKs)
        conn.execute(text("TRUNCATE TABLE fact_encounter;"))

        conn.execute(text("TRUNCATE TABLE dim_time CASCADE;"))
        conn.execute(text("TRUNCATE TABLE dim_provider CASCADE;"))
        conn.execute(text("TRUNCATE TABLE dim_department CASCADE;"))
        conn.execute(text("TRUNCATE TABLE dim_patient CASCADE;"))

        # Re-load dims (append keeps table + constraints)
        dim_department.to_sql("dim_department", conn, if_exists="append", index=False, method="multi")
        dim_provider.to_sql("dim_provider", conn, if_exists="append", index=False, method="multi")
        dim_patient.to_sql("dim_patient", conn, if_exists="append", index=False, method="multi")
        dim_time.to_sql("dim_time", conn, if_exists="append", index=False, method="multi")

        # Load fact last
        # Convert timestamps to dates to match schema (date_key)
        fact_out = fact.copy()
        fact_out["admit_date"] = fact_out["admit_ts"].dt.date
        fact_out["discharge_date"] = fact_out["discharge_ts"].dt.date

        fact_out = fact_out[
            [
                "encounter_id",
                "patient_id",
                "provider_id",
                "department_id",
                "admit_date",
                "discharge_date",
                "encounter_type",
                "length_of_stay_days",
                "total_charges",
            ]
        ]

        fact_out.to_sql("fact_encounter", conn, if_exists="append", index=False, method="multi")

        conn.execute(
            text(
                "UPDATE etl_run_log "
                "SET finished_at = now(), status = 'success', notes = 'warehouse built (truncate+reload)' "
                "WHERE run_id = :run_id"
            ),
            {"run_id": run_id},
        )


if __name__ == "__main__":
    main()
