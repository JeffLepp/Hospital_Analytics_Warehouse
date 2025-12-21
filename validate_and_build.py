from __future__ import annotations

import os
from datetime import datetime
import pandas as pd

from dotenv import load_dotenv
from sqlalchemy import create_engine, text


def fail(msg: str):
    raise RuntimeError(f"VALIDATION FAILED: {msg}")


def main():
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
    # Load staging tables
    # ----------------------------
    patients = pd.read_sql("SELECT * FROM stg_patients", engine)
    encounters = pd.read_sql("SELECT * FROM stg_encounters", engine)
    charges = pd.read_sql("SELECT * FROM stg_charges", engine)
    labs = pd.read_sql("SELECT * FROM stg_labs", engine)
    staff = pd.read_sql("SELECT * FROM stg_staff", engine)

    # ----------------------------
    # VALIDATION - Checks currently included:
    #   
    # ----------------------------
    if patients["patient_id"].duplicated().any():
        fail("Duplicate patient_id in stg_patients")

    if encounters["encounter_id"].duplicated().any(): 
        fail("Duplicate encounter_id in stg_encounters")

    if (charges["amount"] < 0).any():
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
