from __future__ import annotations

import os
from pathlib import Path

import pandas as pd 
from dotenv import load_dotenv
from sqlalchemy import create_engine, text


RAW_DIR = Path("data/raw")

FILES = { 
    "stg_patients": "patients.csv",
    "stg_encounters": "encounters.csv",
    "stg_charges": "charges.csv",
    "stg_labs": "labs.csv",
    "stg_staff": "staff.csv", 
}


def require_file(path: Path) -> None:
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {path}. Run generate_raw_data.py first.")


def main() -> None:
    load_dotenv()
    db_url = os.getenv("DATABASE_URL")
    if not db_url:
        raise RuntimeError("DATABASE_URL not found. Ensure you have a .env file with DATABASE_URL=...")

    engine = create_engine(db_url, future=True)
 
    # Basic guardrails
    for fname in FILES.values(): 
        require_file(RAW_DIR / fname)

    with engine.begin() as conn:
        # Ensure staging schema exists (we'll use public schema but keep names stg_*) 
        conn.execute(
            text(
                """
                CREATE TABLE IF NOT EXISTS etl_run_log ( 
                  run_id BIGSERIAL PRIMARY KEY,
                  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
                  finished_at TIMESTAMPTZ,
                  status TEXT NOT NULL DEFAULT 'running', 
                  notes TEXT
                ); 
                """
            )
        ) 

        run_id = conn.execute(
            text("INSERT INTO etl_run_log(status, notes) VALUES ('running', 'load staging') RETURNING run_id;")
        ).scalar_one() 

        # Drop staging tables if they exist (safe re-run)
        for table in FILES.keys(): 
            conn.execute(text(f"DROP TABLE IF EXISTS {table};"))

    # Load each CSV into Postgres as staging table 
    total_rows = 0 
    for table, fname in FILES.items():
        df = pd.read_csv(RAW_DIR / fname)
  
        # Light cleanup: standardize column names to lowercase
        df.columns = [c.strip().lower() for c in df.columns]

        # Write to Postgres
        df.to_sql(table, engine, if_exists="replace", index=False, method="multi", chunksize=5000)
        print(f"Loaded {table}: {len(df):,} rows") 
        total_rows += len(df)

    with engine.begin() as conn:
        conn.execute( 
            text(
                "UPDATE etl_run_log SET finished_at = now(), status = 'success', notes = :notes WHERE run_id = :run_id"
            ),
            {"run_id": run_id, "notes": f"loaded staging tables, rows={total_rows}"},
        ) 

    print(f"Done. Total rows loaded into staging: {total_rows:,}")


if __name__ == "__main__":
    main()
