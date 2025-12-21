import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()
engine = create_engine(os.getenv("DATABASE_URL"), future=True)

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS etl_run_log (
  run_id BIGSERIAL PRIMARY KEY,
  started_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  finished_at TIMESTAMPTZ,
  status TEXT NOT NULL DEFAULT 'running',
  notes TEXT
);

CREATE TABLE IF NOT EXISTS dim_department (
  department_id TEXT PRIMARY KEY, 
  department_name TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_provider (
  provider_id TEXT PRIMARY KEY,
  provider_name TEXT NOT NULL, 
  department_id TEXT REFERENCES dim_department(department_id)
);

CREATE TABLE IF NOT EXISTS dim_patient (
  patient_id TEXT PRIMARY KEY,
  birth_year INT,
  sex TEXT
);

CREATE TABLE IF NOT EXISTS dim_time (
  date_key DATE PRIMARY KEY,
  year INT NOT NULL,
  month INT NOT NULL,  
  day INT NOT NULL,
  dow INT NOT NULL
); 

CREATE TABLE IF NOT EXISTS fact_encounter (  
  encounter_id TEXT PRIMARY KEY,
  patient_id TEXT REFERENCES dim_patient(patient_id),
  provider_id TEXT REFERENCES dim_provider(provider_id), 
  department_id TEXT REFERENCES dim_department(department_id), 
  admit_date DATE REFERENCES dim_time(date_key), 
  discharge_date DATE REFERENCES dim_time(date_key), 
  encounter_type TEXT, 
  length_of_stay_days INT, 
  total_charges NUMERIC 
);
"""

with engine.begin() as conn:
    conn.execute(text(SCHEMA_SQL))

print("Schema created.") 
