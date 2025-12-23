import os
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv() 
engine = create_engine(os.getenv("DATABASE_URL"), future=True)

for t in ["stg_fhir_patient", "stg_fhir_encounter", "stg_fhir_observation", "stg_fhir_chargeitem"]:
    df = pd.read_sql(f"SELECT * FROM {t} LIMIT 20", engine) 
    print("\n==", t, "==")
    print(df)
