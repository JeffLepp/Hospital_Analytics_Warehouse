import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()
engine = create_engine(os.getenv("DATABASE_URL"), future=True)

out = Path("output/reports")
out.mkdir(parents=True, exist_ok=True)

reports = {
    "encounters_by_department_month.csv": "SELECT * FROM public.vw_encounters_by_department_month",
    "avg_los_by_encounter_type.csv": "SELECT * FROM public.vw_avg_los_by_encounter_type",
}

for fname, sql in reports.items():
    df = pd.read_sql(sql, engine)
    df.to_csv(out / fname, index=False)
    print(f"Wrote {fname} ({len(df)} rows)")
