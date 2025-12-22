import os
from pathlib import Path

from dotenv import load_dotenv 
from sqlalchemy import create_engine, text 


def main():
    load_dotenv()

    db_url = os.getenv("DATABASE_URL") 
    if not db_url:
        raise RuntimeError("DATABASE_URL not found in environment")
 
    engine = create_engine(db_url, future=True) 

    sql_path = Path("sql/views.sql") 
    if not sql_path.exists():
        raise FileNotFoundError(f"Missing SQL file: {sql_path}")
 
    sql = sql_path.read_text() 

    with engine.begin() as conn:
        # Execute entire SQL file 
        conn.execute(text(sql))
 
    print("Reporting views created or replaced successfully.") 


if __name__ == "__main__":
    main() 
 