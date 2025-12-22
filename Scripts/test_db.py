import os
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

load_dotenv()
db_url = os.getenv("DATABASE_URL") 
 
if not db_url:
    raise RuntimeError("DATABASE_URL not set") 

engine = create_engine(db_url, future=True)  
 
with engine.connect() as conn:
    result = conn.execute(text("SELECT current_database(), current_user, now();")).first()  
    print("Connected:", result)
