Infrastructure and data creation -> slow start 12/19
In progress - additional features and documentation yet to be made 12/20

# Hospital Data Warehouse (Docker + Postgres + Python ETL)

End-to-end data engineering mini-project. Simulates hospital data sources, 
loads them into a staging layer, validates data quality, and builds an 
analytics-ready warehouse schema (dimensions + fact table). Includes 
reporting views and CSV exports.

## Tech stack
- Python (pandas, SQLAlchemy)
- Postgres (Docker)
- pgAdmin (optional UI)

## Project layout
Hospital_Analytics_Warehouse/
├─ data/
│  └─ raw/ 
├─ output/ 
│  ├─ reports/
│  └─ logs/
├─ venv/ 
├─ .env
├─ docker-compose.yml       
├─ requirements.txt
├─ README.md
├─ create_schema.py      
├─ generate_raw_data.py 
├─ load_staging.py 
├─ validate_and_build_warehouse.py
├─ export_reports.py
└─ test_db.py   

## Prerequisites
- Docker Desktop
- Python 3.11+ (venv recommended)

## Setup


## Detailed Description and context

Infrastructure behind project is Postgres, which runs in a docker container. 
This docker container can be started with docker-compose.yml which can be 
done with the command "docker compose up -d". Postgress will listen on the 
machine at localhost:5432/. Its convenient to have a .env file to store the 
connection string to the database, which will be loaded at runtime. 

Specific libraries and dedicated purpose in this project are as follows:
    - pandas            for data manipulation
    - SQLAlchemy        for DB connections
    - psycopg2-binary   the Postgres driver
    - python-dotenv     load secrets/config from .env

For conistency and downstream work, each table made with create.schema.py has rigid format:
    - dim_patient       patient_id, birth_year, sex
    - dim_provider      provider_id, provider_name, department_id 
    - dim_department    department_id, department_name 
    - dim_time          date_key, year, month, day, dow 
    - fact_encounter    encounter_id, patient_id, provider_id, department_id, admit_date, 
                        discharge_date, encounter_type, length_of_stay_days, total_charges 
    - etl_run_log       run_id, started_at, finished_at, status, notes

Each table made needs a source of data to populate it with. A common file form for 
data extracts from lab, billing, or HR are .csv files. Using generate_raw_data.py, 
multiple csv files are populated with realistic and relational data matched with 
patient ID rather than any personalized identifiers (we respect privacy!)

To begin transferring raw data ectracts into a queriable postgres database, we
need to load the raw data into a pandas dataframe, which then goes into staging
tables prefixed with "stg_". These are necessary for controlled data flow. 

From here, it is important to run the raw data through some quality checks.
Validate_and_build.py checks the data for patient_id and encounter_id duplicates,
negative charges, weird discharge periods, and referencial intergrity (such as charges
referencing a logged encounter). The program calls fail() upon data quality fail.

Following quality checks, validate_and_build then builds data dimensions and fact table.
The dimensions are a list of entities involved with the data, in this case being patients,
providers, departments, and time. The fact table is organized such that each row is 
an encounter. Includes calculations such as length_of_stay_days and total_charges. 
Each dimension is made into it's own table prefixed with "dim_". The fact table is made
into fact_encounter. 

This dimension and fact table (or dim/fact) is a classic analytical model where the
dimensions describe who/what/where/when while the fact table holds each measurable event. 
This allows for easier reporting and use.