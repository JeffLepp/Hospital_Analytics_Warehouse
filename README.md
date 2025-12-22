# Hospital Analytics Data Warehouse (Docker + Postgres + Python ETL)
End-to-end data engineering project. Simulates hospital data sources, 
loads them into a staging layer, validates data quality, and builds an 
analytics-ready warehouse schema (dimensions + fact table). Includes 
reporting views and CSV exports. More detailed descriptions below.

## Tech stack
- Python (pandas, SQLAlchemy)
- Postgres (Docker)
- pgAdmin (optional UI)

## Prerequisites
- Docker Desktop
- Python 3.11+ (venv recommended)

## Setup
Once Downloaded, open docker desktop and run:
```
docker compose up -d
```
And ensure container is running with:
```
docker ps
```
Create virtual environment and install requirements:
```
py -m venv venv
.\venv\Scripts\activate
pip install --upgrade pip

pip install -r requirements.txt
```
Create a .env file and place within it:
```
DATABASE_URL=postgresql+psycopg2://prh:prh@localhost:5432/prh_dw
```
(Optional) Verify database connectivity before executing the pipeline:
```
py Scripts/test_db.py
```
Run each line below in the following order:
```
py create_schema.py
py generate_raw_data.py
py load_staging.py
py validate_and_build.py
py create_views.py
py export_reports.py
```

## Reporting views

Reporting views are defined as SQL in `sql/views.sql` and created 
automatically using the `create_views.py` script. 

These views provide an analytics-ready semantic layer so downstream 
users do not query raw fact tables directly. A few examples include: 

- **vw_avg_los_by_encounter_type**  
  Average length of stay and encounter counts grouped by encounter type.

- **vw_encounters_by_department_month**  
  Monthly encounter counts and total charges by department.
    
Views are defined in `sql/views.sql` and created automatically by running
`create_views.py`, ensuring reproducible setup.

To create or refresh views:
    ```
    py create_views.py
    ```

## Detailed walkthrough in the sections below

The pipeline executes in the following order:

create_schema.py
 -> generate_raw_data.py
 -> load_staging.py
 -> validate_and_build_warehouse.py
 -> create_views.py  
 -> export_reports.py

Underlying database of the project is PostgreSQL, which runs inside a docker 
container. Docker allows for reproducible environment (no manual PostgreSQL 
installation). The container is defined in docker-compose.yml.

PostgreSQL will listen on localhost:5432/ and connection details are stored 
in a .env file using a SQLAlchemy connection string. This is loaded at run- 
time using python-dotenv. This is convenient because credentials and 
environment specific settings are then seperated from the application itself. 

## Core libraries and their purpose
    
- **pandas**  
  Used for data ingestion, transformation, aggregation, and validation logic.

- **SQLAlchemy**  
  Provides database connectivity and transaction handling between Python and PostgreSQL.

- **psycopg2-binary**  
  PostgreSQL database driver used by SQLAlchemy.

- **python-dotenv**  
  Loads environment variables (such as the database connection string) from a `.env` file.
    
## Warehouse schema

The warehouse schema is created in create_schema.py which makes a fixed table 
for each dimension, fact table, and run log. This schema follows a dimensional 
(star) model, where dimensions describe entities involved in the data (who, 
what, where, when) and the fact table records measurable events (encounters). 
Each are described in more detail below.

### Dimension tables

- **`dim_patient`**  
  `patient_id`, `birth_year`, `sex`

- **`dim_provider`**  
  `provider_id`, `provider_name`, `department_id`

- **`dim_department`**  
  `department_id`, `department_name`

- **`dim_time`**  
  `date_key`, `year`, `month`, `day`, `dow`

---

### Fact table

- **`fact_encounter`**  
  `encounter_id`, `patient_id`, `provider_id`, `department_id`,  
  `admit_date`, `discharge_date`, `encounter_type`,  
  `length_of_stay_days`, `total_charges`

---

### Operational metadata

- **`etl_run_log`**  
  `run_id`, `started_at`, `finished_at`, `status`, `notes`
----------------------------------------------------------------------------- 

Each warehouse table requires a source of data; because real hospital data 
cannot be used, this project generates realistic synthetic datasets. Using 
`generate_raw_data.py`, realistic CSV data from sources such as electronic 
health records, billing systems, and HR systems. All records between CSV's 
are linked with synthetic identifiers like patient ID or encounter ID.

To begin organizing raw data extracts into a database, we load and store the 
data into staging tables prefixed with "stg_". These are necessary for 
controlled data flow as they act as a buffer for validation and inspection 
of the raw data. 

## Data quality checks

The script `validate_and_build.py` enforces data quality checks before
any warehouse tables are modified. These checks include:

- **Duplicate key detection**
  - duplicate `patient_id` values
  - duplicate `encounter_id` values

- **Financial validity**
  - negative charge amounts are rejected

- **Temporal consistency**
  - discharge timestamps occurring before admission timestamps

- **Referential integrity**
  - charges referencing missing encounters
  - labs referencing missing encounters
    

If any validation fails, the pipeline raises an error and aborts immediately.  
Because warehouse loads are wrapped in database transactions, no partial or 
invalid  data is written. The last known-good warehouse state is preserved, 
etl_run_log allows for failure states to be audited.

Following validation, validate_and_build.py constructs the warehouse layer.
This layer consists of the data's dimensions and fact table. The dimensions are 
unique entities derived from staging data and in this case are patients,
providers, departments, and time. The fact table contains one row per encounter. 
It derives metrics such as length_of_stay_days and total_charges. Each dimension 
is made into it's own table prefixed with "dim_". The fact table is made into 
fact_encounter. 

The warehouse uses a truncate-and-reload strategy for repeatable runs while 
preserving foreign key constraints, ensuring consistency without destructive 
table replacement.

All pipeline steps are idempotent and can be safely re-run without manual cleanup.

## Project layout
```
Hospital_Analytics_Warehouse/
├─ data/
│  └─ raw/ 
├─ output/ 
│  ├─ reports/
│  └─ logs/
├─ Scripts/
│  └─ test_db.py  
├─ sql/
│  └─ views.sql
├─ venv/ 
├─ .env (user-created; not committed)
├─ .gitignore
├─ docker-compose.yml       
├─ requirements.txt
├─ README.md
├─ create_schema.py    
├─ create_views.py  
├─ generate_raw_data.py 
├─ load_staging.py 
├─ validate_and_build.py
└─ export_reports.py
```

