
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # #  # 
# docker exec -it airflow_webserver airflow dags reserialize #
# # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # # 
# clinical_trials_etl_v3.py

from __future__ import annotations

import logging
import os
import time
from datetime import datetime, timedelta

import pandas as pd
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine
from sqlalchemy.exc import OperationalError

# ---------------------------------------------------------------------
# Config (env-first, with sensible defaults for your docker-compose)
# ---------------------------------------------------------------------
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "ct_db")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres_db")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))

DB_URI = (
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# Write data to the shared data volume you mounted:
#   - ./airflow/data:/opt/airflow/data
CSV_PATH = "/opt/airflow/data/trials.csv"

API_URL = "https://clinicaltrials.gov/api/v2/studies"
API_PARAMS = {"query.cond": "cancer", "pageSize": 100, "format": "json"}
HTTP_TIMEOUT_SECONDS = 30


def _extract_row_v2(study: dict) -> dict:
    """Flatten the v2 study structure into a simple row."""
    ps = study.get("protocolSection", {}) or {}

    ident = ps.get("identificationModule", {}) or {}
    conds = (ps.get("conditionsModule", {}) or {}).get("conditions", []) or []
    arms = (ps.get("armsInterventionsModule", {}) or {}).get("interventions", []) or []
    design = ps.get("designModule", {}) or {}
    status = ps.get("statusModule", {}) or {}
    sponsor_mod = (ps.get("sponsorCollaboratorsModule", {}) or {})
    lead_sponsor = (sponsor_mod.get("leadSponsor", {}) or {}).get("name")

    start_date = (status.get("startDateStruct") or {}).get("date")
    completion_date = (status.get("completionDateStruct") or {}).get("date")

    return {
        "trial_id": ident.get("nctId"),
        "title": ident.get("briefTitle") or ident.get("officialTitle"),
        "condition": ", ".join(conds),
        "interventions": ", ".join([a.get("name") for a in arms if a.get("name")]),
        "phase": ", ".join(design.get("phases", []) or []) or None,
        "overall_status": status.get("overallStatus"),
        "lead_sponsor": lead_sponsor,
        "start_date": start_date,
        "completion_date": completion_date,
    }
# ---------------------------------------------------------------------
# Task callables
# ---------------------------------------------------------------------
def fetch_trials(**_):
    """
    Fetch a small sample of trials from ClinicalTrials v2 API and save to CSV.
    Adds a hard HTTP timeout and logs the shape of the payload for visibility.
    """
    t0 = time.time()
    logging.info("Fetching trials from %s with params=%s", API_URL, API_PARAMS)
    r = requests.get(API_URL, params=API_PARAMS, timeout=HTTP_TIMEOUT_SECONDS)
    r.raise_for_status()
    payload = r.json()
    print('---------------------------->payload\n')
    print(payload)

    studies = payload.get("studies", []) or []
    logging.info("Received %d studies. Top-level keys: %s", len(studies), list(payload.keys()))
    print('--------------------------> studies\n')
    print(studies)

    rows = []
    for s in studies:
        # Defensive extraction (v2 payloads can vary). Use .get with safe fallbacks.
        rows.append(_extract_row_v2(s))
        # rows.append(
        #     {
        #         "trial_id": s.get("nctId"),
        #         "title": s.get("briefTitle"),
        #         "condition": ", ".join(s.get("conditions", []) or []),
        #         "interventions": ", ".join(s.get("interventions", []) or []),
        #         "phase": s.get("phase"),
        #         "overall_status": s.get("overallStatus"),
        #         "lead_sponsor": (s.get("leadSponsor") or {}).get("agency"),
        #         "start_date": s.get("startDate"),
        #         "completion_date": s.get("completionDate"),
        #     }
        # )

    df = pd.DataFrame(rows)
    print(df)
    logging.info("Built dataframe with %d rows and %d columns", *df.shape)
    os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)
    df.to_csv(CSV_PATH, index=False)
    logging.info("Wrote CSV to %s (%.2f KB) in %.2fs", CSV_PATH, os.path.getsize(CSV_PATH) / 1024, time.time() - t0)


def load_to_postgres(**_):
    """
    Load the CSV into Postgres with a small readiness backoff and connection ping.
    """
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"CSV not found: {CSV_PATH}. Did fetch_trials run?")

    t0 = time.time()
    df = pd.read_csv(CSV_PATH)
    logging.info("Loading %d rows into Postgres table 'clinical_trials'", len(df))

    engine = create_engine(DB_URI, pool_pre_ping=True)

    # Small readiness/backoff loop for the DB (covers race conditions on startup)
    last_err = None
    for attempt in range(1, 6):  # up to ~10 seconds total
        try:
            df.to_sql(
                "clinical_trials",
                engine,
                if_exists="replace",
                index=False,
                method="multi",
                chunksize=1000,
            )
            logging.info("Loaded table 'clinical_trials' successfully.")
            break
        except OperationalError as e:
            last_err = e
            logging.warning("DB not ready (attempt %d/5): %s", attempt, e)
            time.sleep(2)
    else:
        # Only reached if all attempts failed
        raise last_err

    logging.info("load_to_postgres finished in %.2fs", time.time() - t0)


# ---------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 10, 19),
    # Keep retries short to avoid long gaps while iterating
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="clinical_trials_etl_v4",
    schedule="@daily",          # Airflow 2.7+ prefers `schedule` over `schedule_interval`
    catchup=False,
    default_args=default_args,
    tags=["clinicaltrials", "postgres", "etl"],
) as dag:

    t1 = PythonOperator(
        task_id="fetch_trials",
        python_callable=fetch_trials,
        execution_timeout=timedelta(minutes=2),
    )

    t2 = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres,
        execution_timeout=timedelta(minutes=2),
    )

    t1 >> t2



# from datetime import datetime

# from airflow import DAG
# from airflow.decorators import task
# from airflow.operators.bash import BashOperator

# # A DAG represents a workflow, a collection of tasks
# with DAG(dag_id="demo", start_date=datetime(2022, 1, 1), schedule="0 0 * * *") as dag:

#     # Tasks are represented as operators
#     hello = BashOperator(task_id="hello", bash_command="echo hello")

#     @task()
#     def airflow():
#         print("airflow")

#     # Set dependencies between tasks
#     hello >> airflow()


# from airflow import DAG
# from airflow.operators.bash import BashOperator
# from datetime import datetime

# with DAG("test_dag", start_date=datetime(2025, 10, 19), schedule_interval="@once", catchup=False) as dag:
#     t1 = BashOperator(task_id="hello", bash_command="echo hello")
