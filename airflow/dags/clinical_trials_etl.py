
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

# -----------------------------
# Config (env-first defaults)
# -----------------------------
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "ct_db")
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "postgres_db")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))

DB_URI = (
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

CSV_PATH = "/opt/airflow/data/trials.csv"

API_URL = "https://clinicaltrials.gov/api/v2/studies"
HTTP_TIMEOUT_SECONDS = 30

# -----------------------------
# Helpers
# -----------------------------
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

# -----------------------------
# Tasks
# -----------------------------
def fetch_trials(**context):
    """
    Fetch studies from ClinicalTrials.gov v2, flatten, and save to CSV.
    Uses pagination via nextPageToken and a user-tunable condition query.
    """
    query_cond = context["params"].get("query_cond", "cancer")
    page_size = int(context["params"].get("page_size", 100))

    params = {
        "query.cond": query_cond,
        "pageSize": page_size,
        "format": "json",
        "countTotal": "true",
    }

    rows = []
    page = 1
    t0 = time.time()
    while True:
        logging.info("Requesting page %d with params=%s", page, params)
        r = requests.get(API_URL, params=params, timeout=HTTP_TIMEOUT_SECONDS)
        r.raise_for_status()
        payload = r.json()

        studies = payload.get("studies", []) or []
        logging.info("Page %d returned %d studies", page, len(studies))

        for s in studies:
            rows.append(_extract_row_v2(s))

        token = payload.get("nextPageToken")
        if not token:
            break
        params["pageToken"] = token
        page += 1

    df = pd.DataFrame(rows)
    # Normalize dates to YYYY-MM-DD strings (or NaT -> None)
    for col in ("start_date", "completion_date"):
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date.astype("string")

    os.makedirs(os.path.dirname(CSV_PATH), exist_ok=True)
    df.to_csv(CSV_PATH, index=False)
    logging.info(
        "Fetched %d rows, wrote CSV to %s (%.2f KB) in %.2fs",
        len(df),
        CSV_PATH,
        (os.path.getsize(CSV_PATH) / 1024) if os.path.exists(CSV_PATH) else 0,
        time.time() - t0,
    )


def load_to_postgres(**_):
    """
    Load the CSV into Postgres, with a short readiness backoff.
    """
    if not os.path.exists(CSV_PATH):
        raise FileNotFoundError(f"CSV not found: {CSV_PATH}. Did fetch_trials run?")

    t0 = time.time()
    df = pd.read_csv(CSV_PATH, dtype=str)  # keep simple and consistent types
    logging.info("Loading %d rows into Postgres table 'clinical_trials'", len(df))

    engine = create_engine(DB_URI, pool_pre_ping=True)
    last_err = None
    for attempt in range(1, 6):
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
        raise last_err

    logging.info("load_to_postgres finished in %.2fs", time.time() - t0)

# -----------------------------
# DAG
# -----------------------------
default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 10, 19),
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    dag_id="clinical_trials_etl_v5",
    schedule="@daily",
    catchup=False,
    default_args=default_args,
    tags=["clinicaltrials", "postgres", "etl"],
    params={
        "query_cond": "cancer",   # change from UI if you like
        "page_size": 100,
    },
) as dag:

    t1 = PythonOperator(
        task_id="fetch_trials",
        python_callable=fetch_trials,
        execution_timeout=timedelta(minutes=2),
        provide_context=True,  # to use params in fetch_trials
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
