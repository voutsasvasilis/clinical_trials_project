# from typing import Union
# from fastapi import FastAPI
# from pydantic import BaseModel
# from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean
# from sqlalchemy.ext.declarative import declarative_base
# from sqlalchemy.orm import sessionmaker



# SQLALCHEMY_DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/fastapi_db"
# engine = create_engine(SQLALCHEMY_DATABASE_URL)
# SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
# Base = declarative_base()




# test = FastAPI()


# class Item(BaseModel):
#     name: str
#     price: float
#     is_offer: Union[bool, None] = None
#     dummy_item: str


# @test.get("/")
# def read_root():
#     return {"Hello": "World"}


# items = {} 







 
# @test.get("/items/{item_id}")
# def read_item(item_id: int):
#     if item_id in items:
#         return items[item_id]
#     return {"error": "Item not found"}



# @test.put("/items/{item_id}")
# def update_item(item_id: int, item: Item):
#     items[item_id] = item
#     return {"item_id": item_id, "item_name": item.name}



# from typing import Union
# from fastapi import FastAPI
# from pydantic import BaseModel
# from sqlalchemy import create_engine, Column, Integer, String, Float, Boolean
# from sqlalchemy.ext.declarative import declarative_base
# from sqlalchemy.orm import sessionmaker
# import os
# POSTGRES_USER = 'postgres'
# POSTGRES_PASSWORD = 'postgres'
# POSTGRES_DB ='ct_db'
# # uvicorn app:app --reload --host 0.0.0.0 --port 9000
# # # fastapi dev app.py  run the server

# # FastAPI app
# app = FastAPI(title="Clinical Trials API")


# @app.get("/")
# def read_root():
#     return {"Clinical Trials API": "Started"}

# # PostgreSQL connection
# DATABASE_URL = "postgresql+psycopg2://" + POSTGRES_USER + ":" + POSTGRES_PASSWORD + "@127.0.0.1:5432/" + POSTGRES_DB
# engine = create_engine(DATABASE_URL)
# @app.get("/trials")
# def get_trials(phase: str = None, condition: str = None):
#     query = "SELECT * FROM clinical_trials WHERE 1=1"
#     if phase:
#         query += f" AND phase = '{phase}'"
#     if condition:
#         query += f" AND condition = '{condition}'"
#     with engine.connect() as conn:
#         results = [dict(row) for row in conn.execute(text(query))]
#     return results

# @app.get("/trial/{trial_id}")
# def get_trial(trial_id: str):
#     with engine.connect() as conn:
#         result = conn.execute(text("SELECT * FROM clinical_trials WHERE trial_id=:tid"), {'tid': trial_id}).fetchone()
#         return dict(result) if result else {"error":"Trial not found"}

# @app.get("/stats/condition")
# def stats_condition():
#     with engine.connect() as conn:
#         result = conn.execute(text("SELECT condition, COUNT(*) as count FROM clinical_trials GROUP BY condition")).fetchall()
#         return [dict(row) for row in result]



from typing import Optional, List, Dict, Any
from fastapi import FastAPI, Query, HTTPException
from pydantic import BaseModel
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import os
import logging

# -------- logging (see server console for stack traces) --------
logging.basicConfig(level=logging.INFO)
log = logging.getLogger("ct_api")

# -------- config (env-first) --------
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")
POSTGRES_DB = os.getenv("POSTGRES_DB", "ct_db")
# IMPORTANT:
# - If FastAPI runs on your HOST machine: keep 127.0.0.1
# - If FastAPI runs IN docker-compose: use service name 'postgres_db'
POSTGRES_HOST = os.getenv("POSTGRES_HOST", "127.0.0.1")
POSTGRES_PORT = int(os.getenv("POSTGRES_PORT", "5432"))

DATABASE_URL = (
    f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}"
    f"@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
)

# pool_pre_ping handles stale connections nicely
engine = create_engine(DATABASE_URL, pool_pre_ping=True)

app = FastAPI(title="Clinical Trials API")

@app.get("/")
def read_root():
    return {"Clinical Trials API": "Started", "db_host": POSTGRES_HOST}

@app.get("/health/db")
def health_db():
    # simple connectivity check
    try:
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        return {"db": "ok"}
    except Exception as e:
        log.exception("DB healthcheck failed")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/trials")
def get_trials(
    phase: Optional[str] = Query(None, description="Exact phase (e.g. 'Phase 2')"),
    condition: Optional[str] = Query(None, description="Substring match within condition"),
) -> List[Dict[str, Any]]:
    try:
        sql = """
            SELECT *
            FROM clinical_trials
            WHERE 1=1
            {phase_clause}
            {cond_clause}
            ORDER BY trial_id
            LIMIT 1000
        """
        params = {}
        phase_clause = ""
        cond_clause = ""

        if phase:
            phase_clause = " AND phase = :phase"
            params["phase"] = phase

        if condition:
            # your 'condition' column is comma-joined; substring match works better
            cond_clause = " AND condition ILIKE :cond"
            params["cond"] = f"%{condition}%"

        sql = sql.format(phase_clause=phase_clause, cond_clause=cond_clause)

        with engine.connect() as conn:
            rows = conn.execute(text(sql), params).fetchall()
            return [dict(r._mapping) for r in rows]
    except SQLAlchemyError as e:
        log.exception("SQLAlchemy error in /trials")
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        log.exception("Unhandled error in /trials")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/trial/{trial_id}")
def get_trial(trial_id: str) -> Dict[str, Any]:
    try:
        with engine.connect() as conn:
            row = conn.execute(
                text("SELECT * FROM clinical_trials WHERE trial_id = :tid"),
                {"tid": trial_id},
            ).fetchone()
        if not row:
            raise HTTPException(status_code=404, detail="Trial not found")
        return dict(row._mapping)
    except SQLAlchemyError as e:
        log.exception("SQLAlchemy error in /trial")
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        log.exception("Unhandled error in /trial")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/stats/condition")
def stats_condition() -> List[Dict[str, Any]]:
    """
    NOTE: This groups by the entire comma-joined 'condition' string.
    For true per-condition stats, you should normalize into a child table.
    """
    try:
        with engine.connect() as conn:
            rows = conn.execute(text("""
                SELECT condition, COUNT(*) AS count
                FROM clinical_trials
                GROUP BY condition
                ORDER BY count DESC NULLS LAST
            """)).fetchall()
        return [dict(r._mapping) for r in rows]
    except SQLAlchemyError as e:
        log.exception("SQLAlchemy error in /stats/condition")
        raise HTTPException(status_code=500, detail=str(e))
    except Exception as e:
        log.exception("Unhandled error in /stats/condition")
        raise HTTPException(status_code=500, detail=str(e))
