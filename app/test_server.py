# from sqlalchemy import create_engine

# DATABASE_URL = "postgresql://postgres:postgres@localhost:5432/ct_db"
# engine = create_engine(DATABASE_URL)
# with engine.connect() as conn:
#     print("✅ Connected to PostgreSQL in Docker!")



import psycopg2

conn = psycopg2.connect(
    dbname="ct_db",
    user="postgres",
    password="postgres",
    host="172.18.0.3",
    port=5432
)
print("✅ Connected!")
conn.close()

