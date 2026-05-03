import os
from sqlalchemy import create_engine, text

host = os.getenv("DB_HOST", "localhost")
port = os.getenv("DB_PORT", "5432")
user = os.getenv("DB_USER", "postgres")
password = os.getenv("DB_PASSWORD", "postgres")
db = os.getenv("DB_NAME", "postgres")

print(f"Connecting to {host}:{port}/{db} as {user}...")

try:
    engine = create_engine(
        f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}",
        connect_args={"sslmode": "verify-full", "sslrootcert": "./global-bundle.pem"},
    )
    with engine.connect() as conn:
        result = conn.execute(text("SELECT version()"))
        print("Connection successful!")
        print(result.fetchone()[0])
except Exception as e:
    print(f"Connection failed: {e}")
