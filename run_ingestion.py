"""
Script para correr la ingesta manualmente (sin Airflow).
Equivale a lo que haría el DAG, pero ejecutado desde consola.
"""
import sys
from datetime import date, timedelta
from pathlib import Path

from ingestion.config import SnowflakeConfig
from ingestion.snowflake_loader import SnowflakeLoader

DATA_DIR = Path("data/raw")

config = SnowflakeConfig.from_env()

with SnowflakeLoader(config) as loader:
    loader.ensure_stage()

    print("Cargando dimensiones...")
    loader.load_dimension(DATA_DIR / "stores.csv")
    loader.load_dimension(DATA_DIR / "products.csv")

    print("Cargando particiones de ventas...")
    days = int(sys.argv[1]) if len(sys.argv) > 1 else 14
    for offset in range(days):
        sale_date = date.today() - timedelta(days=offset)
        sales_file = DATA_DIR / f"sales_{sale_date.isoformat()}.csv"
        if sales_file.exists():
            loader.load_sales_partition(sales_file, sale_date=sale_date.isoformat())

print("Ingesta completada.")