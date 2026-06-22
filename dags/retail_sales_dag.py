"""
DAG: retail_sales_pipeline

Orquesta el flujo diario:
  1) generar/validar que exista el archivo de ventas del día
  2) cargar dimensiones y partición de ventas a Snowflake (capa RAW)
  3) correr dbt (staging -> marts)
  4) correr tests de dbt
  5) chequeo de calidad de negocio adicional sobre la capa de marts

Diseño pensado para ser idempotente: cada task puede reintentarse o
recorrerse en un backfill sin duplicar datos (ver ingestion/snowflake_loader.py).
"""

from __future__ import annotations

from datetime import datetime, timedelta
from pathlib import Path

from airflow.decorators import dag, task
from airflow.operators.bash import BashOperator
from airflow.exceptions import AirflowFailException

DATA_DIR = Path("/opt/airflow/data/raw")
DBT_PROJECT_DIR = "/opt/airflow/dbt_project"
DBT_PROFILES_DIR = "/opt/airflow/dbt_project"

default_args = {
    "owner": "ander",
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "retry_exponential_backoff": True,
    "max_retry_delay": timedelta(minutes=15),
}


def notify_failure(context) -> None:
    """Stub de alerta. En producción esto postea a Slack/Teams/email."""
    task_id = context["task_instance"].task_id
    dag_id = context["dag"].dag_id
    execution_date = context["ts"]
    print(f"[ALERTA] {dag_id}.{task_id} falló en {execution_date}")


@dag(
    dag_id="retail_sales_pipeline",
    description="Ingesta diaria de ventas retail -> Snowflake -> dbt -> data quality",
    schedule="0 6 * * *",  # 6:00 AM todos los días
    start_date=datetime(2026, 6, 1),
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    on_failure_callback=notify_failure,
    tags=["retail", "snowflake", "dbt"],
)
def retail_sales_pipeline():

    @task
    def check_source_files(ds: str) -> dict:
        """Verifica que existan los archivos esperados para la fecha de ejecución.

        Hace las veces de 'sensor' simplificado: si el archivo no llegó,
        falla rápido en vez de dejar que el COPY INTO truene más adelante.
        """
        sales_file = DATA_DIR / f"sales_{ds}.csv"
        stores_file = DATA_DIR / "stores.csv"
        products_file = DATA_DIR / "products.csv"

        missing = [
            f for f in (sales_file, stores_file, products_file) if not f.exists()
        ]
        if missing:
            raise AirflowFailException(f"Archivos faltantes: {missing}")

        return {
            "sales_file": str(sales_file),
            "stores_file": str(stores_file),
            "products_file": str(products_file),
        }

    @task
    def load_dimensions(files: dict) -> None:
        from ingestion.config import SnowflakeConfig
        from ingestion.snowflake_loader import SnowflakeLoader

        config = SnowflakeConfig.from_env()
        with SnowflakeLoader(config) as loader:
            loader.ensure_stage()
            loader.load_dimension(Path(files["stores_file"]))
            loader.load_dimension(Path(files["products_file"]))

    @task
    def load_sales_partition(files: dict, ds: str) -> None:
        from ingestion.config import SnowflakeConfig
        from ingestion.snowflake_loader import SnowflakeLoader

        config = SnowflakeConfig.from_env()
        with SnowflakeLoader(config) as loader:
            loader.load_sales_partition(Path(files["sales_file"]), sale_date=ds)

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && " f"dbt run --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=(
            f"cd {DBT_PROJECT_DIR} && " f"dbt test --profiles-dir {DBT_PROFILES_DIR}"
        ),
    )

    @task
    def business_quality_check() -> None:
        """Chequeo de negocio extra, fuera del alcance de los tests de dbt:
        que la venta total del día no caiga más de 70% vs el promedio
        de los últimos 7 días (señal de una carga incompleta)."""
        from ingestion.config import SnowflakeConfig
        from ingestion.snowflake_loader import SnowflakeLoader

        config = SnowflakeConfig.from_env()
        query = """
            with daily as (
                select sale_date, sum(total_amount) as daily_total
                from fct_sales
                group by 1
                order by 1 desc
                limit 8
            )
            select sale_date, daily_total,
                   avg(daily_total) over (
                       order by sale_date
                       rows between 7 preceding and 1 preceding
                   ) as avg_prior_7d
            from daily
            order by sale_date desc
            limit 1
        """
        with SnowflakeLoader(config) as loader:
            with loader.conn.cursor() as cur:
                cur.execute(query)
                row = cur.fetchone()

        if row is None:
            return

        _, daily_total, avg_prior_7d = row
        if avg_prior_7d and daily_total < avg_prior_7d * 0.3:
            raise AirflowFailException(
                f"Venta del día ({daily_total}) es <30% del promedio de 7 días "
                f"({avg_prior_7d}). Posible carga incompleta, revisar antes de publicar."
            )

    files = check_source_files()
    dims = load_dimensions(files)
    sales = load_sales_partition(files, "{{ ds }}")

    [dims, sales] >> dbt_run >> dbt_test >> business_quality_check()


retail_sales_pipeline()
