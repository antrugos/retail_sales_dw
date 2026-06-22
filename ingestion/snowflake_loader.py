"""
Carga archivos CSV locales a Snowflake vía un stage interno + COPY INTO.

Patrón de idempotencia:
- Tablas de dimensión completas en cada corrida (stores, products): se
  truncan y recargan. Son catálogos pequeños, no vale la pena un MERGE.
- Tabla de hechos (sales): carga por partición de fecha. Antes de cargar
  un archivo `sales_<fecha>.csv`, se borra cualquier fila existente con
  esa misma `sale_date` en RAW, así un retry o un backfill no duplica
  filas.
"""

from __future__ import annotations

import functools
import logging
import time
from pathlib import Path

import snowflake.connector
from snowflake.connector import SnowflakeConnection

from ingestion.config import SnowflakeConfig

logger = logging.getLogger(__name__)

RAW_TABLES = {
    "stores.csv": "STORES_RAW",
    "products.csv": "PRODUCTS_RAW",
}


class SnowflakeLoadError(Exception):
    """Error de negocio al cargar un archivo a Snowflake."""


def with_retries(max_attempts: int = 3, backoff_seconds: float = 2.0):
    """Reintenta una operación con backoff exponencial simple.

    Pensado para errores transitorios de red/warehouse (no para errores
    de datos, esos deben fallar rápido y no reintentarse a ciegas).
    """

    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exc = None
            for attempt in range(1, max_attempts + 1):
                try:
                    return func(*args, **kwargs)
                except snowflake.connector.errors.OperationalError as exc:
                    last_exc = exc
                    wait = backoff_seconds * (2 ** (attempt - 1))
                    logger.warning(
                        "Intento %s/%s falló (%s). Reintentando en %.1fs",
                        attempt,
                        max_attempts,
                        exc,
                        wait,
                    )
                    time.sleep(wait)
            raise SnowflakeLoadError(
                f"Fallaron los {max_attempts} intentos de conexión a Snowflake"
            ) from last_exc

        return wrapper

    return decorator


class SnowflakeLoader:
    """Encapsula la conexión y las operaciones de carga a la capa RAW."""

    def __init__(self, config: SnowflakeConfig):
        self.config = config
        self._conn: SnowflakeConnection | None = None

    def __enter__(self) -> "SnowflakeLoader":
        self._conn = self._connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        if self._conn is not None:
            self._conn.close()
            self._conn = None

    @with_retries()
    def _connect(self) -> SnowflakeConnection:
        logger.info("Conectando a Snowflake (account=%s)", self.config.account)
        return snowflake.connector.connect(
            account=self.config.account,
            user=self.config.user,
            password=self.config.password,
            role=self.config.role,
            warehouse=self.config.warehouse,
            database=self.config.database,
            schema=self.config.raw_schema,
        )

    @property
    def conn(self) -> SnowflakeConnection:
        if self._conn is None:
            raise RuntimeError("Usa SnowflakeLoader dentro de un bloque 'with'")
        return self._conn

    def ensure_stage(self) -> None:
        with self.conn.cursor() as cur:
            cur.execute(
                f"CREATE STAGE IF NOT EXISTS {self.config.stage_name} "
                f"FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '\"')"
            )

    def load_dimension(self, local_path: Path) -> int:
        """Carga full-refresh para tablas de dimensión pequeñas (truncate + load)."""
        table = RAW_TABLES[local_path.name]
        with self.conn.cursor() as cur:
            cur.execute(
                f"PUT file://{local_path} @{self.config.stage_name} OVERWRITE = TRUE"
            )
            cur.execute(f"TRUNCATE TABLE IF EXISTS {table}")
            cur.execute(
                f"COPY INTO {table} FROM @{self.config.stage_name}/{local_path.name} "
                f"ON_ERROR = ABORT_STATEMENT"
            )
            rows_loaded = cur.rowcount or 0
        logger.info("Dimensión %s cargada: %s filas", table, rows_loaded)
        return rows_loaded

    def load_sales_partition(self, local_path: Path, sale_date: str) -> int:
        """Carga incremental e idempotente de una partición diaria de ventas."""
        table = "SALES_RAW"
        with self.conn.cursor() as cur:
            cur.execute(
                f"PUT file://{local_path} @{self.config.stage_name} OVERWRITE = TRUE"
            )
            # Idempotencia: borra la partición antes de recargarla, así
            # un retry o un backfill manual no duplica filas.
            cur.execute(f"DELETE FROM {table} WHERE sale_date = %s", (sale_date,))
            cur.execute(
                f"COPY INTO {table} FROM @{self.config.stage_name}/{local_path.name} "
                f"ON_ERROR = ABORT_STATEMENT"
            )
            rows_loaded = cur.rowcount or 0
        logger.info("Partición de ventas %s cargada: %s filas", sale_date, rows_loaded)
        return rows_loaded
