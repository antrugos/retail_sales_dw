"""
Tests del SnowflakeLoader.
"""

from unittest.mock import MagicMock, patch

import pytest
import snowflake.connector

from ingestion.config import SnowflakeConfig
from ingestion.snowflake_loader import SnowflakeLoadError, SnowflakeLoader, with_retries


@pytest.fixture
def fake_config() -> SnowflakeConfig:
    return SnowflakeConfig(
        account="fake_account",
        user="fake_user",
        password="fake_pass",
        role="LOADER_ROLE",
        warehouse="LOADER_WH",
        database="RETAIL_DB",
        raw_schema="RAW_RETAIL",
        stage_name="RETAIL_STAGE",
    )


@patch("ingestion.snowflake_loader.snowflake.connector.connect")
def test_loader_connects_with_expected_params(mock_connect, fake_config):
    mock_connect.return_value = MagicMock()

    with SnowflakeLoader(fake_config) as loader:
        assert loader.conn is not None

    mock_connect.assert_called_once_with(
        account="fake_account",
        user="fake_user",
        password="fake_pass",
        role="LOADER_ROLE",
        warehouse="LOADER_WH",
        database="RETAIL_DB",
        schema="RAW_RETAIL",
    )


@patch("ingestion.snowflake_loader.snowflake.connector.connect")
def test_load_dimension_truncates_before_copy(mock_connect, fake_config, tmp_path):
    mock_cursor = MagicMock()
    mock_cursor.rowcount = 40
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_connect.return_value = mock_conn

    products_file = tmp_path / "products.csv"
    products_file.write_text("product_id,product_name\n1,Test\n")

    with SnowflakeLoader(fake_config) as loader:
        rows = loader.load_dimension(products_file)

    assert rows == 40
    executed_sql = " ".join(call.args[0] for call in mock_cursor.execute.call_args_list)
    assert "TRUNCATE TABLE" in executed_sql
    assert "COPY INTO PRODUCTS_RAW" in executed_sql
    # la verdad clave del test: TRUNCATE debe ir ANTES que COPY INTO
    assert executed_sql.index("TRUNCATE") < executed_sql.index("COPY INTO")


@patch("ingestion.snowflake_loader.snowflake.connector.connect")
def test_load_sales_partition_is_idempotent(mock_connect, fake_config, tmp_path):
    """Cargar la misma partición dos veces no debe duplicar filas:
    el DELETE de la partición debe ejecutarse antes del COPY INTO."""
    mock_cursor = MagicMock()
    mock_cursor.rowcount = 250
    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
    mock_connect.return_value = mock_conn

    sales_file = tmp_path / "sales_2026-06-19.csv"
    sales_file.write_text("sale_id,sale_date\n1,2026-06-19\n")

    with SnowflakeLoader(fake_config) as loader:
        loader.load_sales_partition(sales_file, sale_date="2026-06-19")

    calls = [call.args[0] for call in mock_cursor.execute.call_args_list]
    delete_calls = [c for c in calls if "DELETE FROM SALES_RAW" in c]
    assert len(delete_calls) == 1
    assert calls.index(delete_calls[0]) < next(
        i for i, c in enumerate(calls) if "COPY INTO SALES_RAW" in c
    )


def test_with_retries_raises_loaderror_after_exhausting_attempts(fake_config):
    @with_retries(max_attempts=2, backoff_seconds=0)
    def always_fails():
        raise snowflake.connector.errors.OperationalError("conexión perdida")

    with pytest.raises(SnowflakeLoadError):
        always_fails()
