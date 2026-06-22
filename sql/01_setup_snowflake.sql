-- =====================================================================
-- Setup inicial del proyecto retail-sales-dw en Snowflake
-- Correr una sola vez, como ACCOUNTADMIN o un rol con privilegios de
-- creación de objetos a nivel de cuenta.
-- =====================================================================

-- Warehouse separado para carga/transformación (permite costear y
-- escalar distinto que el warehouse de BI/consultas analíticas).
CREATE WAREHOUSE IF NOT EXISTS LOADER_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE
    INITIALLY_SUSPENDED = TRUE;

CREATE DATABASE IF NOT EXISTS RETAIL_DB;

CREATE SCHEMA IF NOT EXISTS RETAIL_DB.RAW_RETAIL;
CREATE SCHEMA IF NOT EXISTS RETAIL_DB.STAGING;
CREATE SCHEMA IF NOT EXISTS RETAIL_DB.MARTS;

-- --------------------------------------------------------------------
-- RBAC: rol dedicado para el pipeline de carga (principio de mínimo
-- privilegio: este rol solo puede escribir en RAW, no en MARTS).
-- --------------------------------------------------------------------
CREATE ROLE IF NOT EXISTS LOADER_ROLE;

GRANT USAGE ON WAREHOUSE LOADER_WH TO ROLE LOADER_ROLE;
GRANT USAGE ON DATABASE RETAIL_DB TO ROLE LOADER_ROLE;
GRANT USAGE ON SCHEMA RETAIL_DB.RAW_RETAIL TO ROLE LOADER_ROLE;
GRANT CREATE TABLE ON SCHEMA RETAIL_DB.RAW_RETAIL TO ROLE LOADER_ROLE;
GRANT CREATE STAGE ON SCHEMA RETAIL_DB.RAW_RETAIL TO ROLE LOADER_ROLE;

-- Rol para dbt: lee de RAW, escribe en STAGING/MARTS.
CREATE ROLE IF NOT EXISTS TRANSFORMER_ROLE;

GRANT USAGE ON WAREHOUSE LOADER_WH TO ROLE TRANSFORMER_ROLE;
GRANT USAGE ON DATABASE RETAIL_DB TO ROLE TRANSFORMER_ROLE;
GRANT USAGE ON SCHEMA RETAIL_DB.RAW_RETAIL TO ROLE TRANSFORMER_ROLE;
GRANT SELECT ON ALL TABLES IN SCHEMA RETAIL_DB.RAW_RETAIL TO ROLE TRANSFORMER_ROLE;
GRANT SELECT ON FUTURE TABLES IN SCHEMA RETAIL_DB.RAW_RETAIL TO ROLE TRANSFORMER_ROLE;
GRANT ALL ON SCHEMA RETAIL_DB.STAGING TO ROLE TRANSFORMER_ROLE;
GRANT ALL ON SCHEMA RETAIL_DB.MARTS TO ROLE TRANSFORMER_ROLE;

-- --------------------------------------------------------------------
-- Tablas RAW (estructura plana, tal como llega del CSV de origen)
-- --------------------------------------------------------------------
USE SCHEMA RETAIL_DB.RAW_RETAIL;

CREATE TABLE IF NOT EXISTS STORES_RAW (
    store_id    NUMBER,
    store_name  STRING,
    city        STRING,
    _loaded_at  TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS PRODUCTS_RAW (
    product_id    NUMBER,
    product_name  STRING,
    category      STRING,
    unit_price    NUMBER(12,2),
    _loaded_at    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

CREATE TABLE IF NOT EXISTS SALES_RAW (
    sale_id       STRING,
    sale_date     DATE,
    store_id      NUMBER,
    product_id    NUMBER,
    quantity      NUMBER,
    unit_price    NUMBER(12,2),
    total_amount  NUMBER(12,2),
    _loaded_at    TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

-- Stage interno usado por el loader de Python (ver ingestion/snowflake_loader.py)
CREATE STAGE IF NOT EXISTS RETAIL_STAGE
    FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1 FIELD_OPTIONALLY_ENCLOSED_BY = '"');