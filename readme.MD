# retail-sales-dw

Pipeline de datos de ventas retail: ingesta a Snowflake, transformación con dbt
(arquitectura medallion) y orquestación con Airflow. Construido como proyecto
de portafolio para complementar [FinanzaDW](#) y [retail-demand-forecasting](#),
con foco específico en el stack Airflow + Snowflake + dbt + Python.

## Arquitectura

```
data_generator/  -->  Snowflake RAW  -->  dbt staging  -->  dbt marts  -->  (BI / ML)
  (simula POS)        (vía stage +         (limpieza,        (esquema
                        COPY INTO)          tipado)            estrella)
                            ^
                            |
                    orquestado por Airflow
                    (dags/retail_sales_dag.py)
```

- **Ingesta**: `data_generator/` simula el export diario de un POS (no depende
  de un dataset externo, así el proyecto corre sin conexión a internet más
  allá de Snowflake). `ingestion/snowflake_loader.py` sube los CSV a un stage
  interno y hace `COPY INTO` las tablas RAW.
- **Idempotencia**: las dimensiones (tiendas, productos) se cargan full-refresh
  (truncate + load). Los hechos de venta se cargan por partición de fecha
  (delete + load de esa partición), así un retry o un backfill no duplica datos.
- **Transformación**: `dbt_project/` con capa `staging` (limpieza/tipado) y
  `marts` (esquema estrella: `fct_sales`, `dim_product`, `dim_store`, `dim_date`),
  con tests de dbt (`not_null`, `unique`, `relationships`) más un test singular
  de consistencia de negocio.
- **Orquestación**: `dags/retail_sales_dag.py` (Airflow, TaskFlow API) encadena
  ingesta → dbt run → dbt test → chequeo de calidad de negocio adicional, con
  retries con backoff exponencial y un callback de alerta en caso de falla.
- **Gobernanza**: `sql/01_setup_snowflake.sql` separa roles (`LOADER_ROLE` solo
  escribe en RAW, `TRANSFORMER_ROLE` lee RAW y escribe STAGING/MARTS) siguiendo
  principio de mínimo privilegio.

## Setup en VS Code

### 1. Clonar y crear entorno virtual

```bash
git clone <tu-repo>
cd retail-sales-dw
python3 -m venv .venv
source .venv/bin/activate        # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

### 2. Configurar Snowflake

Corre `sql/01_setup_snowflake.sql` en tu cuenta de Snowflake (worksheet o
Snowflake CLI) para crear warehouse, base de datos, schemas, roles y tablas RAW.

Copia `.env.example` a `.env` y completa tus credenciales:

```bash
cp .env.example .env
```

### 3. Generar datos de prueba

```bash
python data_generator/generate_retail_data.py --days 14 --output-dir data/raw
```

### 4. Probar la carga a Snowflake manualmente

```python
from ingestion.config import SnowflakeConfig
from ingestion.snowflake_loader import SnowflakeLoader

config = SnowflakeConfig.from_env()
with SnowflakeLoader(config) as loader:
    loader.ensure_stage()
    loader.load_dimension(Path("data/raw/stores.csv"))
    loader.load_dimension(Path("data/raw/products.csv"))
    loader.load_sales_partition(Path("data/raw/sales_2026-06-19.csv"), sale_date="2026-06-19")
```

### 5. Correr dbt

```bash
cd dbt_project
cp profiles_example.yml ~/.dbt/profiles.yml   # y completa credenciales, o usa --profiles-dir .
dbt deps      # instala dbt_utils
dbt run
dbt test
```

### 6. Correr los tests unitarios

```bash
pytest tests/ -v
```

Estos tests **no requieren conexión real a Snowflake** (todo mockeado), así
que corren igual en tu máquina que en CI.

### 7. (Opcional) Levantar Airflow localmente

La forma más simple para una demo es `astro dev start` (Astronomer CLI) o
`airflow standalone` apuntando `AIRFLOW_HOME` a este proyecto con `dags/` como
carpeta de DAGs. El DAG espera los archivos generados en `/opt/airflow/data/raw`
si usas Docker — ajusta `DATA_DIR` en `dags/retail_sales_dag.py` si corres distinto.

## Tests y CI

`.github/workflows/ci.yml` corre en cada push/PR:
- `flake8` (lint) y `black --check` (formato)
- `pytest` (8 tests unitarios sobre ingesta y generación de datos)
- `dbt parse` (valida que el proyecto dbt compile sin errores de sintaxis)

## Decisiones de diseño (para discutir en entrevista)

- **Por qué truncate+load para dimensiones pero delete+load por partición
  para hechos**: las dimensiones son catálogos pequeños donde no vale la pena
  la complejidad de un MERGE; los hechos crecen indefinidamente y romperías
  el historial si truncas toda la tabla en cada corrida.
- **Por qué un test de negocio fuera de dbt** (`business_quality_check` en el
  DAG): dbt valida la *forma* del dato (nulos, unicidad, relaciones). La
  caída anómala de ventas día a día es una validación *temporal* que dbt no
  cubre bien out-of-the-box, así que vive como task de Airflow con acceso a
  ventana histórica.
- **Por qué roles separados (LOADER vs TRANSFORMER)**: si el proceso de carga
  se ve comprometido o tiene un bug, no puede tocar accidentalmente la capa
  de marts que consume el negocio.