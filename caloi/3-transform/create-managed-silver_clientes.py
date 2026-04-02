from config import (
    catalogo,
    schema_silver,
    table_silver
)
# --------------------------------
# 🔥 1. DROP TABLE (se existir)
# --------------------------------
spark.sql(f"""
DROP TABLE IF EXISTS {catalogo}.{schema_silver}.{table_silver}
""")

# --------------------------------
# 💾 2. Criar tabela particionada
# --------------------------------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalogo}.{schema_silver}.{table_silver} (
    id STRING,
    created_at STRING,
    first_name STRING,
    last_name STRING,
    email STRING,
    cell_phone STRING,
    country STRING,
    state STRING,
    street STRING,
    number STRING,
    additionals STRING,
    dt_foto STRING
)
USING DELTA
PARTITIONED BY (dt_foto)
""")
