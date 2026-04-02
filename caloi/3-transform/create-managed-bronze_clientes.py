
from config import (
    catalogo,
    schema_bronze,
    table_bronze
)
# --------------------------------
# 🔥 1. DROP TABLE (se existir)
# --------------------------------
spark.sql(f"""
DROP TABLE IF EXISTS {catalogo}.{schema_bronze}.{table_bronze}
""")

# --------------------------------
# 💾 2. Criar tabela particionada
# --------------------------------

spark.sql(f"""
CREATE TABLE IF NOT EXISTS {catalogo}.{schema_bronze}.{table_bronze} (
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
""")