# ================================
# 🥉 BRONZE - RAW
# ================================
from pyspark.sql.functions import current_timestamp, date_format, col, lit
from config import (
    catalogo,
    schema_bronze,
    table_bronze,
    caminho_arquivo
)

# 👇 variável com a data formatada (string)
#dt_foto = date_format(current_timestamp(), "yyyy-MM-dd")
dt_foto = lit("2026-04-03")


# --------------------------------
# 📥 2. Leitura do CSV
# --------------------------------
df_raw = (
    spark.read.format("csv")
    .option("header", True)
    .option("inferSchema", False)  # 👈 importante
    .load(caminho_arquivo)
)

# --------------------------------
# 🧱 3. Seleção + cast explícito
# --------------------------------
df_bronze = (
    df_raw
    .select(
        col("id").cast("string").alias("id"),
        col("created_at").cast("string").alias("created_at"),
        col("first_name").cast("string").alias("first_name"),
        col("last_name").cast("string").alias("last_name"),
        col("email").cast("string").alias("email"),
        col("cell_phone").cast("string").alias("cell_phone"),
        col("country").cast("string").alias("country"),
        col("state").cast("string").alias("state"),
        col("street").cast("string").alias("street"),
        col("number").cast("string").alias("number"),
        col("additionals").cast("string").alias("additionals")
    )
    .withColumn("dt_foto", dt_foto)
)

# --------------------------------
# 💾 4. Overwrite da tabela
# --------------------------------
df_bronze.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(f"{catalogo}.{schema_bronze}.{table_bronze}")