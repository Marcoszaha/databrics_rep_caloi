# ================================
# 🥈 SILVER - CLEAN
# ================================

from pyspark.sql.functions import col, to_timestamp, trim

from config import (
    catalogo,
    schema_bronze,
    schema_silver,
    table_bronze,
    table_silver
)


df_silver = (
    spark.table(f"{catalogo}.{schema_bronze}.{table_bronze}")
    .withColumn("id", trim(col("id")))
    .withColumn("created_at", trim(col("created_at")))
    .withColumn("first_name", trim(col("first_name")))
    .withColumn("last_name", trim(col("last_name")))
    .withColumn("email", trim(col("email")))
    .withColumn("cell_phone", trim(col("cell_phone")))
    .withColumn("country", trim(col("country")))
    .withColumn("state", trim(col("state")))
    .withColumn("street", trim(col("street")))
    .withColumn("number", trim(col("number")))
    .withColumn("additionals", trim(col("additionals")))
    .withColumn("dt_foto", trim(col("dt_foto")))
)

# Remover duplicados
df_silver = df_silver.dropDuplicates(["id"])


# Exemplo: deletar partição antes
df_silver.createOrReplaceTempView("temp_view")
spark.sql(f"""
DELETE FROM {catalogo}.{schema_silver}.{table_silver}
WHERE dt_foto IN (SELECT DISTINCT dt_foto FROM temp_view)
""")

# Escrita particionada por dt_foto
df_silver.write \
    .format("delta") \
    .mode("append") \
    .partitionBy("dt_foto") \
    .saveAsTable(f"{catalogo}.{schema_silver}.{table_silver}")



# Remover duplicados
#df_silver = df_silver.dropDuplicates(["id"])

# Salvar silver
#df_silver.write \
#    .format("delta") \
#    .mode("overwrite") \
#    .saveAsTable(f"{catalogo}.{schema_silver}.{table_silver}")