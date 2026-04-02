# ================================
# 🥇 GOLD - ANALYTICS
# ================================

from pyspark.sql.functions import count

from config import (
    catalogo,
    schema_silver,
    schema_gold,
    table_silver,
    table_gold    
)

df_gold = (
    spark.table(f"{catalogo}.{schema_silver}.{table_silver}")
    .groupBy("state")
    .agg(count("*").alias("qtd_clientes"))
)

# Salvar gold
df_gold.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{catalogo}.{schema_gold}.{table_gold}")