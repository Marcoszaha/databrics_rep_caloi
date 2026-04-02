# ================================
# 📌 IMPORTS
# ================================
from pyspark.sql.functions import col, to_timestamp, trim

# ================================
# 📌 PARÂMETROS
# ================================
catalogo = "workspace"
schema = "default"
tabela = "clientes"

caminho_arquivo = "/Volumes/workspace/default/arquivos-aula/arquivos_csv/Clientes.csv"

# ================================
# 📌 LEITURA DO CSV
# ================================
df = (
    spark.read.format("csv")
    .option("header", True)
    .option("inferSchema", True)
    .option("sep", ",")
    .load(caminho_arquivo)
)

# ================================
# 📌 TRATAMENTO DOS DADOS
# ================================
df_tratado = (
    df
    .withColumn("id", col("id").cast("int"))
    .withColumn("created_at", to_timestamp(col("created_at"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("first_name", trim(col("first_name")))
    .withColumn("last_name", trim(col("last_name")))
    .withColumn("email", trim(col("email")))
    .withColumn("cell_phone", trim(col("cell_phone")))
    .withColumn("country", trim(col("country")))
    .withColumn("state", trim(col("state")))
    .withColumn("street", trim(col("street")))
    .withColumn("number", col("number").cast("double"))
    .withColumn("additionals", trim(col("additionals")))
)

# ================================
# 📌 REMOVER DUPLICADOS
# ================================
df_final = df_tratado.dropDuplicates(["id"])

# ================================
# 📌 CRIAR SCHEMA (se não existir)
# ================================
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalogo}.{schema}")

# ================================
# 📌 GRAVAR COMO TABELA DELTA
# ================================
df_final.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{catalogo}.{schema}.{tabela}")

# ================================
# 📌 VALIDAÇÃO
# ================================
print("Carga finalizada com sucesso!")

display(df_final)