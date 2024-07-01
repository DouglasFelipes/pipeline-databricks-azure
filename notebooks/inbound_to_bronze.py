# Databricks notebook source
# MAGIC %md
# MAGIC **Conferindo se os dados foram montados e se temos acesso a pasta de inbound**

# COMMAND ----------

dbutils.fs.ls("/mnt/dados/inbound")

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Lendo os dados na camada de inbound**

# COMMAND ----------

path = "dbfs:/mnt/dados/inbound/dados_brutos_imoveis.json"
dados = spark.read.json(path)

# COMMAND ----------

display(dados)

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Removendo colunas**

# COMMAND ----------

# Supondo que 'dados' seja um DataFrame do Spark já carregado
dados_anuncio = dados.drop("imagens", "usuario")

# Exibir o DataFrame resultante
display(dados_anuncio)


# COMMAND ----------

# MAGIC %md
# MAGIC ## **Criando uma coluna de identificação**

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df_bronze = dados_anuncio.withColumn("id", col("anuncio.id"))
display(df_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC ## **Salvando na camada bronze**

# COMMAND ----------

from pyspark.sql import SparkSession

# Inicializar SparkSession (se ainda não estiver inicializado)
spark = SparkSession.builder.appName("Databricks Delta Example").getOrCreate()

# Supondo que 'df_bronze' seja o DataFrame que você deseja salvar
path = "dbfs:/mnt/dados/bronze/dataset_imoveis"
df_bronze.write.format("delta").mode("overwrite").save(path)

