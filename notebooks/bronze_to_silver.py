# Databricks notebook source
# MAGIC %md
# MAGIC **Conferindo se os dados foram montados e se temos acesso a pasta bronze**

# COMMAND ----------

dbutils.fs.ls("/mnt/dados/bronze")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Lendo os dados na camada bronze

# COMMAND ----------

path = "dbfs:/mnt/dados/bronze/dataset_imoveis/"
df = spark.read.format("delta").load(path)

# COMMAND ----------

display(df)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Transformando os campos do json em colunas

# COMMAND ----------

display(df.select("anuncio.*"))

# COMMAND ----------

display(
    df.select("anuncio.*","anuncio.endereco.*")
    )

# COMMAND ----------

dados_detalhados = df.select("anuncio.*","anuncio.endereco.*")

# COMMAND ----------

display(dados_detalhados)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Removendo colunas

# COMMAND ----------

df_silver = dados_detalhados.drop("caracteristicas","endereco")
display(df_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Salvando na camada silver

# COMMAND ----------

path = "dbfs:/mnt/dados/silver/dataset_imoveis"
df_silver.write.format("delta").mode("overwrite").save(path)
