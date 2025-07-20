# Databricks notebook source
# MAGIC %run ./0.Variables
# MAGIC

# COMMAND ----------

catalog_name        = "integration_catalog11"
schema_name_raw     = "raw_files"
volume_name_raw     = "files"
volume_name_tmp     = "tmp"
schema_name_tables  = "raw_tables"

# BRONZE
schema_name_bronze  = "bronze_layer"
table_name_bronze   = "titanic_bronze"
# SILVER
schema_name_silver  = "silver_layer"
table_name_silver   = "titanic_silver"
# GOLD
schema_name_gold  = "gold_layer"
table_name_gold   = "titanic_gold"

repo_name = "databricks_files11"

# schema 
workspace_folder       = "lab_test11"

# COMMAND ----------

# validar archivo raw de origen  
df = spark.table(f"{catalog_name}.{schema_name_tables}.titanic_table")
df.show(5)


# COMMAND ----------

import dlt
from pyspark.sql.functions import current_timestamp, col

# Ruta del archivo parquet en el volumen
parquet_path = f"/Volumes/{catalog_name}/{schema_name_raw}/{volume_name_raw}/titanic.parquet"

# Ruta del esquema completo
full_schema_path_bronze = f"{catalog_name}.{schema_name_bronze}"
full_table_name_bronze = f"{full_schema_path_bronze}.{table_name_bronze}"

# Definición de la tabla Bronze usando DLT
@dlt.table(
    name=full_table_name_bronze,
    comment="Tabla bronze con datos del Titanic cargados desde archivo Parquet"
)
def load_titanic_bronze():
    df = (
        spark.read.parquet(parquet_path)
        .withColumn("ingestion_time", current_timestamp())
        .withColumn("raw_file_name", col("_metadata.file_path"))
    )
    return df


# COMMAND ----------

# Verificar resultado 

# Definir ruta completa de la tabla
#catalog_name = "integration_catalog"
#schema_name_bronze = "bronze_layer"
#table_name_bronze = "titanic_bronze"

full_table_name = f"{catalog_name}.{schema_name_bronze}.{table_name_bronze}"

# Leer la tabla como DataFrame
df = spark.read.table(full_table_name)
df.show()