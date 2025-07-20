# Databricks notebook source
# MAGIC %run ./0.Variables
# MAGIC

# COMMAND ----------

# Nro 6
# Ejercicio con DF
# Leer el archivo Parquet a través de un df

df = spark.read.parquet(f"/Volumes/{catalog_name}/{schema_name_raw}/{volume_name_raw}/titanic.parquet")
# display(df)
total_rows= df.count()
print("Total rows: ", total_rows)

# Ejercicio con DF
# Leer el archivo Parquet a través de un df

df_flights = spark.read.parquet(f"/Volumes/{catalog_name}/{schema_name_raw}/{volume_name_raw}/flights.parquet")
# display(df_flights)
total_rows_flights= df_flights.count()
print("Total rows: ", total_rows_flights)

# COMMAND ----------

# Ejercicio con DF
# Filters

from pyspark.sql.functions import year

#titanic
df_male = df.filter(df.Sex == "male")
display(df_male) 
total_males = df_male.count()
print("Total males  : ", total_males)
total_females = total_rows - total_males
print("Total females: ", total_females)
print("total rows   : ", total_rows)

#flights
df_2006 = df_flights.filter(year(df_flights.FL_DATE) == 2006)
display(df_2006)
total_2006 = df_2006.count()
print("Total 2006  : ", total_2006)


# COMMAND ----------

# Guardar el parquet en una tabla del catalogo
# tabla titanic_table
# Por defecto el UNITY CATALOG guarda las tablas en DELTA. 

df.write.mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name_tables}.titanic_table")
df_flights.write.mode("overwrite").saveAsTable(f"{catalog_name}.{schema_name_tables}.flights_table")


# COMMAND ----------

# Verificar el formato como está guardado la abla
# spark.sql("DESCRIBE FORMATTED integration_catalog.raw_tables.titanic_table").show(truncate=False)
spark.sql(f"DESCRIBE TABLE EXTENDED {catalog_name}.{schema_name_tables}.titanic_table").show(n=50, truncate=False)


# COMMAND ----------

query = f"SELECT * FROM {catalog_name}.{schema_name_tables}.flights_table"
df = spark.sql(query)
display(df)

# COMMAND ----------


# 0. Cargar librerias 

from pyspark.sql.types import StructType, StructField, DoubleType,  IntegerType, BooleanType, StringType, TimestampType
from pyspark.sql.functions import col


# Crear una tabla con un formato diferente al original del archivo parquet 

# 1. Crear el esquema nuevo 

custom_schema = StructType([
    StructField("PassengerId", IntegerType(), True),
    StructField("Survived", IntegerType(), True),
    StructField("Pclass", IntegerType(), True),
    StructField("PassengerName", StringType(), True),
    StructField("Sex", StringType(), True),
    StructField("Age", DoubleType(), True),
    StructField("Sibsp", IntegerType(), True),
    StructField("Parch", IntegerType(), True),
    StructField("Ticket", StringType(), True),
    StructField("Fare", DoubleType(), True),
    StructField("Cabin", StringType(), True),
    StructField("Enmarked", StringType(), True),
])

# 2. Identificar el archivo origen en parquet
df_raw = spark.read.parquet(f"/Volumes/{catalog_name}/{schema_name_raw}/{volume_name_raw}/titanic.parquet")

# 3. Transformar columnas para que coincidan con tu custom_schema
# Origen -> Destino
df_final = df_raw.select(
    col("PassengerId").cast("int").alias("PassengerId"),
    col("Survived").cast("int").alias("Survived"),
    col("Pclass").cast("int").alias("Pclass"),
    col("Name").alias("PassengerName"),
    col("Sex").alias("Sex"),
    col("Age").cast("double").alias("Age"),
    col("SibSp").cast("int").alias("Sibsp"),
    col("Parch").cast("int").alias("Parch"),
    col("Ticket").alias("Ticket"),
    col("Fare").cast("double").alias("Fare"),
    col("Cabin").alias("Cabin"),
    col("Embarked").alias("Embarked")

)

# 4. Guardar como tabla en un esquema específico
df_final.write.mode("overwrite").format("delta").saveAsTable(f"{catalog_name}.{schema_name_tables}.titanic_table_formatted")