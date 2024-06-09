import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, monotonically_increasing_id, explode, array, floor, col, rand, to_date, date_add
from datetime import datetime

from pyspark.sql.types import DoubleType, IntegerType, LongType

spark = SparkSession.builder.appName("Generated Data COVID").getOrCreate()

df = spark.read.json('data/data_covid.json')

df_with_id = df.withColumn("temp_id", monotonically_increasing_id())
ids = array([(lit(i)) for i in range(1, 7)])
tabla_duplicada = df_with_id.withColumn("id", explode(ids))

columnas = ["id"] + [c for c in df_with_id.columns if c != "temp_id"]
tabla_duplicada = tabla_duplicada.select(columnas)

for col_name in df.columns:
    if isinstance(df.schema[col_name].dataType, (DoubleType, IntegerType, LongType)) and col_name != "date":
        tabla_duplicada = tabla_duplicada.withColumn(col_name, floor(col(col_name) * (0.6 + rand() * 1.1)))

tabla_duplicada = tabla_duplicada.withColumn('date', to_date(col('date').cast("string"), 'yyyyMMdd'))

pandas_ciudades = pd.read_json("download/ciudades.json")
pandas_paises = pd.read_json("download/paises.json")

spark_ciudades = spark.createDataFrame(pandas_ciudades)
spark_paises = spark.createDataFrame(pandas_paises)

paises_ciudades_tabla = spark_paises.join(spark_ciudades, spark_paises['id'] == spark_ciudades['id']).select(
    spark_paises.id,
    spark_paises.id_pais,
    spark_ciudades.id_ciudad,
    spark_paises.Pais,
    spark_ciudades.Ciudad,
    spark_ciudades.Coordenadas
)

tabla_union = paises_ciudades_tabla.join(tabla_duplicada, on="id", how="right")

step_size = (1.5 - 0.7) / (120 - 1) if 120 > 1 else 0
multiplier_df = spark.range(120).withColumn("multiplier", lit(0.7) + col("id") * lit(step_size))
multiplier_df = multiplier_df.withColumn("copy_id", col("id").cast("integer")).drop("id")

expanded_df = tabla_union.crossJoin(multiplier_df)

for column in tabla_union.columns:
    if column not in ['date', 'id', 'id_pais', 'id_ciudad'] and isinstance(
            tabla_union.schema[column].dataType, (DoubleType, IntegerType, LongType)
    ):
        expanded_df = expanded_df.withColumn(column, (col(column) * col("multiplier")).cast("integer"))

if 'date' in tabla_union.columns:
    expanded_df = expanded_df.withColumn('date', date_add(col('date'), col('copy_id')))

tabla_final = expanded_df.drop('multiplier', 'copy_id')

final = tabla_final.toPandas()

final.to_parquet("generated_data/data_covid.parquet")

spark.stop()
