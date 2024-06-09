import datetime
import os
import shutil

from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, lit

spark = SparkSession.builder.appName("Process Parquet COVID").getOrCreate()

df = spark.read.parquet('data_covid.parquet')

drop_columns = ['id_pais', 'id_ciudad', 'dateChecked', 'hash', 'lastModified', 'posNeg', 'recovered', 'total']
drop_columns = [col for col in drop_columns if col in df.columns]
result = df.drop(*drop_columns)

df = df.withColumn("YYYY", split(col("date"), "-")[0]) \
       .withColumn("MM", split(col("date"), "-")[1]) \
       .withColumn("DD", split(col("date"), "-")[2])
df = df.drop("date")
current_date = datetime.datetime.now().strftime("%Y-%m-%d %H:%M")
df = df.withColumn("F_procesado", lit(current_date))

final = df.toPandas()

final_parquet = final.to_parquet('data_covid_final.parquet')

filenames = ['download/ciudades.json', 'download/paises.json', 'generated_data/data_covid.parquet']

original_dir = 'generated_data'
destiny_dir = 'old'

os.makedirs(destiny_dir, exist_ok=True)

for filename in filenames:
    original = os.path.join(original_dir, filename)
    destiny = os.path.join(destiny_dir, filename)

    try:
        shutil.move(original, destiny)
    except Exception as e:
        print(f"Error to move {filename}: {e}")
