# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import Row
import json


data = [
    Row(jsondata=json.dumps([{"name": "David", "id": 4, "city": "Kar"}, {"name": "Emma", "id": 5, "city": "Sidney"}])),
    Row(jsondata=json.dumps([{"name": "Wavid", "id": 2, "city": "NY"}, {"name": "mma", "id": 53, "city": "LA"}]))
]

# COMMAND ----------

df = spark.createDataFrame(data)

# Перезапись данных в JSON-файл
df.write.mode("overwrite").json("/FileStore/tablesr/")

# COMMAND ----------

from pyspark.sql.types import StructType, StringType, StructField, IntegerType, ArrayType
from pyspark.sql.functions import explode, from_json, col

# Схема для данных внутри JSON
schema = StructType([
    StructField("name", StringType()),
    StructField("id", IntegerType()),
    StructField("city", IntegerType()),
])

# Схема для чтения потока
schema_stream = StructType().add("jsondata", StringType())

# Чтение потока с одним полем jsondata
stream_df = spark.readStream.schema(schema_stream).json("/FileStore/tablesr/")

# Преобразование jsondata в структурированный формат и расширение массивов
transformed_stream_df = stream_df \
    .withColumn("json_array", from_json("jsondata", ArrayType(schema))) \
    .withColumn("exploded", explode("json_array")) \
    .select("exploded.*")

# Отображение преобразованного потока
display(transformed_stream_df)

# COMMAND ----------

class LakeConfig:
    def __init__(self, basePath):
        self.basePath = basePath

    def getCheckpointLocation(self, tableName):
        return f"{self.basePath}/checkpoints/{tableName}"

    def getTableRef(self, tableName):
        return f"{self.basePath}/tables/{tableName}"

lakeConfig = LakeConfig("dbfs:/user/hive/warehouse/default")

def writeStreamTo(df, tableName):
    tablePath = lakeConfig.getTableRef(tableName)
    checkpointLocation = lakeConfig.getCheckpointLocation(tableName)

    return df.writeStream.format("delta").outputMode("append") \
        .trigger(availableNow=True) \
        .option("mergeSchema", "true") \
        .option("checkpointLocation", checkpointLocation) \
        .start(tablePath)  # Изменено на start для указания пути

# Использование функции с вашим потоковым DataFrame
writeStreamTo(transformed_stream_df, "new_test_table")
