# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import Row
import json


data = [
    Row(jsondata=json.dumps([{"name": "Alice", "id": 1}, {"name": "Bob", "id": 2}])),
    Row(jsondata=json.dumps([{"name": "Christina", "id": 3}])),
    Row(jsondata=json.dumps([{"name": "David", "id": 4}, {"name": "Emma", "id": 5}]))
]

# COMMAND ----------

df = spark.createDataFrame(data)
df.write.json("/FileStore/tables/")

# COMMAND ----------

from pyspark.sql.types import StructType, StringType, IntegerType, ArrayType

# Определение схемы данных
schema = StructType() \
    .add("name", StringType()) \
    .add("id", IntegerType())

schema_lake_segments = ArrayType(schema, True)

# Создание потокового DataFrame путем чтения из каталога
stream_df = spark.readStream.schema(schema).json("/FileStore/tables/")

# COMMAND ----------

from pyspark.sql.types import StructType, StringType, ArrayType, IntegerType
from pyspark.sql.functions import from_json, col, explode

# Используйте правильную схему для чтения потокового DataFrame
schema_stream = StructType().add("jsondata", StringType())
schema_lake_segments = ArrayType(StructType().add("name", StringType()).add("id", IntegerType()))

# Чтение потокового DataFrame
stream_df = spark.readStream.schema(schema_stream).json("/FileStore/tables/")

# Преобразование данных
df_stream_transformed = stream_df.withColumn("testdata", from_json(col("jsondata"), schema_lake_segments)) \
    .drop("jsondata") \
    .withColumn("segment_struct", explode(col("testdata"))) \
    .drop("testdata") \
    .withColumn("name", col("segment_struct.name")) \
    .withColumn("id", col("segment_struct.id")) \
    .drop("segment_struct")

# COMMAND ----------

class LakeConfig:
    def __init__(self, basePath):
        self.basePath = basePath

    def getCheckpointLocation(self, tableName):
        return f"{self.basePath}/checkpoints/{tableName}"

    def getTableRef(self, tableName):
        return f"{self.basePath}/tables/{tableName}"

lakeConfig = LakeConfig("dbfs:/user/hive/warehouse")

def writeStreamTo(df, tableName):
    tablePath = lakeConfig.getTableRef(tableName)
    checkpointLocation = lakeConfig.getCheckpointLocation(tableName)

    return df.writeStream.format("delta").outputMode("append") \
        .trigger(availableNow=True) \
        .option("mergeSchema", "true") \
        .option("checkpointLocation", checkpointLocation) \
        .start(tablePath)  # Изменено на start для указания пути

# Использование функции с вашим потоковым DataFrame
writeStreamTo(df_stream_transformed, "test_Table")

# COMMAND ----------


