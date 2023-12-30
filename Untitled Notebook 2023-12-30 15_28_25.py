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
df.write.mode("overwrite").json("/FileStore/tables/")

# COMMAND ----------

from pyspark.sql.types import StructType, StringType, IntegerType, ArrayType

# Определение схемы данных
schema = StructType() \
    .add("name", StringType()) \
    .add("id", IntegerType()) \
    .add("city", StringType()) \

schema_lake_segments = ArrayType(schema, True)

# Создание потокового DataFrame путем чтения из каталога
stream_df = spark.readStream.schema(schema).json("/FileStore/tables/")

# COMMAND ----------

from pyspark.sql.types import StructType, StringType, StructField, IntegerType
from pyspark.sql.functions import from_json, col
import json

# Create the schema for the stream DataFrame
schema_stream = StructType().add("jsondata", StringType())
# Create the schema for the data in the JSON payload
schema_lake_segments = StructType([
    StructField("name", StringType()),
    StructField("id", IntegerType()),
    StructField("city", StringType())
])

# Convert the schema to a format that can be used in from_json()
schema_json = schema_lake_segments.json()

# Read the stream DataFrame
stream_df = spark.readStream.schema(schema_stream).json("/FileStore/tables/")

# Transform the stream DataFrame
df_stream_transformed = stream_df \
    .withColumn("testdata", from_json(col("jsondata"), schema_json)) \
    .drop("jsondata") \
    .withColumn("name", col("testdata.name")) \
    .withColumn("id", col("testdata.id")) \
    .withColumn("city", col("testdata.city")) \
    .drop("testdata")

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
writeStreamTo(df_stream_transformed, "new_test_table")
