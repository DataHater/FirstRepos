# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql import Row
import json

# Инициализация Spark Session
spark = SparkSession.builder.appName("example").getOrCreate()

# Создание списка данных
data = [
    Row(jsondata=json.dumps([{"name": "Alice", "id": 1}, {"name": "Bob", "id": 2}])),
    Row(jsondata=json.dumps([{"name": "Christina", "id": 3}])),
    Row(jsondata=json.dumps([{"name": "David", "id": 4}, {"name": "Emma", "id": 5}]))
]


df2 = spark.createDataFrame(data)

# COMMAND ----------

from pyspark.sql.functions import from_json, col, explode
from pyspark.sql.types import *
schema_segment = StructType([    
    StructField("name", StringType(), True),
    StructField("id", IntegerType(), True),
])

schema_lake_segments = ArrayType(schema_segment, True)

df3 = df2.withColumn( "testdata", from_json( col("jsondata"), schema_lake_segments) ) \
         .drop("jsondata") \
         .withColumn("segment_struct", explode(col('testdata'))) \
         .drop("testdata") \
         .withColumn("name", col('segment_struct.name')) \
         .withColumn("id", col('segment_struct.id')) \
         .drop("segment_struct")


# COMMAND ----------

df3 = df2.withColumn( "testdata", from_json( col("jsondata"), schema_lake_segments) ) \
         .drop("jsondata") \
         .withColumn("segment_struct", explode(col('testdata'))) \
         .drop("testdata") \
         .withColumn("name", col('segment_struct.id')) \
         .withColumn("id", col('segment_struct.id')) \
         .drop("segment_struct")

# COMMAND ----------

def writeStreamTo(self, df, tableName):
    return df.writeStream.format("delta").outputMode("append") \
        .trigger(availableNow=True)  \
        .option("mergeSchema", "true")  \
        .option("checkpointLocation", self.lakeConfig.getCheckpointLocation(tableName)).toTable(self.lakeConfig.getTableRef(tableName))

