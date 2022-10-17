import time
from pyspark import SparkContext
from pyspark.sql import SparkSession
from delta import *
import os
import random

from datetime import datetime
import json

# [ INIT PYSPARK ]

# ---

def load_config(spark_context: SparkContext):
    spark_context._jsc.hadoopConfiguration().set('fs.s3a.access.key','minio')
    spark_context._jsc.hadoopConfiguration().set('fs.s3a.secret.key','minio123')
    spark_context._jsc.hadoopConfiguration().set('fs.s3a.path.style.access','true')
    spark_context._jsc.hadoopConfiguration().set('fs.s3a.impl','org.apache.hadoop.fs.s3a.S3AFileSystem')
    spark_context._jsc.hadoopConfiguration().set('fs.s3a.endpoint','localhost:9000')
    spark_context._jsc.hadoopConfiguration().set('fs.s3a.connection.ssl.enabled','false')

builder = SparkSession.builder \
                .appName('Pyspark Delta Minio') \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = spark = configure_spark_with_delta_pip(builder).getOrCreate()
load_config(spark.sparkContext)

# ---


# [ Create Example Data ]

# ---

x = [[4,'Suzy',21],[5,'Sakura',16],[6,'Makarov',58]]
df = spark.createDataFrame(data=x, schema = ["id","name","age"])
df.show()

# ---



# [ CREATE TABLES ]


# --- 

bucket_name = "example-data-pipeline"
df.write.format("delta").mode("append").option("mergeSchema", "true").save("s3a://" + bucket_name + "/raw_user_data")
print("[ successful ] - write raw_user_data")

# --- 


# [ ECHO PRINTING ]

# --- 

out_df = spark.sql(
    """
    SELECT *
    FROM delta.`s3a://""" + bucket_name + "/raw_user_data`"
)
out_df.show() 

# --- 

