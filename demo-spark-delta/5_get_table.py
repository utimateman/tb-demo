import time
from pyspark import SparkContext
from pyspark.sql import SparkSession
from delta import *
import os
import random

from datetime import datetime
import json


##################### MERGE DELTA TABLE #####################


# ----- [ Initialize PySpark and S3 Connections ] -----

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




# ----- [ Retrieve Data: raw_user_data] -----

bucket_name = "demo-bucket"

processed_user_df = spark.sql(
    """
    SELECT *
    FROM delta.`s3a://""" + bucket_name + "/raw_user_data`"
)
print("----- [ raw_user_data ] -----")
processed_user_df.show() 

# ----- [ Retrieve Data: processed_user_data ] -----

bucket_name = "demo-bucket"

processed_user_df = spark.sql(
    """
    SELECT *
    FROM delta.`s3a://""" + bucket_name + "/processed_user_data`"
)
print("----- [ processed_user_data ] -----")
processed_user_df.show() 

# ----- [ Retrieve Data: user_activity_data ] -----

bucket_name = "demo-bucket"

processed_user_df = spark.sql(
    """
    SELECT *
    FROM delta.`s3a://""" + bucket_name + "/raw_user_activity_data`"
)
print("----- [ raw_user_activity_data ] -----")
processed_user_df.show() 

# ----- [ Retrieve Data: detail_user_data ] -----

bucket_name = "demo-bucket"

processed_user_df = spark.sql(
    """
    SELECT *
    FROM delta.`s3a://""" + bucket_name + "/detail_user_data`"
)
print("----- [ detail_user_data ] -----")
processed_user_df.show() 