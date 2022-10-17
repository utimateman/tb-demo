import time
from pyspark import SparkContext
from pyspark.sql import SparkSession
import pyspark.sql.functions as func
from pyspark.sql.types import IntegerType, StringType

from delta import *
import os
import random

from datetime import datetime
import json



##################### GET TABLE HISTORY #####################


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



bucket_name = "demo-bucket"


history = spark.sql("DESCRIBE HISTORY delta.`s3a://""" + bucket_name + "/raw_user_data`")
history.select('timestamp').show(20, False)


history = spark.sql("DESCRIBE HISTORY delta.`s3a://""" + bucket_name + "/raw_user_data`")
history.show(20, False)


# past_df = spark.read.format("delta").option("timestampAsOf", "2022-10-17 17:37:34").load("s3a://" + bucket_name + "/raw_user_data")
# past_df.show()