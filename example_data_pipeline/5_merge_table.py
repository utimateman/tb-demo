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


# [ READ PROCESSED DATA ]

# --- 
bucket_name = "example-data-pipeline"

processed_user_df = spark.sql(
    """
    SELECT *
    FROM delta.`s3a://""" + bucket_name + "/processed_user_data`"
)
processed_user_df.show() 

processed_user_level_df = spark.sql(
    """
    SELECT *
    FROM delta.`s3a://""" + bucket_name + "/processed_user_level_data`"
)
processed_user_level_df = processed_user_level_df.withColumnRenamed("user_id","id")
processed_user_level_df.show() 

# --- 


# [ JOIN TABLE ]
# https://sparkbyexamples.com/spark/rename-a-column-on-spark-dataframes/
# ---

df_inner = processed_user_df.join(processed_user_level_df, on=['id'], how='inner')
df_inner.show()

# ---


# [ CREATE TABLES ]


# --- 

bucket_name = "example-data-pipeline"
df_inner.write.format("delta").mode("append").option("mergeSchema", "true").save("s3a://" + bucket_name + "/final_user_data")
print("[ successful ] - write final_user_data")

# --- 
