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

# [ INIT PYSPARK ]

# ---

def load_config(spark_context: SparkContext):
    spark_context._jsc.hadoopConfiguration().set('fs.s3a.access.key','minio')
    spark_context._jsc.hadoopConfiguration().set('fs.s3a.secret.key','minio123')
    spark_context._jsc.hadoopConfiguration().set('fs.s3a.path.style.access','true')
    spark_context._jsc.hadoopConfiguration().set('fs.s3a.impl','org.apache.hadoop.fs.s3a.S3AFileSystem')
    spark_context._jsc.hadoopConfiguration().set('fs.s3a.endpoint','localhost:9000')
    spark_context._jsc.hadoopConfiguration().set('fs.s3a.connection.ssl.enabled','false')



def classifyAgeGroup(age):
    if age < 20:
        return "kid"
    elif age < 40:
        return "adult"
    return "old"


builder = SparkSession.builder \
                .appName('Pyspark Delta Minio') \
                .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
                .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

spark = spark = configure_spark_with_delta_pip(builder).getOrCreate()
load_config(spark.sparkContext)

# ---

# [ PROCESS DATA ]
# > lower case name

# --- 

bucket_name = "example-data-pipeline"
df = spark.sql(
    """
    SELECT *
    FROM delta.`s3a://""" + bucket_name + "/raw_user_level_data`"
)
df.show() 

lower_case_name_df = df.withColumn("level",func.lower(func.col("level")))
lower_case_name_df.show()

# --- 



# [ CREATE TABLES ]


# --- 

bucket_name = "example-data-pipeline"
lower_case_name_df.write.format("delta").mode("append").option("mergeSchema", "true").save("s3a://" + bucket_name + "/processed_user_level_data")
print("[ successful ] - write processed_user_level_data")

# --- 



# [ ECHO PRINTING ]

# --- 

bucket_name = "example-data-pipeline"
df = spark.sql(
    """
    SELECT *
    FROM delta.`s3a://""" + bucket_name + "/processed_user_level_data`"
)
df.show() 

# ---