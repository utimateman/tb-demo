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

def labelRequiredProcess(id):
    return "no"

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

# [ PROCESS DATA ]
# > lower case name
# > adding new column, class range
# > is_processed

# --- 

bucket_name = "example-data-pipeline"
df = spark.sql(
    """
    SELECT *
    FROM delta.`s3a://""" + bucket_name + "/raw_user_data`"
)
df.show() 



""" Converting function to UDF """
requiredProcessUDF = func.udf(lambda z: labelRequiredProcess(z),StringType())

pre_processed_df = df.withColumn("is_processed", requiredProcessUDF(func.col("id")))
pre_processed_df.show()

# --- 



# [ CREATE TABLES ]


# --- 

bucket_name = "example-data-pipeline"
pre_processed_df.write.format("delta").mode("append").option("mergeSchema", "true").save("s3a://" + bucket_name + "/pre_processed_user_data")
print("[ successful ] - write pre_processed_user_data")

# --- 

# [ ECHO PRINTING ]

# --- 

bucket_name = "example-data-pipeline"
df = spark.sql(
    """
    SELECT *
    FROM delta.`s3a://""" + bucket_name + "/pre_processed_user_data`"
)
df.show() 

# ---
