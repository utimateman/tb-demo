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

def classifyAgeGroup(age):
    if age < 20:
        return "kid"
    elif age < 40:
        return "adult"
    return "old"

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
    SELECT id, name, age
    FROM delta.`s3a://""" + bucket_name + "/pre_processed_user_data` " +
    "WHERE is_processed='no'"
)
df.show() 

lower_case_name_df = df.withColumn("name",func.lower(func.col("name")))
lower_case_name_df.show()


""" Converting function to UDF """
classifyAgeGroupUDF = func.udf(lambda z: classifyAgeGroup(z),StringType())

processed_df = lower_case_name_df.withColumn("age_group", classifyAgeGroupUDF(func.col("age")))

# extra column to show "DELTA UPDATE OPERATION"
processed_df = processed_df.withColumn("age_range", classifyAgeGroupUDF(func.col("age")))

processed_df.show()

# --- 



# [ CREATE TABLES ]


# --- 

bucket_name = "example-data-pipeline"
processed_df.write.format("delta").mode("append").option("mergeSchema", "true").save("s3a://" + bucket_name + "/processed_user_data")
print("[ successful ] - write processed_user_data")

df = spark.sql(
    """
    UPDATE delta.`s3a://""" + bucket_name + "/pre_processed_user_data`" + 
    " SET is_processed='yes' WHERE is_processed='no'"
)

df = spark.sql(
    """
    DELETE FROM delta.`s3a://""" + bucket_name + "/raw_user_data`"
)



# --- 

# [ ECHO PRINTING ]

# --- 

bucket_name = "example-data-pipeline"
df = spark.sql(
    """
    SELECT *
    FROM delta.`s3a://""" + bucket_name + "/processed_user_data`"
)
df.show() 

# ---
