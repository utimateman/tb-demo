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


##################### UPDATE DELTA TABLE #####################

# -> map age with label (forget)
# -> gender to upper case 
# -> is_processed from "no" to "yes"

###############################################################


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

# ----- [ Helper Function ] -----

def classifyAgeGroup(age):
    if age < 20:
        return "kid"
    elif age < 40:
        return "adult"
    return "old"


# ----- [ Update Dataframe Using PySpark ] -----

bucket_name = "demo-bucket"
df = spark.sql(
    """
    SELECT *
    FROM delta.`s3a://""" + bucket_name + "/raw_user_data`"
)
df.show() 

upper_case_name_df = df.withColumn("gender",func.upper(func.col("gender")))
upper_case_name_df.show()


# ----- [ Create New Delta Table ] -----

bucket_name = "demo-bucket"
upper_case_name_df.write.format("delta").mode("append").option("mergeSchema", "true").save("s3a://" + bucket_name + "/processed_user_data")
print("[ successful ] - write processed_user_data")

# ----- [ Update New Delta Table ] -----

spark.sql(
    """
    UPDATE delta.`s3a://""" + bucket_name + "/processed_user_data`" + 
    " SET is_processed='yes' WHERE is_processed='no'"
)
print("[ successful ] - update processed_user_data")




# ----- [ Echo Printing: Verification ] -----


bucket_name = "demo-bucket"
df = spark.sql(
    """
    SELECT *
    FROM delta.`s3a://""" + bucket_name + "/processed_user_data`"
)
df.show() 

# --------------------------------------
