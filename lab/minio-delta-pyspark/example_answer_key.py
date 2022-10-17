import time
from pyspark import SparkContext
from pyspark.sql import SparkSession
from delta import *
import os
import random
import pandas as pd

from datetime import datetime
import json

##################### CREATE DELTA TABLE #####################

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



# ----- [ Create Example Data ] -----

# Read CSV data using Pandas
file_path = "../data/computer_games.csv"
computer_games_df = pd.read_csv(file_path)

# Convert Pandas dataframe to Spark dataframe 
computer_games_spark_df = spark.createDataFrame(computer_games_df)
computer_games_spark_df.show()




# ----- [ Create Delta Table ] -----

bucket_name = "lab"
schema_name = "computergames"
schema_format = "delta"
computer_games_spark_df.write.format(schema_format).mode("append").option("mergeSchema", "true").save("s3a://" + bucket_name + "/" + schema_name)
print("[ successful ] - write on " + bucket_name + "/" + schema_name)


# ----- [ Echo Printing: Verification ] -----

spark.sql(
    """
    SELECT *
    FROM delta.`s3a://""" + bucket_name + "/" + schema_name + "`"
)

out_df = spark.sql(
    """
    SELECT *
    FROM delta.`s3a://""" + bucket_name + "/" + schema_name + "`"
)

print("---------- Computer Games Data (Delta) ----------")
out_df.show() 

# --------------------------------------

