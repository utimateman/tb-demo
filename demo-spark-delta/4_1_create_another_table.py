import time
from pyspark import SparkContext
from pyspark.sql import SparkSession
from delta import *
import os
import random

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

x = [[1,'programmer',"swim"],[2,'painter',"football"],[3,'programmer',"swim"],[4,'painter',"badminton"],
     [5, 'programmer',"football"], [6,'painter',"swim"],[7,'programmer',"badminton"],[8,'driver',"swim"]]

df = spark.createDataFrame(data=x, schema = ["id","occupation","sport"])
df.show()




# ----- [ Create Delta Table ] -----


bucket_name = "demo-bucket"
df.write.format("delta").mode("append").option("mergeSchema", "true").save("s3a://" + bucket_name + "/raw_user_activity_data")
print("[ successful ] - write raw_user_activity_data")


# ----- [ Echo Printing: Verification ] -----


out_df = spark.sql(
    """
    SELECT *
    FROM delta.`s3a://""" + bucket_name + "/raw_user_activity_data`"
)
out_df.show() 

# --------------------------------------

