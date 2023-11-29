# -*- coding: utf-8 -*-
"""
Created on Tue Oct 10 21:36:50 2023

@author: ASUS
"""

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import overlay
from pyspark.sql.types import StructType, StructField, StringType,IntegerType

spark = SparkSession.builder.appName('RDD').getOrCreate()
df = spark.createDataFrame([("SPARK_SQL", "CORE")], ("x", "y"))
df.select(overlay("x", "y", 7).alias("overlayed")).show()