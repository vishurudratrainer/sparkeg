# -*- coding: utf-8 -*-
"""
Created on Wed Oct 11 20:27:32 2023

@author: ASUS
"""


# Import
from pyspark.sql import SparkSession

# Create SparkSession
spark = SparkSession.builder.master("local[1]") \
                    .appName('RDD.com') \
                    .getOrCreate()

# Read CSV file into DataFrame
df = spark.read.csv("/tmp/resources/zipcodes.csv")

#Write DataFrame to CSV file
df.write.csv("/tmp/spark_output/zipcodes")

# Write CSV file with column header (column names)
df.write.option("header",True) \
 .csv("/tmp/spark_output/zipcodes")

# Other CSV options
df2.write.options(header='True', delimiter=',') \
 .csv("/tmp/spark_output/zipcodes")




# Saving modes
df2.write.mode('overwrite').csv("/tmp/spark_output/zipcodes")
# You can also use this
df2.write.format("csv").mode('overwrite').save("/tmp/spark_output/zipcodes")
