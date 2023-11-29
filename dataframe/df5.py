# -*- coding: utf-8 -*-
"""
Created on Thu Dec  9 23:50:23 2021

@author: ASUS
"""


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField, StringType, IntegerType,BooleanType,DoubleType
spark = SparkSession.builder \
    .master("local[1]") \
    .appName("RDD") \
    .getOrCreate()

# Read JSON file into dataframe    
df = spark.read.json("C:/sparkeg/code/data/zipcodes.json")
df.printSchema()
df.show(2)

# Read multiline json file
multiline_df = spark.read.option("multiline","true") \
      .json("C:/sparkeg/code/data/multiline-zipcode.json")
multiline_df.show()

#Read multiple files
df2 = spark.read.json(
    ['C:/sparkeg/code/data/zipcode2.json',
     'C:/sparkeg/code/data/zipcode1.json'])
df2.show()    

#Read All JSON files from a directory
df3 = spark.read.json("C:/sparkeg/code/data/*.json")
df3.show()

# Define custom schema
schema = StructType([
      StructField("RecordNumber",IntegerType(),True),
      StructField("Zipcode",IntegerType(),True),
      StructField("ZipCodeType",StringType(),True),
      StructField("City",StringType(),True),
      StructField("State",StringType(),True),
      StructField("LocationType",StringType(),True),
      StructField("Lat",DoubleType(),True),
      StructField("Long",DoubleType(),True),
      StructField("Xaxis",IntegerType(),True),
      StructField("Yaxis",DoubleType(),True),
      StructField("Zaxis",DoubleType(),True),
      StructField("WorldRegion",StringType(),True),
      StructField("Country",StringType(),True),
      StructField("LocationText",StringType(),True),
      StructField("Location",StringType(),True),
      StructField("Decommisioned",BooleanType(),True),
      StructField("TaxReturnsFiled",StringType(),True),
      StructField("EstimatedPopulation",IntegerType(),True),
      StructField("TotalWages",IntegerType(),True),
      StructField("Notes",StringType(),True)
  ])

df_with_schema = spark.read.schema(schema) \
        .json("C:/sparkeg/code/data/zipcodes.json")
df_with_schema.printSchema()
df_with_schema.show()

# Create a table from Parquet File
spark.sql("CREATE OR REPLACE TEMPORARY VIEW zipcode3 USING json OPTIONS" + 
      " (path 'C:/sparkeg/code/data/zipcodes.json')")
spark.sql("select * from zipcode3").show()

# PySpark write Parquet File
df2.write.mode('Overwrite').json("C:/sparkeg/code/data/spark_output/zipcodes.json")
