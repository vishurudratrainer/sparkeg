# -*- coding: utf-8 -*-
"""
Created on Thu Oct 12 20:33:55 2023

@author: ASUS
"""

# Imports
from pyspark.sql import SparkSession

# Create SparkSession
#spark = SparkSession.builder.appName('RDD.com').config("spark.jars", "C:/Users/ASUS/.m2/repository/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar").getOrCreate()
spark = SparkSession \
        .builder \
        .appName('test') \
        .master('local[*]') \
        .config("spark.driver.extraClassPath", "C:/Users/ASUS/.m2/repository/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar") \
        .getOrCreate()

# Create DataFrame 
columns = ["id", "name","age","gender"]
data = [(1, "James",30,"M"), (2, "Ann",40,"F"),
    (3, "Jeff",41,"M"),(4, "Jennifer",20,"F")]

sampleDF = spark.sparkContext.parallelize(data).toDF(columns)

# Write to MySQL Table
sampleDF.write \
  .format("jdbc") \
  .option("driver","com.mysql.jdbc.Driver") \
  .option("url", "jdbc:mysql://localhost:3306/hdfc") \
  .option("dbtable", "employee") \
  .option("user", "root") \
  .option("password", "root") \
  .save()

# Read from MySQL Table
df = spark.read \
    .format("jdbc") \
    .option("driver","com.mysql.jdbc.Driver") \
    .option("url", "jdbc:mysql://localhost:3306/hdfc") \
    .option("dbtable", "employee") \
    .option("user", "root") \
    .option("password", "root") \
    .load()

df.show()