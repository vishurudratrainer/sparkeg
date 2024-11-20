# -*- coding: utf-8 -*-
"""
Created on Wed Nov 20 12:26:03 2024

@author: VISHWA
"""
"""
How to get the positions where values of two columns match?
Difficulty Level: L1

Input

# Create sample DataFrame
data = [("John", "John"), ("Lily", "Lucy"), ("Sam", "Sam"), ("Lucy", "Lily")]
df = spark.createDataFrame(data, ["Name1", "Name2"])

df.show()

"""



"""
from pyspark.sql.functions import when
from pyspark.sql.functions import col

# Add new column Match to indicate if Name1 and Name2 match
df = df.withColumn("Match", when(col("Name1") == col("Name2"), True).otherwise(False))

# Display DataFrame
df.show()

"""