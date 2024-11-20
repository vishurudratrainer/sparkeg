# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 07:05:32 2024

@author: VISHWA
"""

#How to convert the index of a 
#PySpark DataFrame into a column?



















from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("RDD").getOrCreate()

# Assuming df is your DataFrame
df = spark.createDataFrame([
("Alice", 1),
("Bob", 2),
("Charlie", 3),
], ["Name", "Value"])

df.show()



#Ans

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, monotonically_increasing_id

# Define window specification
w = Window.orderBy(monotonically_increasing_id())

# Add index
df = df.withColumn("index", row_number().over(w) - 1)

df.show()
