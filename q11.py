# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 07:16:36 2024

@author: VISHWA

 How to find the numbers that are multiples of 3 from a column?
Difficulty Level: L2

Input

from pyspark.sql.functions import rand

# Generate a DataFrame with a single column "id" with 10 rows
df = spark.range(10)

# Generate a random float between 0 and 1, scale and shift it to get a random integer between 1 and 10
df = df.withColumn("random", ((rand(seed=42) * 10) + 1).cast("int"))

# Show the DataFrame
df.show()
"""




"""

from pyspark.sql.functions import col, when

# Assuming df is your DataFrame and "your_column" is the column with the numbers
df = df.withColumn("is_multiple_of_3", when(col("random") % 3 == 0, 1).otherwise(0))

df.show()
"""