# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 07:17:21 2024

@author: VISHWA

How to extract items at given positions from a column?
Difficulty Level: L2

Input

from pyspark.sql.functions import rand

# Generate a DataFrame with a single column "id" with 10 rows
df = spark.range(10)

# Generate a random float between 0 and 1, scale and shift it to get a random integer between 1 and 10
df = df.withColumn("random", ((rand(seed=42) * 10) + 1).cast("int"))

# Show the DataFrame
df.show()

pos = [0, 4, 8, 5]
"""





"""
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, monotonically_increasing_id

pos = [0, 4, 8, 5]

# Define window specification
w = Window.orderBy(monotonically_increasing_id())

# Add index
df = df.withColumn("index", row_number().over(w) - 1)

df.show()

# Filter the DataFrame based on the specified positions
df_filtered = df.filter(df.index.isin(pos))

df_filtered.show()

"""