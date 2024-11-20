# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 07:34:33 2024

@author: VISHWA

 How to filter every nth row in a dataframe?
Difficulty Level: L2

Input

# Sample data
data = [("Alice", 1), ("Bob", 2), ("Charlie", 3), ("Dave", 4), ("Eve", 5),
("Frank", 6), ("Grace", 7), ("Hannah", 8), ("Igor", 9), ("Jack", 10)]

# Create DataFrame
df = spark.createDataFrame(data, ["Name", "Number"])

df.show()
"""




"""
# Define window
window = Window.orderBy(monotonically_increasing_id())

# Add row_number to DataFrame
df = df.withColumn("rn", row_number().over(window))

n = 5 # filter every 5th row

# Filter every nth row
df = df.filter((df.rn % n) == 0)

df.show()
"""