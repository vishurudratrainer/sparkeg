# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 07:24:49 2024

@author: VISHWA



How to filter valid emails from a list?
Difficulty Level: L3

# Create a list
data = ['buying books at amazom.com', 'rameses@egypt.com', 'matt@t.co', 'narendra@modi.com']

# Convert the list to DataFrame
df = spark.createDataFrame(data, "string")
df.show(truncate =False)
"""













"""
# Define a regular expression pattern for emails
pattern = "^[a-zA-Z0-9_.+-]+@[a-zA-Z0-9-]+\.[a-zA-Z0-9-.]+$"

# Apply filter operation to keep only valid emails
df_filtered = df.filter(F.col("value").rlike(pattern))

# Show the DataFrame
df_filtered.show()

"""