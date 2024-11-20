# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 07:32:11 2024

@author: VISHWA


How to change the order of columns of a dataframe?
Difficulty Level: L1

Input

# Sample data
data = [("John", "Doe", 30), ("Jane", "Doe", 25), ("Alice", "Smith", 22)]

# Create DataFrame from the data
df = spark.createDataFrame(data, ["First_Name", "Last_Name", "Age"])

# Show the DataFrame
df.show()
"""






"""
new_order = ["Age", "First_Name", "Last_Name"]

# Reorder the columns
df = df.select(*new_order)

# Show the DataFrame with reordered columns
df.show()

"""