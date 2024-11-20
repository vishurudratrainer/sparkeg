# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 07:20:26 2024

@author: VISHWA


How to compute summary statistics for all columns in a dataframe
Difficulty Level: L1

# For the sake of example, we'll create a sample DataFrame
data = [('James', 34, 55000),
('Michael', 30, 70000),
('Robert', 37, 60000),
('Maria', 29, 80000),
('Jen', 32, 65000)]

df = spark.createDataFrame(data, ["name", "age" , "salary"])

df.show()
"""








"""
# Summary statistics
summary = df.summary()

# Show the summary statistics
summary.show()

"""