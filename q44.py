# -*- coding: utf-8 -*-
"""
Created on Wed Nov 20 12:34:53 2024

@author: VISHWA
"""

"""
How to calculate missing value percentage in each column?
Difficulty Level: L3

Input

# Create a sample dataframe
data = [("John", "Doe", None),
(None, "Smith", "New York"),
("Mike", "Smith", None),
("Anna", "Smith", "Boston"),
(None, None, None)]

df = spark.createDataFrame(data, ["FirstName", "LastName", "City"])

df.show()

"""



"""
 Calculate the total number of rows in the dataframe
total_rows = df.count()

# For each column calculate the number of null values and then calculate the percentage
for column in df.columns:
null_values = df.filter(df[column].isNull()).count()
missing_percentage = (null_values / total_rows) * 100
print(f"Missing values in {column}: {missing_percentage}%")
"""