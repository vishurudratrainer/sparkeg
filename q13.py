# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 07:18:06 2024

@author: VISHWA

 How to stack two DataFrames vertically ?
Difficulty Level: L1

Input

# Create DataFrame for region A
df_A = spark.createDataFrame([("apple", 3, 5), ("banana", 1, 10), ("orange", 2, 8)], ["Name", "Col_1", "Col_2"])
df_A.show()

# Create DataFrame for region B
df_B = spark.createDataFrame([("apple", 3, 5), ("banana", 1, 15), ("grape", 4, 6)], ["Name", "Col_1", "Col_3"])
df_B.show()
"""





"""
df_A.union(df_B).show()
"""