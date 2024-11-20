# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 07:14:05 2024

@author: VISHWA

 How to Drop rows with NA values specific to a particular column?
Difficulty Level: L1

input

# Assuming df is your DataFrame
df = spark.createDataFrame([
("A", 1, None),
("B", None, "123" ),
("B", 3, "456"),
("D", None, None),
], ["Name", "Value", "id"])

df.show()
"""







"""
df_2 = df.dropna(subset=['Value'])

df_2.show()

"""