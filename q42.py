# -*- coding: utf-8 -*-
"""
Created on Wed Nov 20 12:32:49 2024

@author: VISHWA
"""

"""
How to calculate Correlation of two variables in a DataFrame?
Difficulty Level: L1

Input

# Create a sample dataframe
data = [Row(feature1=5, feature2=10, feature3=25),
Row(feature1=6, feature2=15, feature3=35),
Row(feature1=7, feature2=25, feature3=30),
Row(feature1=8, feature2=20, feature3=60),
Row(feature1=9, feature2=30, feature3=70)]
df = spark.createDataFrame(data)

df.show()

"""



"""
# Calculate correlation
correlation = df.corr("feature1", "feature2")

print("Correlation between feature1 and feature2 :", correlation)


"""