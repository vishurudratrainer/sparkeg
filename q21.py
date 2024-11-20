# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 07:24:06 2024

@author: VISHWA

How to filter words that contain atleast 2 vowels from a series?
Difficulty Level: L3

# example dataframe
df = spark.createDataFrame([('Apple',), ('Orange',), ('Plan',) , ('Python',) , ('Money',)], ['Word'])

df.show()
"""












"""
from pyspark.sql.functions import col, length, translate

# Filter words that contain at least 2 vowels
df_filtered = df.where((length(col('Word')) - length(translate(col('Word'), 'AEIOUaeiou', ''))) >= 2)
df_filtered.show()

"""