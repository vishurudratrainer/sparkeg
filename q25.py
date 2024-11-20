# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 07:26:53 2024

@author: VISHWA


 How to replace missing spaces in a string with the least frequent character?
Difficulty Level: L3

Replace the spaces in my_str with the least frequent characte

#Sample DataFrame
df = spark.createDataFrame([('dbc deb abed gade',),], ["string"])
df.show()
+-----------------+
| string|
+-----------------+
|dbc deb abed gade|
+-----------------+
Desired output

+-----------------+-----------------+
| string| modified_string|
+-----------------+-----------------+
|dbc deb abed gade|dbccdebcabedcgade
"""

























"""
from pyspark.sql.functions import udf, explode
from pyspark.sql.types import StringType, ArrayType
from collections import Counter

def least_freq_char_replace_spaces(s):
counter = Counter(s.replace(" ", ""))
least_freq_char = min(counter, key = counter.get)
return s.replace(' ', least_freq_char)

udf_least_freq_char_replace_spaces = udf(least_freq_char_replace_spaces, StringType())

df = spark.createDataFrame([('dbc deb abed gade',)], ["string"])
df.withColumn('modified_string', udf_least_freq_char_replace_spaces(df['string'])).show()
"""
