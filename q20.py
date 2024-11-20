# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 07:23:21 2024

@author: VISHWA


 How to convert year-month string to dates corresponding to the 4th day of the month?
Difficulty Level: L2

# example dataframe
df = spark.createDataFrame([('Jan 2010',), ('Feb 2011',), ('Mar 2012',)], ['MonthYear'])

df.show()
"""











"""
from pyspark.sql.functions import expr, col

# convert YearMonth to date (default to first day of the month)
df = df.withColumn('Date', expr("to_date(MonthYear, 'MMM yyyy')"))

df.show()

# replace day with 4
df = df.withColumn('Date', expr("date_add(date_sub(Date, day(Date) - 1), 3)"))

df.show()
"""