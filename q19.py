# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 07:22:39 2024

@author: VISHWA

How to get the day of month, week number, day of year and day of week from a date strings?
Difficulty Level: L2

# example data
data = [("2023-05-18","01 Jan 2010",), ("2023-12-31", "01 Jan 2010",)]
df = spark.createDataFrame(data, ["date_str_1", "date_str_2"])

df.show()
"""













"""
from pyspark.sql.functions import to_date, dayofmonth, weekofyear, dayofyear, dayofweek

# Convert date string to date format
df = df.withColumn("date_1", to_date(df.date_str_1, 'yyyy-MM-dd'))
df = df.withColumn("date_2", to_date(df.date_str_2, 'dd MMM yyyy'))

df = df.withColumn("day_of_month", dayofmonth(df.date_1))\
.withColumn("week_number", weekofyear(df.date_1))\
.withColumn("day_of_year", dayofyear(df.date_1))\
.withColumn("day_of_week", dayofweek(df.date_1))

df.show()
"""