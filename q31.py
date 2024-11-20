# -*- coding: utf-8 -*-
"""
Created on Tue Nov 19 07:32:43 2024

@author: VISHWA


How to format or suppress scientific notations in a PySpark DataFrame?
# Assuming you have a DataFrame df and the column you want to format is 'your_column'
df = spark.createDataFrame([(1, 0.000000123), (2, 0.000023456), (3, 0.000345678)], ["id", "your_column"])

df.show()
+---+-----------+
| id|your_column|
+---+-----------+
| 1| 1.23E-7|
| 2| 2.3456E-5|
| 3| 3.45678E-4|
+---+-----------+




from pyspark.sql.functions import format_number

# Determine the number of decimal places you want
decimal_places = 10

df = df.withColumn("your_column", format_number("your_column", decimal_places))
df.show()

+---+------------+
| id| your_column|
+---+------------+
| 1|0.0000001230|
| 2|0.0000234560|
| 3|0.0003456780|
+---+------------+
"""

