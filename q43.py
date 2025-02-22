# -*- coding: utf-8 -*-
"""
Created on Wed Nov 20 12:34:03 2024

@author: VISHWA
"""

"""
How to calculate the Standard Deviation?
Difficulty Level: L1

Input

# Sample data
data = [("James", "Sales", 3000),
("Michael", "Sales", 4600),
("Robert", "Sales", 4100),
("Maria", "Finance", 3000),
("James", "Sales", 3000),
("Scott", "Finance", 3300),
("Jen", "Finance", 3900),
("Jeff", "Marketing", 3000),
("Kumar", "Marketing", 2000),
("Saif", "Sales", 4100)]

# Create DataFrame
df = spark.createDataFrame(data, ["Employee", "Department", "Salary"])

df.show()

"""



"""
from pyspark.sql.functions import stddev

salary_stddev = df.select(stddev("Salary").alias("stddev"))

salary_stddev.show()
"""