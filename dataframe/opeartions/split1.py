# -*- coding: utf-8 -*-
"""
Created on Thu Oct 12 14:41:04 2023

@author: ASUS
"""


from pyspark.sql import SparkSession
spark = SparkSession.builder \
         .appName('RDD') \
         .getOrCreate()

data = [("James, A, Smith","2018","M",3000),
            ("Michael, Rose, Jones","2010","M",4000),
            ("Robert,K,Williams","2010","M",4000),
            ("Maria,Anne,Jones","2005","F",4000),
            ("Jen,Mary,Brown","2010","",-1)
            ]

columns=["name","dob_year","gender","salary"]
df=spark.createDataFrame(data,columns)
df.printSchema()
df.show(truncate=False)

from pyspark.sql.functions import split, col
split_column = split(df.name,",")
df2 = df.withColumn("firstname", split_column.getItem(0)).withColumn("secondname", split_column.getItem(1))


df2.show()