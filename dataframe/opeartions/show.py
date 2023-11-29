# -*- coding: utf-8 -*-
"""
Created on Thu Dec  9 22:54:00 2021

@author: ASUS
"""


from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('RDD').getOrCreate()
columns = ["Seqno","Quote"]
data = [("1", "Be the change that you wish to see in the world"),
    ("2", "Everyone thinks of changing the world, but no one thinks of changing himself."),
    ("3", "The purpose of our lives is to be happy."),
    ("4", "Be cool.")]
df = spark.createDataFrame(data,columns)
df.show()
df.show(truncate=False)
# Display 2 rows and full column contents
df.show(2,truncate=False) 
# Display 2 rows & column values 25 characters
df.show(2,truncate=25) 
# Display DataFrame rows & columns vertically
df.show(n=3,truncate=25,vertical=True)
