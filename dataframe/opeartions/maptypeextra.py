# -*- coding: utf-8 -*-
"""
Created on Wed Oct 11 20:35:54 2023

@author: ASUS
"""


from pyspark.sql.types import StructField, StructType, StringType, MapType
schema = StructType([
    StructField('name', StringType(), True),
    StructField('properties', MapType(StringType(),StringType()),True)
])

from pyspark.sql import SparkSession
spark = SparkSession.builder.appName('RDD.com').getOrCreate()
dataDictionary = [
        ('James',{'hair':'black','eye':'brown'}),
        ('Michael',{'hair':'brown','eye':None}),
        ('Robert',{'hair':'red','eye':'black'}),
        ('Washington',{'hair':'grey','eye':'grey'}),
        ('Jefferson',{'hair':'brown','eye':''})
        ]
df = spark.createDataFrame(data=dataDictionary, schema = schema)
df.printSchema()
df.show(truncate=False)

df.withColumn("hair",df.properties.getItem("hair")) \
  .withColumn("eye",df.properties.getItem("eye")) \
  .drop("properties") \
  .show()

df.withColumn("hair",df.properties["hair"]) \
  .withColumn("eye",df.properties["eye"]) \
  .drop("properties") \
  .show()

from pyspark.sql.functions import explode
df.select(df.name,explode(df.properties)).show()



from pyspark.sql.functions import map_keys
df.select(df.name,map_keys(df.properties)).show()



from pyspark.sql.functions import map_values
df.select(df.name,map_values(df.properties)).show()