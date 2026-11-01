# -*- coding: utf-8 -*-
"""
Created on Fri Dec 10 13:09:11 2021

@author: ASUS
"""

# -*- coding: utf-8 -*-
"""
Created on Thu Dec  9 21:41:07 2021

@author: ASUS
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('RDD').getOrCreate()
spark.sparkContext.setLogLevel("INFO")


rdd = spark.sparkContext.textFile("C:/sparkeg/code/data/data.txt")
res = rdd.collect()

rddaftersplit=rdd.flatMap(lambda x: x.split(" "))
res=rddaftersplit.collect()

rdd3=rddaftersplit.map(lambda x: (x.lower(),1))
res=rdd3.collect()


rdd4=rdd3.reduceByKey(lambda a,b: a+b)
res=rdd4.collect()



rdd5 = rdd4.map(lambda x: (x[1],x[0])).sortByKey()
res=rdd5.collect()
print(res)


rdd5.saveAsTextFile("C:/sparkeg/code/data/wordCount1.txt")


