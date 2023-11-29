# -*- coding: utf-8 -*-
"""
Created on Sun Dec 12 22:34:16 2021

@author: ASUS
"""

from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("linear_regression_model").getOrCreate()
real_estate = spark.read.option("inferSchema", "true").csv("C://files//real_estate.csv",header=True)
real_estate.printSchema()
real_estate.show(2)

from pyspark.ml.feature import VectorAssembler

assembler = VectorAssembler(inputCols=[ 
 'X1 transaction date',
 'X2 house age',
 'X3 distance to the nearest MRT station',
 'X4 number of convenience stores',
 'X5 latitude',
 'X6 longitude'],
 outputCol='features')

data_set = assembler.transform(real_estate)
data_set.select(['features','Y house price of unit area']).show(2)
train_data,test_data = data_set.randomSplit([0.7,0.3])

from pyspark.ml.regression import LinearRegression

lr = LinearRegression(labelCol='Y house price of unit area')
lrModel = lr.fit(train_data)
test_stats = lrModel.evaluate(test_data)
print(f"RMSE: {test_stats.rootMeanSquaredError}")
print(f"R2: {test_stats.r2}")
print(f"R2: {test_stats.meanSquaredError}")
