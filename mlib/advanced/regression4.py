# -*- coding: utf-8 -*-
"""
Created on Sun Dec 12 22:34:16 2021

@author: ASUS
"""

from pyspark.sql import SparkSession
spark = SparkSession.builder.master("local").appName("linear_regression_model").getOrCreate()
real_estate = spark.read.option("inferSchema", "true").option("header","true").csv("C://sparkeg//BostonHousing.csv",header=True)
real_estate.printSchema()
real_estate.show(2)
inputColumns =real_estate.columns
inputColumns.remove("medv")
print(inputColumns)

from pyspark.ml.feature import VectorAssembler

assembler = VectorAssembler(inputCols=inputColumns,outputCol='features')

data_set = assembler.transform(real_estate)
data_set.select(['features','medv']).show(2)
train_data,test_data = data_set.randomSplit([0.7,0.3])

from pyspark.ml.regression import DecisionTreeRegressor

lr = DecisionTreeRegressor(labelCol='medv')
lrModel = lr.fit(train_data)
predictions = lrModel.transform(test_data)
from pyspark.ml.evaluation import RegressionEvaluator

# Select example rows to display.
predictions.select("prediction", "medv", "features").show(5)

# Select (prediction, true label) and compute test error
evaluator = RegressionEvaluator(
    labelCol="medv", predictionCol="prediction", metricName="rmse")
rmse = evaluator.evaluate(predictions)
print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)

print(lrModel.featureImportances.toArray())
print(inputColumns)