# -*- coding: utf-8 -*-
"""
Created on Sun Dec 12 13:50:38 2021

@author: ASUS
"""

# import libraries
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col
# instantiate spark environment
sc = SparkContext("local[*]", "Decision Tree Classifier")
spark = SparkSession(sc)
# load the dataset
df = spark.read.csv("C://files//BreastTissue.csv", inferSchema=True, header=True).toDF('label','I0','PA500','HFS','DA','Area','A/DA','Max IP','DR','P'
)
df.show(5)


from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
# transformer
vector_assembler = VectorAssembler(inputCols=['I0','PA500','HFS','DA','Area','A/DA','Max IP','DR','P'
],outputCol="features")
df_temp = vector_assembler.transform(df)
df_temp.show(3)
# drop the original data features column
df = df_temp.drop('I0','PA500','HFS','DA','Area','A/DA','Max IP','DR','P'
)
df.show(3)
from pyspark.ml.feature import StringIndexer
# estimator
l_indexer = StringIndexer(inputCol="label", outputCol="labelIndex")
df = l_indexer.fit(df).transform(df)
df.show(3)
# data splitting
(training,testing) = df.randomSplit([0.7,0.3])

from pyspark.ml.classification import DecisionTreeClassifier
# train our model using training data
dt = DecisionTreeClassifier(labelCol="labelIndex", featuresCol="features")
model = dt.fit(training)
# test our model and make predictions using testing data
predictions = model.transform(testing)
predictions.select("prediction", "labelIndex").show(5)

from pyspark.ml.evaluation import MulticlassClassificationEvaluator
evaluator = MulticlassClassificationEvaluator(labelCol="labelIndex", predictionCol="prediction",metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test Error = %g " % (1.0 - accuracy))
print("Accuracy = %g " % accuracy)


sc.stop() 





