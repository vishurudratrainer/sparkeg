# -*- coding: utf-8 -*-
"""
Created on Sun Dec 12 13:50:38 2021

@author: ASUS
"""

# import libraries
from pyspark import SparkContext
from pyspark.sql.session import SparkSession
# instantiate spark environment
sc = SparkContext("local[*]", "Decision Tree Classifier")
spark = SparkSession(sc)
# load the dataset
df = spark.read.csv("C://files//breast-cancer.data", inferSchema=True, header=False).toDF('label','age','m','t','in','nc','dm','b','bg','i') 
df.show(5)
df.printSchema()
from pyspark.ml.feature import StringIndexer
stringcol=['label','age','m','t','in','nc','b','bg','i']
outputcol=['labeli','agei','mi','ti','ini','nci','bi','bgi','ii']
l_indexer = StringIndexer(inputCols=stringcol, outputCols=outputcol)
df = l_indexer.fit(df).transform(df)
df = df.drop('label','age','m','t','in','nc','b','bg','i')
from pyspark.ml.feature import VectorAssembler
# transformer
vector_assembler = VectorAssembler(inputCols=['agei','mi','ti','ini','nci','dm','bi','bgi','ii'],outputCol="features")
df_temp = vector_assembler.transform(df)
df_temp.show(3)
# drop the original data features column
df = df_temp.drop('agei','mi','ti','ini','nci','dm','bi','bgi','ii')
df.show(3)


# data splitting
(training,testing) = df.randomSplit([0.7,0.3])

from pyspark.ml.classification import DecisionTreeClassifier
# train our model using training data
dt = DecisionTreeClassifier(labelCol="labeli", featuresCol="features")
model = dt.fit(training)
# test our model and make predictions using testing data
predictions = model.transform(testing)
predictions.select("prediction", "labeli").show(5)

from pyspark.ml.evaluation import MulticlassClassificationEvaluator
evaluator = MulticlassClassificationEvaluator(labelCol="labeli", predictionCol="prediction",metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test Error = %g " % (1.0 - accuracy))
print("Accuracy = %g " % accuracy)


sc.stop() 





