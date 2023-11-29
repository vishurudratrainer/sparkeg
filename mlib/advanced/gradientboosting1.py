# -*- coding: utf-8 -*-
"""
Created on Sun Dec 12 14:33:42 2021

@author: ASUS
"""

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
df = spark.read.csv("C://sparkeg//iris.csv", inferSchema=True, header=True).toDF(*["id","sep_len", "sep_wid", "pet_len", "pet_wid","label"])
df=df.drop('id')
df.show(5)


from pyspark.ml.linalg import Vectors
from pyspark.ml.feature import VectorAssembler
# transformer
vector_assembler = VectorAssembler(inputCols=["sep_len", "sep_wid", "pet_len", "pet_wid"],outputCol="features")
df_temp = vector_assembler.transform(df)
df_temp.show(3)
# drop the original data features column
df = df_temp.drop('sep_len', 'sep_wid', 'pet_len', 'pet_wid')
df.show(3)
from pyspark.ml.feature import StringIndexer
# estimator
l_indexer = StringIndexer(inputCol="label", outputCol="labelIndex")
df = l_indexer.fit(df).transform(df)
df.show(3)
# data splitting
(training,testing) = df.randomSplit([0.7,0.3])

from pyspark.ml.evaluation import MulticlassClassificationEvaluator

from pyspark.ml.classification import GBTClassifier,OneVsRest
# train our model using training data
rf=GBTClassifier(labelCol="labelIndex",featuresCol="features")
multi = OneVsRest(classifier=rf,labelCol="labelIndex",featuresCol="features")

# train the multiclass model.
model = multi.fit(training)
# test our model and make predictions using testing data
predictions = model.transform(testing)
predictions.select("prediction", "labelIndex").show(5)
# evaluate the performance of the classifier
evaluator = MulticlassClassificationEvaluator(labelCol="labelIndex",predictionCol="prediction", metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print("Test Error = %g" % (1.0 - accuracy))
print("Accuracy = %g " % accuracy)






