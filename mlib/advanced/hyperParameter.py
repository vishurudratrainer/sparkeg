# -*- coding: utf-8 -*-
"""
Created on Thu Oct 12 19:59:56 2023

@author: ASUS
"""

from pyspark.ml.classification import DecisionTreeClassifier
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.mllib.evaluation import BinaryClassificationMetrics
#from mmlspark import ComputeModelStatistics

# Create initial Decision Tree Model
dt = DecisionTreeClassifier(labelCol="label", featuresCol="features", maxDepth=2)

# Create ParamGrid for Cross Validation
dtparamGrid = (ParamGridBuilder()
             .addGrid(dt.maxDepth, [2, 5, 10, 20, 30])
             .addGrid(dt.maxBins, [10, 20, 40, 80, 100])
             .build())

# Evaluate model
dtevaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction")

# Create 5-fold CrossValidator
dtcv = CrossValidator(estimator = dt,
                      estimatorParamMaps = dtparamGrid,
                      evaluator = dtevaluator,
                      numFolds = 5)



# Run cross validations
dtcvModel = dtcv.fit(train)
print(dtcvModel)

# Use test set here so we can measure the accuracy of our model on new data
dtpredictions = dtcvModel.transform(test)

# cvModel uses the best model found from the Cross Validation
# Evaluate best model
print('Accuracy:', dtevaluator.evaluate(dtpredictions))
print('AUC:', BinaryClassificationMetrics(dtpredictions['label','prediction'].rdd).areaUnderROC)