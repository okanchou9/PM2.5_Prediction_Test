#
# Script name: bank.py
# 
# Script propose: Using PySpark to analysis the data of Santander Bank Customer Satisfaction
# and try to predict which customers are happy customers(predict the TARGET value by history data)
# 
# Step:
#   1. Import lib spark-csv to support the csv format in PySpark
#   2. Load the training data from HDFS to PySpark and transform it to RDD
#   3. Analyze the data by the machine learning library in PySpark
#   4. Create the predict model for testing data and do the prediction
#   5. Pop out the result csv and exit the program
#   
# Script owner: Kenie Liu
# 
# Last modified date: 160315
#   

"""
Santander Bank Customer Satisfaction Practice
"""
from __future__ import print_function
import sys
from pyspark import SparkContext, SQLContext, SparkConf
from pyspark.sql.types import *
from pyspark.sql.functions import lit
from pyspark.ml import Pipeline
from pyspark.ml.regression import DecisionTreeRegressor
from pyspark.ml.feature import StringIndexer, VectorIndexer, VectorAssembler
from pyspark.ml.evaluation import RegressionEvaluator

if __name__ == "__main__":

    sc = SparkContext(appName="PM2.5_Prediction_Test")
    sqlContext = SQLContext(sc)

    segment = 108
    target = "t107"
    file = "train_2/output_pm25_108_HC.csv"
    output_file = "result"

    # Load the training data into spark
    data = sqlContext.read.format("com.databricks.spark.csv").options(header="true", inferschema="true").load(file)

    # Modify the data type of column "TARGET" from Integer to Double
    data = data.withColumn(target, data[target].cast(DoubleType()))

    # data_TAR_wo_0 = data.filter(data.TARGET > 0)

    # data_TAR_0_2000 = data.filter(data.TARGET == 0).limit(2000)

    # data_new = data_TAR_wo_0.unionAll(data_TAR_0_2000)

    # Output the schema of training data
    data.printSchema()

    print("Training data size is %d" %(data.count()))

    # Get the list of column name of training data
    columns = data.columns
    print(columns)

    # Remove the column "t47" since no need to add this column to predict
    columns.remove(columns[len(columns) - 1])
    print(columns)

    # Index labels, adding metadata to the label column
    # Fit on whole dataset to include all labels in index
    #targetIndexer = StringIndexer(inputCol="TARGET", outputCol="indexedLabel").fit(data)

    print("After labelIndexer~~~~~~~~~~~~~~~~")

    # Identify categorical features, and index them
    #featureIndexer = VectorIndexer(inputCol="TARGET", outputCol="indexedFeatures", maxCategories = 371).fit(data)
    assembler = VectorAssembler(inputCols = columns, outputCol = "indexedFeatures")

    print("After featureIndexer~~~~~~~~~~~~~~~")

    # Split the data into training and test sets (10% held out for testing)
    (trainingData, testData) = data.randomSplit([0.1, 0.9])

    print("After randomSplit~~~~~~~~~~~~~~~~")

    # Train a DecisionTreeRegressor model
    dtr = DecisionTreeRegressor(labelCol = target, featuresCol = "indexedFeatures")

    print("After DecisionTreeRegressor~~~~~")

    # Chain indexers and tree in a Pipeline
    pipeline = Pipeline(stages = [assembler, dtr])

    print("After pipeline~~~~~~~~~~~~~~~~~~~")

    # Train model.  This also runs the indexers
    model = pipeline.fit(data)

    print(model)

    print("After model fit~~~~~~~~~~~~~~~~~")

    # Make predictions.
    predictions = model.transform(testData)

    print("After predictions~~~~~~~~~~~~~~~")

    # Select example rows to display.
    predictions.select("prediction", "indexedFeatures").show(5)

    predictions.select(target, "prediction",).write.mode("overwrite").format("com.databricks.spark.csv").save(output_file)

    # Select (prediction, true label) and compute test error
    evaluator = RegressionEvaluator(predictionCol = "prediction", labelCol = target)
    
    # mse|rmse|r2|mae
    mse = evaluator.evaluate(predictions, {evaluator.metricName: "mse"})
    rmse = evaluator.evaluate(predictions, {evaluator.metricName: "rmse"})
    r2 = evaluator.evaluate(predictions, {evaluator.metricName: "r2"})
    mae = evaluator.evaluate(predictions, {evaluator.metricName: "mae"})

    print("mse:", mse, " rmse:", rmse, " r2:", r2, " mae:", mae)
    
    # # Load the testing data into spark
    # realData = sqlContext.read.format("com.databricks.spark.csv").options(header="true", inferschema="true").load("test/test_bank.csv")

    # # Add one empty column named "TARGET" to match the same schema between training/testing data
    # new_realData = realData.withColumn("TARGET", lit(0.0))

    # realPredictions = model.transform(new_realData)

    # realPredictions.select("ID", "prediction").show(5)

    # realPredictions.select("ID", "prediction").write.mode("overwrite").format("com.databricks.spark.csv").save("result")
    #realPredictions.select("ID", "prediction").write.mode("overwrite").format("com.databricks.spark.csv").save("file:/home/hduser/result_bank")

    # Select (prediction, true label) and compute test error
    #evaluator = MulticlassClassificationEvaluator(
    #    labelCol="indexedLabel", predictionCol="prediction", metricName="precision")
    #accuracy = evaluator.evaluate(predictions)
    #print("Test Error = %g " % (1.0 - accuracy))

    treeModel = model.stages[1]
    # summary only
    print(treeModel)

    sc.stop()
    

