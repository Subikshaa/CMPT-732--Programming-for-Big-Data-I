import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types,Row
spark = SparkSession.builder.appName('weather tomorrowr').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')
from datetime import datetime
from pyspark.ml import PipelineModel
from pyspark.ml.evaluation import RegressionEvaluator


tmax_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.DateType()),
    types.StructField('latitude', types.FloatType()),
    types.StructField('longitude', types.FloatType()),
    types.StructField('elevation', types.FloatType()),
    types.StructField('tmax', types.FloatType()),
])


def test_model(model_file):
    # get the data
    test_tmax = [('campus',datetime.strptime('2018-11-12',"%Y-%m-%d").date(),49.2771,-122.9146,330.0,12.0),('campus',datetime.strptime('2018-11-13',"%Y-%m-%d").date(),49.2771,-122.9146,330.0,0.0)]
    df = spark.createDataFrame(test_tmax, schema=tmax_schema)
    # load the model
    model = PipelineModel.load(model_file)

    # use the model to make predictions
    predictions = model.transform(df)
    #predictions.show()

    # evaluate the predictions

    rmse_evaluator = RegressionEvaluator(predictionCol='prediction', labelCol='tmax',
            metricName='rmse')
    rmse = rmse_evaluator.evaluate(predictions)
    pred = predictions.collect()
    print('----------------------------------------------')
    print('Predicted tmax tomorrow:')
    print(pred[0][8])
    print('----------------------------------------------')
    print('rmse =', rmse)

    # If you used a regressor that gives .featureImportances, maybe have a look...
    #print(model.stages[-1].featureImportances)


if __name__ == '__main__':
    model_file = sys.argv[1]
    test_model(model_file)
