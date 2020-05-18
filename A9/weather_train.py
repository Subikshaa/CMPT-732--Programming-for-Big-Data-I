import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('Weather train').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
spark.sparkContext.setLogLevel('WARN')


from pyspark.ml.feature import VectorAssembler,SQLTransformer
from pyspark.ml.regression import GBTRegressor
from pyspark.ml.pipeline import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator


def main(input,model_file):
    tmax_schema = types.StructType([
    types.StructField('station', types.StringType()),
    types.StructField('date', types.DateType()),
    types.StructField('latitude', types.FloatType()),
    types.StructField('longitude', types.FloatType()),
    types.StructField('elevation', types.FloatType()),
    types.StructField('tmax', types.FloatType()),
    ])
    data = spark.read.csv(input, schema=tmax_schema)
    train, validation = data.randomSplit([0.75, 0.25],seed = 123)
    train = train.cache()
    validation = validation.cache()
    y_tmax = SQLTransformer(statement="SELECT today.station,today.latitude,today.longitude,today.elevation,today.date,today.tmax,yesterday.tmax AS yesterday_tmax FROM __THIS__ as today INNER JOIN __THIS__ as yesterday ON date_sub(today.date, 1) = yesterday.date AND today.station = yesterday.station")
    getvalues = SQLTransformer(statement="SELECT station,latitude,longitude,elevation,dayofyear(date) AS dayofyear,tmax,yesterday_tmax from __THIS__")

    assemble_features = VectorAssembler(
        inputCols=['latitude', 'longitude', 'elevation','dayofyear','yesterday_tmax'],
        outputCol='features')
    classifier = GBTRegressor(
        featuresCol='features', labelCol='tmax')
    pipeline = Pipeline(stages=[y_tmax,getvalues,assemble_features, classifier])

    model = pipeline.fit(train)
    predictions = model.transform(validation)

    r2_evaluator = RegressionEvaluator(
        predictionCol='prediction', labelCol='tmax',
        metricName='r2')
    r2 = r2_evaluator.evaluate(predictions)
    print('-----------------------------------')
    print('r2: %g' % (r2, ))
    print('-----------------------------------')
    rmse_evaluator = RegressionEvaluator(
        predictionCol='prediction', labelCol='tmax',
        metricName='rmse')
    rmse = rmse_evaluator.evaluate(predictions)
    print('rmse: %g' % (rmse, ))
    model.write().overwrite().save(model_file)


if __name__ == '__main__':
    input = sys.argv[1]
    output = sys.argv[2]
    main(input,output)
