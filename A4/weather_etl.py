import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
spark = SparkSession.builder.appName('weather etl').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
# sc = spark.sparkContext

# add more functions as necessary

def main(inputs,output):
    # main logic starts here
    observation_schema = types.StructType([
    types.StructField('station', types.StringType(), False),
    types.StructField('date', types.StringType(), False),
    types.StructField('observation', types.StringType(), False),
    types.StructField('value', types.IntegerType(), False),
    types.StructField('mflag', types.StringType(), False),
    types.StructField('qflag', types.StringType(), False),
    types.StructField('sflag', types.StringType(), False),
    types.StructField('obstime', types.StringType(), False),
    ])
    weather = spark.read.csv(inputs, schema=observation_schema)
    filter_qflag = weather.filter(weather.qflag.isNull())
    filter_station = filter_qflag.filter(filter_qflag.station.startswith('CA'))
    filter_observation = filter_station.filter(filter_station.observation.isin('TMAX'))
    filter_value = filter_observation.withColumn('tmax',filter_observation.value / 10)
    etl_data = filter_value.select('station','date','tmax')
    etl_data.write.json(output, compression='gzip', mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs,output)
