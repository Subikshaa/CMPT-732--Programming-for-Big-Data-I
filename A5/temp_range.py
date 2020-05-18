import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import max,min,asc
#from pyspark.sql.functions import inputFileName

spark = SparkSession.builder.appName('temp range').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
# sc = spark.sparkContext

# add more functions as necessary


def main(inputs, output):
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
    filter_observation = filter_qflag.filter(filter_qflag.observation.isin('TMAX','TMIN'))
    filter_values = filter_observation.groupBy(filter_observation.date,filter_observation.station).agg(max(filter_observation.value).alias("max_value"),min(filter_observation.value).alias("min_value"))
    find_range = filter_values.withColumn("range",filter_values.max_value - filter_values.min_value).cache()
    find_maxrange = find_range.groupBy(find_range.date).agg(max(find_range.range).alias("max_range"))
    weather1 = find_range.join(find_maxrange,"date").where(find_range.range == find_maxrange.max_range)
    final_range = weather1.withColumn("range",weather1.max_range / 10)#.sort(weather1.date.asc,weather1.station.asc)
    sort_final_range = final_range.orderBy(final_range.date,final_range.station)
    tem_range = sort_final_range.select(sort_final_range.date,sort_final_range.station,sort_final_range.range)
    tem_range.write.csv(output, mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
