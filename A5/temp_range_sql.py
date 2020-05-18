import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import max,min,asc
#from pyspark.sql.functions import inputFileName

spark = SparkSession.builder.appName('temp range sql').getOrCreate()
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

    weather1 = spark.read.csv(inputs, schema=observation_schema)
    weather1.createOrReplaceTempView("weather")
    weatherdf = spark.sql("SELECT * from weather where qflag IS null AND observation IN ('TMAX','TMIN')")
    weatherdf.createOrReplaceTempView("weathertemp")
    max_temp = spark.sql("select date,station,MAX(value) AS max_value,MIN(value) AS min_value from weathertemp GROUP BY date,station")
    max_temp.createOrReplaceTempView("maxtemp")
    find_range = spark.sql("select date,station,max_value - min_value AS range from maxtemp")
    find_range.createOrReplaceTempView("rangetemp")
    max_temp_date = spark.sql("select date,MAX(range) AS max_range from rangetemp GROUP BY date")
    max_temp_date.createOrReplaceTempView("maxrange")
    final_range = spark.sql("select rangetemp.date,rangetemp.station,rangetemp.range from rangetemp JOIN maxrange ON rangetemp.date = maxrange.date WHERE rangetemp.range = maxrange.max_range")
    final_range.createOrReplaceTempView("weather_final")
    tem_range = spark.sql("select date,station,range/10 as range from weather_final ORDER BY date,station")
    tem_range.write.csv(output, mode='overwrite')

if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
