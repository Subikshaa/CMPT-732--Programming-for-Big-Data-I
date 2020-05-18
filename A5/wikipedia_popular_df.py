import sys
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

from pyspark.sql import SparkSession, functions, types
from pyspark.sql.functions import max,asc
#from pyspark.sql.functions import inputFileName

spark = SparkSession.builder.appName('wikipedia popular df').getOrCreate()
assert spark.version >= '2.3' # make sure we have Spark 2.3+
# sc = spark.sparkContext

# add more functions as necessary
@functions.udf(returnType=types.StringType())
def path_to_hour(path):
    path_split = path.split("/")
    hour = path_split[-1][11:22]
    return hour


def main(inputs, output):
    # main logic starts here
    wikipedia_schema = types.StructType([ # commented-out fields won't be read
        types.StructField('language', types.StringType(), True),
        types.StructField('title', types.StringType(), True),
        types.StructField('views', types.LongType(), True),
        types.StructField('bytes_returned', types.LongType(), True),
    ])
    wiki = spark.read.csv(inputs, schema=wikipedia_schema,sep=" ").withColumn('Hour', path_to_hour(functions.input_file_name()))
    wiki_filtered = wiki.filter((wiki.language == "en") & (wiki.title != "Main_Page") & (~ wiki.title.like("Special:%"))).cache()
    max_count = wiki_filtered.groupBy("Hour").agg(max(wiki_filtered.views).alias("max_views"))
    find_max_count = wiki_filtered.join(functions.broadcast(max_count),"Hour").where(max_count.max_views == wiki_filtered.views)
    hour_sorted = find_max_count.sort("Hour")
    wiki_popular = hour_sorted.select(hour_sorted.Hour,hour_sorted.title,hour_sorted.views)
    wiki_popular.coalesce(1).write.json(output, mode='overwrite')
    # wiki_popular.explain()
if __name__ == '__main__':
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
