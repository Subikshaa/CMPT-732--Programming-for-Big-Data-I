from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types,Row
from pyspark.sql.functions import count,sum,lit
from math import sqrt
import datetime
import sys
import re
import operator
import uuid
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

def words_split(line):
    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
    split_tup = line_re.split(line)
    return split_tup

def get_data(line):
    return (str(uuid.uuid4()),line[1],line[2],line[3],int(line[4]))

def get_row_data(tup):
    row = Row(id=tup[0],host=tup[1],datetime=datetime.datetime.strptime(tup[2],"%d/%b/%Y:%H:%M:%S").strftime("%Y-%m-%d %H:%M:%S"),path=tup[3],bytes=tup[4])
    return row

def main(input_dir,keyspace,tab_name):
    text = sc.textFile(input_dir).repartition(80)
    reg_split_words = text.map(words_split).filter(lambda x: len(x) == 6).map(get_data)
    convert_row = reg_split_words.map(get_row_data)
    cluster_seeds = ['199.60.17.188', '199.60.17.216']
    spark = SparkSession.builder.appName('load logs spark').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    df = spark.createDataFrame(convert_row)
    df.write.format("org.apache.spark.sql.cassandra").options(table=tab_name, keyspace=keyspace).save()

if __name__ == '__main__':
    conf = SparkConf().setAppName('load logs spark')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    input_dir = sys.argv[1]
    keyspace = sys.argv[2]
    tab_name = sys.argv[3]
    main(input_dir,keyspace,tab_name)
