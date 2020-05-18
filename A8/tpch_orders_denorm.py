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

def output_line(tup):
    namestr = ', '.join(sorted(list(tup[2])))
    return 'Order #%d $%.2f: %s' % (tup[0], tup[1], namestr)

def main(keyspace,outdir,orderkeys):
    cluster_seeds = ['199.60.17.188', '199.60.17.216']
    spark = SparkSession.builder.appName('tpch orders denorm').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    df = spark.read.format("org.apache.spark.sql.cassandra").options(table="orders_parts", keyspace=keyspace).load()
    filtered_df = df.select(df.orderkey,df.totalprice,df.part_names).filter(df.orderkey.isin(orderkeys)).sort(df['orderkey'])
    final_rdd = filtered_df.rdd.map(output_line)
    final_rdd.saveAsTextFile(outdir)

if __name__ == '__main__':
    keyspace = sys.argv[1]
    outdir = sys.argv[2]
    orderkeys = sys.argv[3:]
    main(keyspace,outdir,orderkeys)
