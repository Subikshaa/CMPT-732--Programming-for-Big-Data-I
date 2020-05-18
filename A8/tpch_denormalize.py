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

def main(ipkeyspace, opkeyspace):
    cluster_seeds = ['199.60.17.188', '199.60.17.216']
    spark = SparkSession.builder.appName('tpch denormalize').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    part_df = spark.read.format("org.apache.spark.sql.cassandra").options(table="part", keyspace=ipkeyspace).load()
    lineitem_df = spark.read.format("org.apache.spark.sql.cassandra").options(table="lineitem", keyspace=ipkeyspace).load()
    orders_df = spark.read.format("org.apache.spark.sql.cassandra").options(table="orders", keyspace=ipkeyspace).load().cache()
    join1_df = orders_df.join(lineitem_df, "orderkey")
    join2_df = join1_df.join(part_df, "partkey")
    agg_new_df = join2_df.groupBy(join2_df['orderkey'],join2_df['totalprice']).agg(functions.collect_set(join2_df['name']).alias('part_names')).drop('totalprice')
    final_df = agg_new_df.join(orders_df,"orderkey")
    final_df.write.format("org.apache.spark.sql.cassandra").options(table="orders_parts", keyspace=opkeyspace).save()

if __name__ == '__main__':
    ipkeyspace = sys.argv[1]
    opkeyspace = sys.argv[2]
    main(ipkeyspace,opkeyspace)
