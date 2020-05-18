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

def main(keyspace,tab_name):
    cluster_seeds = ['199.60.17.188', '199.60.17.216']
    spark = SparkSession.builder.appName('correlate logs cassandra').config('spark.cassandra.connection.host', ','.join(cluster_seeds)).getOrCreate()
    web_df = spark.read.format("org.apache.spark.sql.cassandra").options(table=tab_name, keyspace=keyspace).load()
    df_hname_xy = web_df.groupBy(web_df.host).agg(count(web_df.bytes).alias("x"),sum(web_df.bytes).alias("y"))
    values = df_hname_xy.withColumn("1",lit(1)).withColumn("x^2",df_hname_xy.x ** 2).withColumn("y^2",df_hname_xy.y ** 2).withColumn("xy",df_hname_xy.x * df_hname_xy.y).drop(df_hname_xy.host)
    sum_values = values.groupBy().sum()
    df_values = sum_values.collect()
    numer = (df_values[0][2] * df_values[0][5]) - (df_values[0][0] * df_values[0][1])
    denom1 = sqrt((df_values[0][2] * df_values[0][3]) - (df_values[0][0]**2))
    denom2 = sqrt((df_values[0][2] * df_values[0][4]) - (df_values[0][1]**2))
    denom = denom1 * denom2
    r = numer / denom
    rsq = r*r
    print("-------------------------------------------")
    print("r = "+str(r))
    print("-------------------------------------------")
    print("r^2 = "+str(rsq))
    print("-------------------------------------------")


if __name__ == '__main__':
    keyspace = sys.argv[1]
    tab_name = sys.argv[2]
    main(keyspace,tab_name)
