from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions, types,Row
from pyspark.sql.functions import count,sum,lit
from math import sqrt
import sys
import re
import operator
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

def words_split(line):
    line_re = re.compile(r'^(\S+) - - \[(\S+) [+-]\d+\] \"[A-Z]+ (\S+) HTTP/\d\.\d\" \d+ (\d+)$')
    split_tup = line_re.split(line)
    return split_tup

def get_data(line):
    return (line[1],int(line[4]))

def get_row_data(tup):
    row = Row(host_name=tup[0],num_bytes_req=tup[1])
    return row

def main(inputs):
    text = sc.textFile(inputs)
    reg_split_words = text.map(words_split).filter(lambda x: len(x) == 6).map(get_data)
    convert_row = reg_split_words.map(get_row_data)
    spark = SparkSession(sc)
    web_df = spark.createDataFrame(convert_row)
    df_hname_xy = web_df.groupBy(web_df.host_name).agg(count(web_df.num_bytes_req).alias("x"),sum(web_df.num_bytes_req).alias("y"))
    values = df_hname_xy.withColumn("1",lit(1)).withColumn("x^2",df_hname_xy.x ** 2).withColumn("y^2",df_hname_xy.y ** 2).withColumn("xy",df_hname_xy.x * df_hname_xy.y).drop(df_hname_xy.host_name)#.withColumn("x",df_hname_xy.x).withColumn("x^2",df_hname_xy.x ** 2).withColumn("y",df_hname_xy.y).withColumn("y^2",df_hname_xy.y ** 2).withColumn("xy",df_hname_xy.x * df_hname_xy.y).show()
    sum_values = values.groupBy().sum()
    df_values = sum_values.collect()
    numer = (df_values[0][2] * df_values[0][5]) - (df_values[0][0] * df_values[0][1])
    denom1 = sqrt((df_values[0][2] * df_values[0][3]) - (df_values[0][0]**2))
    denom2 = sqrt((df_values[0][2] * df_values[0][4]) - (df_values[0][1]**2))
    denom = denom1 * denom2
    r = numer / denom
    rsq = r*r
    sum_values.show()
    print("Six values for debugging")
    print("-------------------------------------------")
    print("r = "+str(r))
    print("-------------------------------------------")
    print("r^2 = "+str(rsq))
    print("-------------------------------------------")


if __name__ == '__main__':
    conf = SparkConf().setAppName('correlate logs')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    main(inputs)
