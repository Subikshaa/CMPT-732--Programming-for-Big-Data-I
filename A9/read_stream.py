import sys
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession, functions
from kafka import KafkaConsumer

conf = SparkConf().setAppName('read stream')
sc = SparkContext(conf=conf)
assert sc.version >= '2.3'
topic = sys.argv[1]
spark = SparkSession(sc)
spark.sparkContext.setLogLevel('WARN')
messages = spark.readStream.format('kafka').option('kafka.bootstrap.servers', '199.60.17.210:9092,199.60.17.193:9092').option('subscribe', topic).load()
values = messages.select(messages['value'].cast('string'))
split_col = functions.split(values['value'], ' ')
values = values.withColumn('x',split_col.getItem(0)).withColumn('y',split_col.getItem(1))
values= values.withColumn("1",functions.lit(1)).withColumn('xy',values['x'] * values['y']).withColumn('xsq',values['x'] ** 2).drop(values['value'])
values.createOrReplaceTempView("xytable")
sum_val = spark.sql("SELECT SUM(1) as n, SUM(x) as sumx, SUM(y) as sumy, SUM(xy) as sumxy, SUM(xsq) as sumxsq from xytable")
betanum = sum_val['sumxy'] - ((1/sum_val['n'])*sum_val['sumx']*sum_val['sumy'])
betadenom = sum_val['sumxsq'] - ((1/sum_val['n'])*(sum_val['sumx']**2))
beta = betanum / betadenom
alpha = (sum_val['sumy']/sum_val['n'])-(beta * (sum_val['sumx'] / sum_val['n']))
beta_alpha = sum_val.withColumn('beta',beta).withColumn('alpha',alpha)
final_df = beta_alpha.select(beta_alpha['beta'],beta_alpha['alpha'])
stream = final_df.writeStream.format('console').outputMode('update').start()
stream.awaitTermination(400)
