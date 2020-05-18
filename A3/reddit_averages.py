from pyspark import SparkConf, SparkContext
import sys
import operator
import json
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

def get_values(w):
    subreddit_value=w["subreddit"]
    score_value=w["score"]
    return subreddit_value,(1,score_value)

def add_pairs(w1,w2):
    count = w1[0] + w2[0]
    score = w1[1] + w2[1]
    return count,score

def avg(w):
    average = w[1][1] / w[1][0]
    return w[0],average

def get_key(w):
    return w[0]

def main(inputs, output):
    text = sc.textFile(inputs)
    json_value = text.map(json.loads).map(get_values)
    pair_values = json_value.reduceByKey(add_pairs).coalesce(1)
    outdata = pair_values.sortBy(get_key).map(avg).map(json.dumps)
    outdata.saveAsTextFile(output)


if __name__ == '__main__':
    conf = SparkConf().setAppName('example code')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
