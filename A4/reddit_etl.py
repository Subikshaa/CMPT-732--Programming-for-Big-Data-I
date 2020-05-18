from pyspark import SparkConf, SparkContext
import sys
import operator
import json
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

def get_values(comment):
    subreddit_val=comment["subreddit"]
    score_val=comment["score"]
    author_val=comment["author"]
    return subreddit_val,score_val,author_val

def filter_data(v):
    if "e" in v[0]:
        return True
    else:
        return False

def get_positive(v):
    if v[1] > 0:
        return True
    else:
        return False

def get_negative(v):
    if v[1] <= 0:
        return True
    else:
        return False

def main(inputs, output):
    text = sc.textFile(inputs)
    json_data = text.map(json.loads).map(get_values).filter(filter_data).cache()
    json_data.filter(get_positive).map(json.dumps).saveAsTextFile(output + '/positive')
    json_data.filter(get_negative).map(json.dumps).saveAsTextFile(output + '/negative')

if __name__ == '__main__':
    conf = SparkConf().setAppName('reddit etl')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
