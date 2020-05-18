from pyspark import SparkConf, SparkContext
import sys
import operator
import json
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

def get_values(comment):
    subreddit_val = comment["subreddit"]
    score_val = comment["score"]
    return subreddit_val,(1,score_val)

def add_pairs(w1,w2):
    count = w1[0] + w2[0]
    score = w1[1] + w2[1]
    return count,score

def avg(w):
    average = w[1][1] / w[1][0]
    if average > 0:
        return w[0],average

def ret_values(w):
    best_comment_score = w[1][0]["score"] / w[1][1]
    author_val = w[1][0]["author"]
    return best_comment_score,author_val

def get_key(w):
    return w[0]

def main(inputs, output):
    text = sc.textFile(inputs)
    commentdata = text.map(json.loads).cache()
    average = commentdata.map(get_values).reduceByKey(add_pairs).map(avg)
    commentbysub = commentdata.map(lambda c: (c['subreddit'], c)).join(average)
    outdata = commentbysub.map(ret_values).sortBy(get_key,ascending=False)
    outdata.saveAsTextFile(output)

if __name__ == '__main__':
    conf = SparkConf().setAppName('relative score')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
