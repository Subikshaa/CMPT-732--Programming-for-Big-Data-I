from pyspark import SparkConf, SparkContext
import sys
import operator
import json
assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

def get_values(comment):
    subreddit = comment["subreddit"]
    score = comment["score"]
    return subreddit,(1,score)

def add_pairs(w1,w2):
    count = w1[0] + w2[0]
    score = w1[1] + w2[1]
    return count,score

def avg(w):
    average = w[1][1] / w[1][0]
    if average > 0:
        return w[0],average

def get_key(w):
    return w[0]

def get_best_score(averages,comments):
    subreddit_val = comments["subreddit"]
    score_val = comments["score"]
    author_val = comments["author"]
    best_comment_score = score_val / averages.value[subreddit_val]
    return best_comment_score,author_val

def main(inputs, output):
    text = sc.textFile(inputs)
    commentdata = text.map(json.loads).cache()
    averages = commentdata.map(get_values).reduceByKey(add_pairs).map(avg)
    avg_dict = dict(averages.collect())
    avg_broad = sc.broadcast(avg_dict)
    commentbysub = commentdata.map(lambda a: get_best_score(avg_broad,a))
    outdata = commentbysub.sortBy(get_key,ascending=False)
    outdata.saveAsTextFile(output)

if __name__ == '__main__':
    conf = SparkConf().setAppName('relative score bcast')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    output = sys.argv[2]
    main(inputs, output)
