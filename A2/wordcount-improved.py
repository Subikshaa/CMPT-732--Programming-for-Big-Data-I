from pyspark import SparkConf, SparkContext
import sys
import operator
import re, string

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('wordcount improved')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+

def words_once(line):
    wordsep = re.compile(r'[%s\s]+' % re.escape(string.punctuation))
    for w in wordsep.split(line):		#lines are separated based on the pattern
        if(len(w)>0):				#spaces are removed
            yield (w.lower(), 1)		#key is converted to lower case

def get_key(kv):
    return kv[0]

def output_format(kv):
    k, v = kv
    return '%s %i' % (k, v)

text = sc.textFile(inputs)
words = text.flatMap(words_once)
wordcount = words.reduceByKey(operator.add)

outdata = wordcount.sortBy(get_key).map(output_format)
outdata.saveAsTextFile(output)
