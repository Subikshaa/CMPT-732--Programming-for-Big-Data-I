from pyspark import SparkConf, SparkContext
import sys
import operator
import re, string

inputs = sys.argv[1]
output = sys.argv[2]

conf = SparkConf().setAppName('Wikipedia Popular')
sc = SparkContext(conf=conf)
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+
assert sc.version >= '2.3'  # make sure we have Spark 2.3+

def words_once(line):
    w = line.split(" ")		#line is split
    w = tuple(w)		#tuple of words is created
    yield w

def get_key(kv):
    return kv[0]

def tab_separated(kv):
    return "%s\t%s" % (kv[0], kv[1])

def convert_to_int(w):
    w=list(w)			#since tuples are immutabale,it is to converted to list
    w[3] = int(w[3])		#count is converted to int
    w=tuple(w)
    return w

def filter_data(w):
    title=w[2]
    if(w[1]=="en" and title != "Main_Page" and not(title.startswith("Special:"))):		#filtering is done
        return True

def convert_key_value_tuple(w):
    key=w[0]
    val=(w[3],w[2])
    return (key,val)		#key,tuple of values are returned


text = sc.textFile(inputs)
words = text.flatMap(words_once).map(convert_to_int).filter(filter_data)
a=words.map(convert_key_value_tuple)
wordcount = a.reduceByKey(max)		#max is used to find the max of the count.Here max value is found based on the first value of the tuple (i.e.)count.A function can also be created to find the max of the count values
max_count = wordcount.sortBy(get_key)
max_count.map(tab_separated).saveAsTextFile(output)
