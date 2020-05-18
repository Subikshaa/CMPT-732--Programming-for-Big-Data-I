from pyspark import SparkConf, SparkContext
import sys
import operator
import re, string
import random
#assert sys.version_info >= (3, 5) # make sure we have Python 3.5+

def find_iterations(v):
    random.seed()
    total_iterations = 0
    for i in range(v):
        sum = 0.0
        while(sum < 1.0):
            sum += random.uniform(0,1)
            total_iterations += 1
    return total_iterations


def main(inputs):
    val = sc.range(1,int(inputs),numSlices = 16).glom().map(len)
    iter = val.map(find_iterations)
    tot_iter = iter.reduce(operator.add)
    print(tot_iter/int(inputs))


if __name__ == '__main__':
    conf = SparkConf().setAppName('euler')
    sc = SparkContext(conf=conf)
#    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    main(inputs)
