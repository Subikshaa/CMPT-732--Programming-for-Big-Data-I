from pyspark import SparkConf, SparkContext
import sys
import operator
import re, string
assert sys.version_info >= (3, 5)  # make sure we have Python 3.5+

def words_once(line):
    w = line.split(":")
    for i in range(len(w[1])):
        if (w[1][i] != ' '):
            yield (int(w[0]),int(w[1][i]))

def convert_key_value_tuple(w):
    key=w[0]
    val=(w[3],w[2])
    return (key,val)

def cal_dist(l):
    return (l[1][0],(l[0],l[1][1][1]+1))

def find_min_dist(sd1,sd2):
    if (sd1[1]<=sd2[1]):
        return sd1
    else:
        return sd2


def main(inputs,output,sourc,dest):
    text = sc.textFile(inputs+'/links-simple-sorted.txt')
    source_dest = text.flatMap(words_once).cache()
    initial_dist = sc.parallelize([(sourc,(' ',0))])
    for i in range(6):
        comb_rdd = source_dest.join(initial_dist).map(cal_dist)
        initial_dist = initial_dist.union(comb_rdd).reduceByKey(find_min_dist)
        initial_dist.coalesce(1).saveAsTextFile(output+'/iter-'+str(i))
    fin_path = [dest]
    try:
        while (dest != ' ' and dest!=sourc):
            prev_lookup = initial_dist.lookup(dest)
            if prev_lookup != ' ':
                dest = prev_lookup[0][0]
                fin_path.append(dest)
    except:
        print ("\n\n\nPath between "+str(sourc)+" and "+str(dest)+" does not exist.\n\n\n")
        sys.exit()
    fin_path = fin_path[::-1]  #reverses the list
    fin_path_rdd = sc.parallelize(fin_path)
    fin_path_rdd.coalesce(1).saveAsTextFile(output+'/path')


if __name__ == '__main__':
    conf = SparkConf().setAppName('Shortest Path')
    sc = SparkContext(conf=conf)
    assert sc.version >= '2.3'  # make sure we have Spark 2.3+
    inputs = sys.argv[1]
    output = sys.argv[2]
    sourc = sys.argv[3]
    dest = sys.argv[4]
    main(inputs,output,int(sourc),int(dest))
