from pyspark import SparkConf, SparkContext
import re
conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)
file = sc.textFile("hdfs://localhost:8020/user/cloudera/input0/all-bible.txt")
counts = file.flatMap(lambda line: line.lower().split(" ")) \
             .map(lambda word: re.compile("[^\w]").sub("",word)) \
             .map(lambda word: (word,1)) \
             .reduceByKey(lambda a,b: a + b)
counts.saveAsTextFile("hdfs://localhost:8020/user/cloudera/all-bible_wc")
 



 
