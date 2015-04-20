from pyspark import SparkConf, SparkContext
import re

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)

file = sc.textFile("file:///home/cloudera/Downloads/all-bible.txt")

'''use glom() to create a list that join all lines from the document
  and split the entire document by terminate punctuation'''
sentence = file.glom() \
               .map(lambda line: " ".join(line)) \
               .flatMap(lambda doc: doc.split("."))

'''for each sentence, separate the words apart
   change to lowercase and remove punctuations'''
word = sentence.map(lambda w: re.compile("[^\w ]").sub("",w)) \
               .map(lambda s: s.lower().split())

'''create pairs and map'''
pair = word.flatMap(lambda x: [((x[i],x[i+1]),1) for i in range(0,len(x)-1)])

'''Reduce; sort the final count in descending order so that most frequent pairs on the top'''
count = pair.reduceByKey(lambda a, b: a+b) \
            .map(lambda x: (x[1],x[0])) \
            .sortByKey(False)


'''output to local file'''
count.saveAsTextFile("file:///home/cloudera/all-bible_bigram_cnt")




