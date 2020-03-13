import re
from pyspark import SparkConf, SparkContext
import collections

def normalizeWord(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

conf = SparkConf().setMaster('local').setAppName('temperatures')
sc = SparkContext(conf=conf)

book = sc.textFile('/home/sidhandsome/Coder_X/PySpark/SparkCourse/book.txt')
words = book.flatMap(normalizeWord)
wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
wordCountsSorted = wordCounts.map(lambda (x, y): (y, x)).sortByKey()
results = wordCountsSorted.collect()

for result in results:
    count = str(result[0])
    word = result[1].encode('ascii', 'ignore')
    if word:
        print(word + ':\t\t' + count)
