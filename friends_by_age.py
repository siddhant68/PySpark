from pyspark import SparkConf, SparkContext
import collections

def parseLine(line):
    return (int(line.split(',')[2]), int(line.split(',')[3]))

conf = SparkConf().setMaster('local').setAppName('SocialNetwork')
sc = SparkContext(conf = conf)

lines = sc.textFile('/home/sidhandsome/Coder_X/PySpark/SparkCourse/fakefriends.csv')
rdd = lines.map(parseLine)
total_by_age = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y:
                                     (x[0]+y[0], x[1]+y[1]))

averages_by_age = total_by_age.mapValues(lambda x: x[0]/x[1])
result = averages_by_age.collect()
for i in result:
    print(i)