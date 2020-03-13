from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster('local').setAppName('temperatures')
sc = SparkContext(conf=conf)

def parseLines(line):
    fields = line.split(',')
    return (fields[0], fields[2], float(fields[3]))

lines = sc.textFile('/home/sidhandsome/Coder_X/PySpark/SparkCourse/1800.csv')
rdd = lines.map(parseLines)
filtered_rdd = rdd.filter(lambda x: x[1] == 'TMIN')
id_temp_rdd = filtered_rdd.map(lambda x: (x[0], x[2]))
min_temp = id_temp_rdd.reduceByKey(lambda x, y: min(x, y))

results = min_temp.collect()
for i in results:
    print(i)