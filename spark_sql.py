from pyspark.sql import SparkSession
from pyspark.sql import Row

import collections

spark = SparkSession.builder.appName('SparkSQL').getOrCreate()

def mapper(line):
    fields = line.split(',')
    return Row(Id=int(fields[0]), name=fields[1].encode('utf-8'),
               age=int(fields[2]), numFriends=int(fields[3]))

lines = spark.sparkContext.textFile('/home/sidhandsome/Coder_X/PySpark/SparkCourse/fakefriends.csv')
people = lines.map(mapper)

schemaPeople = spark.createDataFrame(people).cache()
schemaPeople.createOrReplaceTempView('people')

teenagers = spark.sql('SELECT * FROM people WHERE age >= 13 and age <=19')
teenagers1 = spark.sql('SELECT age, count(*) FROM people GROUP BY age HAVING count(*) > (SELECT avg(count) FROM (SELECT count(*) as count FROM people GROUP BY age))')
averageAge = spark.sql('SELECT avg(count) FROM (SELECT count(*) as count FROM people GROUP BY age)')

for teen in teenagers1.collect():
    print(teen)

for i in averageAge.collect():
    print(i)
    
schemaPeople.groupBy('age').count().orderBy('age').show()
