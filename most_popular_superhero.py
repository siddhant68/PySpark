from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("PopularMovies")
sc = SparkContext(conf = conf)

def countCoOccurences(line):
    elements = line.split()
    return (int(elements[0]), len(elements)-1)

def parseNames(line):
    fields = line.split('\"')
    return (int(fields[0]), fields[1].encode('utf8'))

names = sc.textFile('/home/sidhandsome/Coder_X/PySpark/SparkCourse/Marvel-names.txt')
namesRDD = names.map(parseNames)

lines = sc.textFile('/home/sidhandsome/Coder_X/PySpark/SparkCourse/Marvel-graph.txt')
pairings = lines.map(countCoOccurences)

totalFriendsByCharacter = pairings.reduceByKey(lambda x, y: x+y)
flipped = totalFriendsByCharacter.map(lambda (x, y): (y, x))

mostPopular = flipped.max()

mostPopularName = namesRDD.lookup(mostPopular[1])[0]

print(mostPopularName + ' ' + str(mostPopular[0]))


        
    