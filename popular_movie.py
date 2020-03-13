# BroadCast Concept
from pyspark import SparkConf, SparkContext

def loadMovieNames():
    movieNames = {}
    with open("/home/sidhandsome/Coder_X/PySpark/ml-100k/u.item") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
    return movieNames

conf = SparkConf().setMaster("local").setAppName("PopularMovies")
sc = SparkContext(conf = conf)

nameDict = sc.broadcast(loadMovieNames())

lines = sc.textFile("/home/sidhandsome/Coder_X/PySpark/ml-100k/u.data")
movies = lines.map(lambda x: (int(x.split()[1]), 1))
moviesCounts = movies.reduceByKey(lambda x, y: x+y)

flipped = moviesCounts.map(lambda x: (x[1], x[0]))
sortedMovies = flipped.sortByKey()

sortedMoviesWithNames = sortedMovies.map(lambda (count, movieId)
                                         : (nameDict.value[movieId], count))

results = sortedMoviesWithNames.collect()

for result in results:
    print(result)
