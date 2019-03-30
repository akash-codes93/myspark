from pyspark import SparkConf, SparkContext


def loadMoviesNames():
    movieNames = {}

    with open("data/ml-100k/u.item") as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]

    return movieNames


conf = SparkConf().setMaster("local").setAppName("broadcastVariables")
sc = SparkContext(conf=conf)

nameDict = sc.broadcast(loadMoviesNames())

lines = sc.textFile("data/ml-100k/u.data")
movies = lines.map(lambda x: (int(x.split()[1]), 1))

movieCounts = movies.reduceByKey(lambda x, y: x + y)

flipped = movieCounts.map(lambda x:(x[1], x[0]))
sortedMovies = flipped.sortByKey()

sortedMoviesNames = sortedMovies.map(lambda a: (nameDict.value[a[1], a[0]]))
results = sortedMoviesNames.collect()

for result in results:
    print(result)
