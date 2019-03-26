import re

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("wordCount")
sc = SparkContext(conf=conf)


def normalize_words(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())


lines = sc.textFile("data/words.txt")

# making it better
# words = lines.flatMap(lambda x: x.split())

words = lines.flatMap(normalize_words)

# manually extracting count using lambda
# wordCount = words.countByValue()
wordCounts = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x + y)
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortBy()

results = wordCountsSorted.collect()

# for word, count in wordCount.items():
for result in results:
    count = str(results[0])
    cleanWord = results[1].encode('ascii', 'ignore')

    if cleanWord:
        print(cleanWord.decode() + " " + str(count))
