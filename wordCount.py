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


wordCount = words.countByValue()

for word, count in wordCount.items():
    cleanWord = word.encode('ascii', 'ignore')

    if cleanWord:
        print(cleanWord.decode() + " " + str(count))
