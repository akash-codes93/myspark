from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("wordCount")
sc = SparkContext(conf=conf)


def parse_line(line):
    fields = line.split(',')

    age = int(fields[2])
    num_friends = int(fields[3])

    return age, num_friends


lines = sc.textFile("data/words.txt")
words = lines.flatMap(lambda x: x.split())

wordCount = words.countByValue()

for word, count in wordCount.items():
    cleanWord = word.encode('ascii', 'ignore')

    if cleanWord:
        print(cleanWord.decode() + " " + str(count))
