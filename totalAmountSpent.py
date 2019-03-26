from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("totalAmountSpent")
sc = SparkContext(conf=conf)


def parse_line(line):
    fields = line.split(',')

    customer = int(fields[0])
    spent = float(fields[2])

    return customer, spent


lines = sc.textFile("data/amount.csv")
rdd = lines.map(parse_line)

totalAmountSpent = lines.reduceByKey(lambda x, y: x + y)

results = totalAmountSpent.collect()

# for word, count in wordCount.items():
for _id, amount in results:
    print(_id, amount)
