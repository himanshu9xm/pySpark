from pyspark import SparkConf, SparkContext
import re

conf = SparkConf().setMaster("local").setAppName("countWords")
sc = SparkContext(conf = conf)

def normalizeText(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

try:
    input = sc.textFile("file:///SparkCourse/SampleData/book.txt")
    words = input.flatMap(normalizeText)

    wordsTuple = words.map(lambda x: (x,1))
    wordCounts = wordsTuple.reduceByKey(lambda x, y: x + y)
    wordCountsSorted = wordCounts.map(lambda x : (x[1], x[0])).sortByKey()

    results = wordCountsSorted.collect()

    for result in results:
        cleanWord = result[1].encode('ascii', 'ignore')
        if(cleanWord):
            print(cleanWord.decode(), result[0])

finally:
    sc.stop()