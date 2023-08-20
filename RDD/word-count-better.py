import re
from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("countWords")

sc = SparkContext(conf = conf)

def normaliseWords(text):
    return re.compile(r'\W+', re.UNICODE).split(text.lower())

try:
    input = sc.textFile("file:///SparkCourse/pySpark/SampleData/book.txt")
    words = input.flatMap(normaliseWords)
    wordCounts = words.countByValue()
    print(type(wordCounts))
    sortedWordCounts = sorted(wordCounts.items(), key = lambda item : item[1])


    for word, count in sortedWordCounts:
        cleanWord = word.encode('ascii','ignore')
        if(cleanWord):
            print(cleanWord.decode(), count)

finally:
    sc.stop()