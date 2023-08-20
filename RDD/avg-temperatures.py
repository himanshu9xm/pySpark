from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("AverageTemperatures")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(",")
    stationID = fields[0]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, temperature)

try:
    lines = sc.textFile("file:///SparkCourse/pySpark/SampleData/1800.csv")
    parsedLines = lines.map(parseLine)
    stationCount = parsedLines.mapValues(lambda x: (x,1))
    totalTemp = stationCount.reduceByKey(lambda x,y: ((x[0] + y[0]), (x[1] + y[1])))
    averageTemp = totalTemp.mapValues(lambda x: float(x[0]) / float(x[1]))

    results = averageTemp.collect()
    orderedResults = sorted(results)

    for result in orderedResults:
        print(result[0], " \t {:.2f}F".format(result[1]))

finally:
    sc.stop()