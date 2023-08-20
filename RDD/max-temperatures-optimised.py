from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MaxTemperaturesOPtimised")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(",")
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)

try:
    lines = sc.textFile("file:///SparkCourse/pySpark/SampleData/1800.csv")
    parsedLines = lines.map(parseLine)
    maxTemps = parsedLines.filter(lambda x: "TMAX" in x[1])
    stationTemps = maxTemps.map(lambda x: ( x[0], x[2] ))
    maxTemps = stationTemps.reduceByKey( lambda x, y : max(x, y))

    # results = maxTemps.collect()
    # results = stationTemps.collect()
    results = maxTemps.collect()

    print("StationID \t MaxTemp")
    for result in results:
        print(result[0] + " \t {:.2f}F".format(result[1]))
        # print(result)

finally:
    sc.stop()