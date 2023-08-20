from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("MinTemperatures")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0 / 5.0) + 32.0
    return (stationID, entryType, temperature)

# try-finally block to make sure that sc.shut() is called even if an exception occurs in our code preventing resource leaks in case of failures
try:
    lines = sc.textFile("file:///SparkCourse/SampleData/1800.csv")
    parsedLines = lines.map(parseLine)
    minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])
    stationTemps = minTemps.map(lambda x: (x[0], x[2]))
    minStationTemps = stationTemps.reduceByKey(lambda x, y: min(x,y))

    results = minStationTemps.collect()

    for result in results:
        print(result[0], "\t{:.2f}F".format(result[1]))

finally:
# used to gracefully shutdown the SparkContext and releasing any resources associated with it such as memory, threads, and connections
    sc.stop() 

