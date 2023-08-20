from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("AmountSpentCustomerWise")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(",")
    customerID = int(fields[0])
    amount = float(fields[2])
    return (customerID, amount)

try:
    input = sc.textFile("file:///SparkCourse/pySpark/SampleData/customer-orders.csv")

    parsedLines = input.map(parseLine)
    totAmount = parsedLines.reduceByKey(lambda x,y : x + y)

    #sort by customerID
    # sortedTotAmount = totAmount.sortByKey()
    #sort by Amount
    sortedTotAmount = totAmount.map(lambda x : (x[1], x[0])).sortByKey()


    results = sortedTotAmount.collect()

    for result in results:
        print(str(result[1]) + " \t ${:.2f}".format(result[0]))
finally:
    sc.stop()