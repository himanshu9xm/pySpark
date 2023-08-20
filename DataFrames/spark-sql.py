from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql.functions import col

# Create a spark session
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()


def parseLine(line):
    fields = line.split(",")
    return Row(
        ID=int(fields[0]),
        name=str(fields[1]),
        age=int(fields[2]),
        numFriends=int(fields[3])
    )


try:

    lines = spark.sparkContext.textFile("file:///SparkCourse/pySpark/SampleData/fakefriends.csv")
    people = lines.map(parseLine)

    # Infer the schema, and register the DataFrame as a table.
    schemaPeople = spark.createDataFrame(people).cache()
    schemaPeople.createOrReplaceTempView("people")

    #--------------------------Selecting all teenagers i.e people with age from 13 to 19---------------------------------------------------
    # print("With SQL-----------------------------------------")
    # SQL can be run over DataFrames that have been registered as a table.
    # teenagers = spark.sql("SELECT * FROM people WHERE age>=13 AND age<=19").show()
    
    # The results of SQL queries are RDDs and support all the normal RDD operations.
    # results = teenagers.collect()
    # print("ID\tName\tAge\tNumber Of Friends")
    # for result in results:
    #     print(str(result[0]) + "\t" + result[1] + "\t" + str(result[2]) + "\t" + str(result[3]) )

    # print("Without SQL-----------------------------------------")
    # We can also use functions instead of SQL queries:
    # schemaPeople.select("ID", "name", col("age").cast("int"), "numFriends").filter(schemaPeople["age"]>=13).filter(schemaPeople["age"]<=19).orderBy("ID").show()



    #--------------------------Getting the number of people for each age-------------------------------------------------------------------
    print("With SQL-----------------------------------------")
    spark.sql("SELECT age, count(1) FROM people GROUP BY age ORDER BY age").show(schemaPeople.count(), truncate=False)

    print("Without SQL-----------------------------------------")
    schemaPeople.groupBy("age").count().orderBy("age").show(schemaPeople.count(), truncate=False)

finally:
    spark.stop()