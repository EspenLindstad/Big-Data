from __future__ import print_function
from pyspark import SparkConf, SparkContext
import sys
from pyspark.sql import Row
from pyspark.sql import SparkSession


conf = SparkConf().setAppName("task8").setMaster("local[2]")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: task8 <file>", file=sys.stderr)
        exit(-1)

    spark = SparkSession\
        .builder\
        .appName("task8")\
        .getOrCreate()


    tweets = sc.textFile(sys.argv[1])
    # Used smaller amount of data for testing
    tweetsTest = tweets.sample(False, 0.1, 5)
    tweets = tweets.map(lambda line: line.split("\t"))


    # Creates a tavle with columns based on the tsv-file
    table = tweets.map(lambda p: Row(utc_time=p[0], country_name=p[1], country_code=p[2], place_type=p[3], place_name=p[4], language=p[5],
        username=str(p[6]), user_screen_name=p[7], timezone_offset=p[8], number_of_friends=p[9], tweet_text=p[10], latitude=p[11], longitude=p[12]))

    # Creates a DataFrame of the table and creates a tempView
    table = spark.createDataFrame(table)
    table.createOrReplaceTempView("table")

    """OPPGAVE 8A"""
    numberOfTweets = spark.sql("SELECT  COUNT(*) FROM table").collect()
    
        
    """OPPGAVE 8B"""
    distinctUsernames = spark.sql("SELECT  COUNT(DISTINCT (username)) FROM table").collect()


    """OPPGAVE 8C"""
    distinctCountries = spark.sql("SELECT  COUNT(DISTINCT (country_name)) FROM table").collect()
        

    """OPPGAVE 8D"""
    distinctPlaces = spark.sql("SELECT  COUNT(DISTINCT (place_name)) FROM table").collect()


    """OPPGAVE 8E"""
    distinctLanguages = spark.sql("SELECT  COUNT(DISTINCT (language)) FROM table").collect()


    """OPPGAVE 8F"""
    minimumValues = spark.sql("SELECT MIN(latitude), MIN(longitude) FROM table").collect()


    """OPPGAVE 8G"""
    maximumValues = spark.sql("SELECT MAX(latitude), MAX(longitude) FROM table").collect()

    # Transforms into a RDD to use the saveAsTextFile() function.
    results = sc.parallelize([numberOfTweets, distinctUsernames, distinctCountries, distinctPlaces, distinctLanguages, minimumValues, maximumValues])
    results.repartition(1).saveAsTextFile("/Users/Lindstad/Documents/hadoop-3.0.0/Sparkshit/src/results/result_8.tsv")  


spark.stop()