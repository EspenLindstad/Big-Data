from __future__ import print_function
from pyspark import SparkConf, SparkContext
import sys


conf = SparkConf().setAppName("task1").setMaster("local[2]")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: task3 <file>", file=sys.stderr)
        exit(-1)


    tweets = sc.textFile(sys.argv[1])
    # Used smaller amount of data for testing
    tweetsTest = tweets.sample(False, 0.1, 5)
    tweets = tweets.map(lambda line: line.split("\t"))


    # Filters out countries where there is <= 10 tweets
    TweetsPerCountry = tweets.map(lambda line: line[1]).map(lambda country: (country,1)) \
                    .reduceByKey(lambda a,b: a+b) \
                    .filter(lambda line: line[1] > 10)

    # Creates a list with the countries for later use
    list = TweetsPerCountry.map(lambda line: line[0]).collect()


    aTuple = (0,0)
    # Filters out the countries which are not a part of the list
    # Maps the country_name and tweet_text
    # AggregatesByKey on number of sum of latitudes and number of occurences
    # Maps the values by dividing the sum og number of occurences to get the average
    averageLatitude = tweets.filter(lambda line: line[1] in list) \
                    .map(lambda line: (line[1], line[11])) \
                    .aggregateByKey(aTuple, lambda a,b: (a[0] + float(b),    a[1] + 1),
                                    lambda a,b: (a[0] + b[0], a[1] + b[1])) \
                    .mapValues(lambda v: v[0]/v[1])


    # Filters out the countries which are not a part of the list
    # Maps the country_name and tweet_text
    # AggregatesByKey on number of sum of longitudes and number of occurences
    # Maps the values by dividing the sum og number of occurences to get the average
    bTuple = (0,0)
    averageLongitude = tweets.filter(lambda line: line[1] in list) \
                    .map(lambda a: (a[1], a[12])) \
                    .aggregateByKey(aTuple, lambda a,b: (a[0] + float(b),    a[1] + 1),
                                    lambda a,b: (a[0] + b[0], a[1] + b[1])) \
                    .mapValues(lambda v: v[0]/v[1])


    # Join the average latitude and longitude with countryname as key
    finalResult = (averageLatitude.fullOuterJoin(averageLongitude))


    finalResult.repartition(1).saveAsTextFile("data/result_3.tsv")

