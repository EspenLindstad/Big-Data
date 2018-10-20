from __future__ import print_function
from pyspark import SparkConf, SparkContext
import sys



conf = SparkConf().setAppName("task1").setMaster("local[2]")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: task1 <file>", file=sys.stderr)
        exit(-1)


    tweets = sc.textFile(sys.argv[1])
    # Used smaller amount of data for testing
    tweetsTest = tweets.sample(False, 0.1, 5)
    tweets = tweets.map(lambda line: line.split("\t"))

    # Task 1A: Total number of tweets
    numberOfTweets = tweets.count()


    # Task 1B: Number of distinct usernames
    distinctUsernames = tweets.map(lambda x: x[6]).distinct().count()


    # Task 1C: Number of distinct countries
    distinctCountryNames = tweets.map(lambda x: x[1]).distinct().count()


    # Task 1D: Number of distinct places
    distinctPlaceNames = tweets.map(lambda x: x[4]).distinct().count()

    
    # Task 1E: Number of distinct languages
    distinctLanguages = tweets.map(lambda x: x[5]).distinct().count()


    # Task 1F and 1G: Minimal latitude and minimal longitude
    minLatitude = tweets.map(lambda x: x[11]).reduce(lambda a, b: min(a,b))
    minLongitude = tweets.map(lambda x: x[12]).reduce(lambda a, b: min(a,b))



    # Task 1H and 1I: Maximal latitude and maximal longitude
    maxLatitude = tweets.map(lambda x: x[11]).reduce(lambda a, b: max(a,b))
    maxLongitude = tweets.map(lambda x: x[12]).reduce(lambda a, b: max(a,b))



    # Task 1J: Average number of characters in tweet texts
    totalLength = tweets.map(lambda line: len(line[10])).sum()
    avg_char = totalLength/(numberOfTweets)



    # Task 1K: Average number of words in tweets texts
    totalWords = tweets.map(lambda line: len(line[10].split(" "))).sum()
    avg_words = totalWords/(numberOfTweets)


    results = sc.parallelize([numberOfTweets, distinctUsernames, distinctCountryNames, distinctPlaceNames, distinctLanguages,
        minLatitude, minLongitude, maxLatitude, maxLongitude, avg_char, avg_words])
    results.repartition(1).saveAsTextFile("data/result_1.tsv")
