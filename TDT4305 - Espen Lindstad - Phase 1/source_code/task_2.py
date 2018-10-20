from __future__ import print_function
from pyspark import SparkConf, SparkContext
import sys



conf = SparkConf().setAppName("task2").setMaster("local[2]")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: task2 <file>", file=sys.stderr)
        exit(-1)

  


    tweets = sc.textFile(sys.argv[1])
    # Used smaller amount of data for testing
    tweetsTest = tweets.sample(False, 0.1, 5)
    tweets = tweets.map(lambda line: line.split("\t"))

    # Maps the countries into a key-value pair
    # Reduces to find amount of occurences
    # Sorts first by number in descending order and then alphabetically on country_name
    TweetsPerCountry = tweets.map(lambda line: line[1]).map(lambda country: (country,1)) \
    				.reduceByKey(lambda a,b: a+b) \
    				.sortBy(lambda line: (-line[1], line[0]))


    TweetsPerCountry.repartition(1).saveAsTextFile("data/result_2.tsv")
