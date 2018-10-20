from __future__ import print_function
from pyspark import SparkConf, SparkContext
import sys
from operator import add



conf = SparkConf().setAppName("task5").setMaster("local[2]")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: task5 <file>", file=sys.stderr)
        exit(-1)



    tweets = sc.textFile(sys.argv[1])
    # Used smaller amount of data for testing
    tweetsTest = tweets.sample(False, 0.1, 5)
    tweets = tweets.map(lambda line: line.split("\t"))


    # Filters out the elements where the country_code != US and place_type != city.
    # Map the city to a key-value pair.
    # Reduces by key with add and sorts first on occurence by descending order and then on place_name alphabetically
    tweetsPerCity = tweets.filter(lambda line: line[2] == 'US').filter(lambda line: line[3] == 'city') \
                    .map(lambda line: (line[4],1)) \
                    .reduceByKey(add).sortBy(lambda line: (-line[1], line[0])) \
        

    tweetsPerCity.repartition(1).saveAsTextFile("data/result_5.tsv")