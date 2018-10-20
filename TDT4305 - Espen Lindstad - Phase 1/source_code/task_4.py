from __future__ import print_function
from pyspark import SparkConf, SparkContext
from datetime import datetime
import sys



conf = SparkConf().setAppName("task4").setMaster("local[2]")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: task4 <file>", file=sys.stderr)
        exit(-1)



    tweets = sc.textFile(sys.argv[1])
    # Used smaller amount of data for testing
    tweetsTest = tweets.sample(False, 0.1, 5)
    tweets = tweets.map(lambda line: line.split("\t"))


    # Maps the country_name og calculates the hour by the datetime.fromtimestamp inclusion
    # Maps into a key_value pair and reduces with add. Sorts by key.
    tweetsPerHour = tweets.map(lambda line: (line[1], datetime.fromtimestamp(int(line[0])/1000+int(line[8])))) \
                    .map(lambda line: (((line[0]),int(line[1].hour)),1)).reduceByKey(lambda a,b: a+b).sortByKey(True,1)
    
    # Makes a list with countryname and the hour. Reduces the lines to find the element with the highest count in the hour.
    list = tweetsPerHour.map(lambda line: (line[0][0],line[1])).reduceByKey(max).collect()

    # Filters out elementswhich are not in the list. Here there might be several occurences of a element if a country have the same 
    # amount of tweets in different hours.
    # Maps, reduces and maps to get rid of the "duplicates".
    tweetsPerHour = tweetsPerHour.filter(lambda line: (line[0][0], line[1]) in list) \
                    .map(lambda line: ((line[0][0], line[1]), line[0][1])) \
                    .reduceByKey(lambda a,b: a) \
                    .map(lambda line: (line[0][0], line[1], line[0][1]))
    
    tweetsPerHour.repartition(1).saveAsTextFile("data/result_4.tsv")