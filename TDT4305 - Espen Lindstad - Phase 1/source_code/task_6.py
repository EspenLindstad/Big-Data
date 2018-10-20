from __future__ import print_function
from pyspark import SparkConf, SparkContext
import sys
from operator import add



conf = SparkConf().setAppName("task6").setMaster("local[2]")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: task6 <file>", file=sys.stderr)
        exit(-1)


    tweets = sc.textFile(sys.argv[1])
    # Used smaller amount of data for testing
    tweetsTest = tweets.sample(False, 0.1, 5)
    tweets = tweets.map(lambda line: line.split("\t"))
  

     # Creates a list for the stop_words
    stopWords = []
    with open('data/stop_words.txt', 'r') as stopwords:
        for word in stopwords:
            stopWords.append(word.strip())
    stopwords.close()

    # Filter out the country_codes != 'US'
    # Maps the tweetText and flatMaps it by lowercase and split on whitespace
    # Filters out the the stopwords
    # Filters out the words shorter than 1
    # Maps words into a key-value pair
    # Reduces by key with add and sorts by descending order.
    cities = tweets.filter(lambda line: line[2] == 'US') \
                    .map(lambda line: (line[10])) \
                    .flatMap(lambda line: line.lower().split(" ")) \
                    .filter(lambda line: line not in stopWords and (len(line)>1)) \
                    .map(lambda line: (line, 1)) \
                    .reduceByKey(add).sortBy(lambda line: line[1], False)

    	
    cities.repartition(1).saveAsTextFile("data/result_6.tsv")