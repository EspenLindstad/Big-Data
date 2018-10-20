from __future__ import print_function
from pyspark import SparkConf, SparkContext
import sys
from operator import add



conf = SparkConf().setAppName("task7").setMaster("local[2]")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: task7 <file>", file=sys.stderr)
        exit(-1)


    tweets = sc.textFile(sys.argv[1])
    # Used smaller amount of data for testing
    tweetsTest = tweets.sample(False, 0.1, 5)
    tweets = tweets.map(lambda line: line.split("\t"))


    # Filters out the elements where the country_code != US and place_type != city
    # Maps to a key-value pair
    # Reduces by key with add and sorts in descending order
    # Takes only the first 5 elements to the new RDD; the ones with the highest tweet count

    cityCount = tweets.filter(lambda line: line[2] == 'US' and line[3] == 'city') \
                    .map(lambda line: (line[4],1)) \
                    .reduceByKey(add).sortBy(lambda line: line[1], False) \
                    .take(5)

    # Creates list for the cities
    cities=[]
    for line in cityCount:
        cities.append(line[0])

    # Creates a list for the stop_words
    stopWords = []
    with open('data/stop_words.txt', 'r') as stopwords:
        for word in stopwords:
            stopWords.append(word.strip())
    stopwords.close()




       
    # Filter out tweets that are not one of the top 5 cities
    # Uses flatmap to change the tweet to seperate words and then makes them lowercase
    # Then filter out words which is a part of stopwords or are shorter than 2
    # Map and reduce to count words on the form (word, wordcount)
    # Sort words by count value, take top 10 words

    frequentTweets = tweets.filter(lambda line: line[4] in cities) \
                .flatMap(lambda line: ((line[4], word) for word in line[10].lower().split(" "))) \
                .filter(lambda line: line[1] not in stopWords and len(line[1]) > 2) \
                .map(lambda line: ((line[0], line[1]),1)) \
                .reduceByKey(lambda a,b: a+b) \
                .map(lambda line: ((line[0][0]),(line[0][1],line[1]))).sortBy(lambda line: line[1][1], False) \
                .groupByKey().mapValues(list) \
                .map(lambda line: (line[0],line[1][:10]))


       
    frequentTweets.repartition(1).saveAsTextFile("/Users/Lindstad/Documents/hadoop-3.0.0/Sparkshit/src/results/result_7.tsv")
