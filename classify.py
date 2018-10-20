from pyspark import SparkConf, SparkContext
import sys
import argparse



conf = SparkConf().setAppName("classify").setMaster("local[*]")
sc = SparkContext(conf=conf)
sc.setLogLevel("WARN")


if __name__ == "__main__":

    # Initiating with the input parameters

    parser = argparse.ArgumentParser(description='A program to classify tweet locations')
    parser.add_argument('-t', '--training', dest='training_data')
    parser.add_argument('-i', '--input', dest='input_tweet')
    parser.add_argument('-o', '--output', dest='output_file')
    args = parser.parse_args()

    
    # Tranforms the input tweet to a list of words

    words = sc.textFile(args.input_tweet)
    words = words.map(lambda line: line.lower().split(" ")).collect()
    words = words[0]

  

    data = sc.textFile(args.training_data)

    # Used smaller amount of data for testing

    """data = data.sample(False, 0.1, 5)"""

    data = data.map(lambda line: line.split("\t"))
    data = data.map(lambda line: (line[4], line[10]))


    # Number of tweets from place
    # Caches the data for later use

    numberOfTweetsFromPlace = data.map(lambda line: (line[0], 1)) \
                    .reduceByKey(lambda a,b: a+b).cache()

    # Total number of tweets
    numberOfTweets = numberOfTweetsFromPlace.map(lambda line: line[1]) \
                    .reduce(lambda a,b: a+b)

    

    # Flatmaps the words into key-value pairs with the place. The words in the tweets are separated and duplicates are removed.
    # Then the key-value pairs are filtered up against the words-list
    # Then the key-value pairs are reduced by the key
    # Mapped to a easier format
    # Grouped by the place and the appearances are added to a list
    # The results are then cached for later use

    data = data.flatMap(lambda line: (((line[0], word), 1) for word in set(line[1].lower().split(" ")))) \
                    .filter(lambda line: line[0][1] in words) \
                    .reduceByKey(lambda a,b: a+b) \
                    .map(lambda line: ((line[0][0]),(line[0][1],line[1]))) \
                    .groupByKey().map(lambda line : (line[0], list(line[1]))).cache()
  
    # Takes the number of appearances of each word and adds it to a list without the word
    def wordCount(line):
        if len(line) >= 2:
            alist = []
            for i in range(0, len(line)):
                alist.append(line[i][1])
                i = i + 1
            return alist
        else:
            alist = [line[0][1]]
            return alist

    # The Naive Bayer Classifier
    def naiveBayer(line, list):
        sum = 0
        sum = line/float(numberOfTweets)
        for i in range(0, len(list)):
            sum = sum * list[i]/line
        return sum

    
    # The numberOfTweetsFromPlace are joined with tweetsTest on the placename to get all the parameters on one line
    # The all the places with no results are filtered out
    # First mapped with the use of the wordCount()-function and then the naiveBayer()-function

    finalResult = (numberOfTweetsFromPlace.fullOuterJoin(data)) \
                    .filter(lambda line: line[1][1] != None) \
                    .map(lambda line: (line[0], line[1][0], wordCount(line[1][1]))) \
                    .map(lambda line: (line[0], naiveBayer(line[1], line[2])))



    if finalResult.count() != 0:

        # Brings out the maximum probability

        maxResult = finalResult.max(key=lambda x:x[1])[1]

        # Filters on the highest probability to find other results with equal probability

        finalResult = finalResult.filter(lambda line: line[1] == maxResult).collect()

        # Saves results to the output file

        with open(args.output_file, 'w') as outputfile:
            for line in finalResult:
                outputfile.write(line[0] + "\t" + str(line[1]))

    # Return nothing if all places have zero probability
    
    else:
        with open(args.output_file, 'w') as outputfile:
            outputfile.write(" ")
