from pyspark import SparkConf,SparkContext
from pyspark.sql import Row,SQLContext
from pyspark.streaming import StreamingContext
from textblob import TextBlob

# FIFA semi-finalists
COUNTRIES = ["france", "morocco", "argentina", "croatia"]

# TCP Config
IP_ADDR = "localhost"
PORT = 8080

# Add up the sentiment polarity and count to previous state
def add_sentiment(new_values, last_value):
    count = last_value[1] if last_value else 0
    sentiment = last_value[0] if last_value  else 0

    count_arr = [field[1] for field in new_values]
    sentiment_arr = [field[0] for field in new_values]

    output = (sum(sentiment_arr) + sentiment, sum(count_arr) + count)
    return output

def output_data(dataframe):

    # Store team name, average sentiment, and count data in local variables
    teams = [str(t.name) for t in dataframe.select("name").collect()]
    avg_sentiments = [str(s.avg_sentiment) for s in dataframe.select("avg_sentiment").collect()]
    counts = [int(c.tweet_count) for c in dataframe.select("tweet_count").collect()]
    total_count = sum(counts)
    
    # Build output string
    output_str = ""

    for i in range(len(teams)):
        output_str += teams[i] + ": " + avg_sentiments[i] + ", "

    output_str += "tweets_processed: " + str(total_count)

    # Output to file
    output_file = open("output.txt", "a")
    output_file.write(output_str + "\n")
    output_file.close()

def build_database(t, rdd):
    try:
        # Get spark sql singleton context from the current context
        if ('sql_context' not in globals()):
            globals()['sql_context'] = SQLContext(rdd.context)
        
        sql_context = globals()['sql_context']

        # convert the RDD to dataframe and then register a table
        row = rdd.map(lambda w: Row(name=w[0], sentiment=w[1][0], tweet_count=w[1][1]))
        dataframe = sql_context.createDataFrame(row)
        dataframe.registerTempTable("fifa_insights")
        
        # Get the average sentiment and count insights
        insights_view = sql_context.sql("select fifa_insights.name, fifa_insights.tweet_count, fifa_insights.sentiment/fifa_insights.tweet_count as avg_sentiment from fifa_insights")
        insights_view.show()

        # Output the data to a file
        output_data(insights_view)
    except Exception as e:
        print("Encountered error: " + str(e))

# Output a list of sentiment and count tuples for each team match
def format_tuple(tweet_tuple):
    tweet_str, textblob_data = tweet_tuple
    output = []

    print("Tweet: " + tweet_str + "\n******************************************n")

    for team in COUNTRIES:
        if team in tweet_str.lower():
            output.append((team, (textblob_data.sentiment.polarity, 1)))
    return output


if __name__ == "__main__":

    # Spark config
    conf = SparkConf()
    conf.setAppName("Twitter Tweet FIFA Sentiment Analysis App")
    spark_context = SparkContext(conf=conf)
    spark_context.setLogLevel("ERROR")
    streaming_spark_context = StreamingContext(spark_context, 2)
    
    # Setting a checkpoint for recovery
    streaming_spark_context.checkpoint("checkpoint_FIFA_App")
    
    # Stream from socket
    stream = streaming_spark_context.socketTextStream(IP_ADDR, PORT)

    # Strip each string from quotes
    tweet_strings = stream.map(lambda x: x[2:len(x) -1 ])

    # Map each string to a tuple with sentiment analysis
    textblob_tweets = tweet_strings.map(lambda x: (x, TextBlob(x)))

    # Flatmap for each tuple list with count and sentiment polarity
    tweets = textblob_tweets.flatMap(format_tuple)
    
    # Add the sentiments and counts to previous state
    totals = tweets.updateStateByKey(add_sentiment)

    # Process the RDD for each interval
    totals.foreachRDD(build_database)

    streaming_spark_context.start()
    streaming_spark_context.awaitTermination()