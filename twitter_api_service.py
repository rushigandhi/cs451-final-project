import socket
import os
import tweepy

# From Twitter API keys page
TWITTER_API_BEARER_TOKEN = os.environ.get("TWITTER_API_BEARER_TOKEN")

# TCP Config
IP_ADDR = "localhost"
PORT = 8080

def send_to_spark_app(tweet, connection):
    try:
        text = str(tweet.encode("utf-8"))
        print("Tweet: " + text + "\n******************************************n")
        data = bytes(text + "\n", 'utf-8')
        connection.send(data)
    except Exception as e:
        print("Encountered error: " + str(e))

class TwitterStreamingClient(tweepy.StreamingClient):

    def __init__(self, bearer_token, connection): 
        self.connection = connection
        super().__init__(bearer_token)
 
    def on_tweet(self, tweet):
        send_to_spark_app(tweet.text, self.connection)

if __name__ == "__main__":
    
    connection = None
    skt = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    skt.bind((IP_ADDR, PORT))
    skt.listen(1)

    print("Waiting to establish connection...")
    connection, _ = skt.accept()
    print("Connected.")

    streamer = TwitterStreamingClient(TWITTER_API_BEARER_TOKEN, connection)
    streamer.add_rules(tweepy.StreamRule("(france OR argentina OR morocco OR croatia) lang:en"))
    streamer.filter()


