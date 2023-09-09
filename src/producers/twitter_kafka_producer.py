#==============================================================================#
#                                                                              #
#    Title: Title                                                              #
#    Sources:                                                                  #
#    - https://www.bmc.com/blogs/working-streaming-twitter-data-using-kafka/   #
#    - https://towardsdatascience.com/using-kafka-to-optimize-data-flow-of-your-twitter-stream-90523d25f3e8 #
#    - https://elkhayati.me/kafka-python-twitter/                              #
#                                                                              #
#==============================================================================#



#------------------------------------------------------------------------------#
#                                                                              #
#    Setup                                                                  ####
#                                                                              #
#------------------------------------------------------------------------------#


#------------------------------------------------------------------------------#
# Import Globals                                                            ####
#------------------------------------------------------------------------------#

import sys, os, json
from tweepy.streaming import StreamListener
from tweepy import OAuthHandler, Stream
from confluent_kafka import Producer
from queue import Queue
from threading import Thread 

# https://github.com/tweepy/tweepy/issues/908
from http.client import IncompleteRead as http_incompleteRead
from urllib3.exceptions import IncompleteRead as urllib3_incompleteRead
import time

#------------------------------------------------------------------------------#
# Fix local issue                                                           ####
#------------------------------------------------------------------------------#

# Ensure the directory is correct... Every time. ----
for i in range(5):
    if not os.path.basename(os.getcwd()).lower() == "mdsi_bde_aut21_at3":
        os.chdir("..")
    else:
        break

# Ensure the current directory is in the system path. ---
if not os.path.abspath(".") in sys.path: sys.path.append(os.path.abspath("."))


#------------------------------------------------------------------------------#
# Local Imports                                                             ####
#------------------------------------------------------------------------------#

from src.secrets import secrets as sc


#------------------------------------------------------------------------------#
# Key Variables                                                             ####
#------------------------------------------------------------------------------#

consumer_key = sc.twitter_consumer_key
consumer_secret = sc.twitter_consumer_secret
twitter_access_token = sc.twitter_access_token
twitter_access_token_secret = sc.twitter_access_token_secret



#------------------------------------------------------------------------------#
#                                                                              #
#    Build Kafka                                                            ####
#                                                                              #
#------------------------------------------------------------------------------#


#------------------------------------------------------------------------------#
# Variables                                                                 ####
#------------------------------------------------------------------------------#

"""
See config options:
https://github.com/edenhill/librdkafka/blob/master/CONFIGURATION.md
"""

producer = Producer({"bootstrap.servers": "broker:29092"})
# producer = Producer({"bootstrap.servers": "localhost:9092"})
topic_name = 'twitterdata'


#------------------------------------------------------------------------------#
#                                                                              #
#    Build Twitter                                                          ####
#                                                                              #
#------------------------------------------------------------------------------#


#------------------------------------------------------------------------------#
# Variables                                                                 ####
#------------------------------------------------------------------------------#

tracking = ['cryptocurrency',
    'crypto',
    'binance',
    'coinbase',
    'coinmarketcap',
    'musk',
    'memecoin',
    'shitcoin', #🤨
    'moon',
    'hodl',
    'fud',
    'bitcoin',
    'btc',
    'ethereum',
    'eth',
    'ether',
    'gwei',
    'vitalik buterin',
    'gavin wood',
    'erc20',
    'dogecoin',
    'doge',
    'billy markus',
    'jackson palmer',
    'pancakeswap',
    'cake',
    'swap'
]


#------------------------------------------------------------------------------#
# Classes                                                                   ####
#------------------------------------------------------------------------------#

class TwitterAuth():

    def authenticate(self):
        auth = OAuthHandler(consumer_key, consumer_secret)
        auth.set_access_token(twitter_access_token, twitter_access_token_secret)
        return auth
    

class TwitterListener(StreamListener):

    def __init__(self):
        super().__init__()
        
        # To resolve the 'Connection broken: Incomplete Read' issue we are implementing 
        # a queue, where on_data items will be placed on the queue, we will then
        # have 4 threads polling the queue for new data items to push to kafka.
        # based on: https://stackoverflow.com/questions/48034725/tweepy-connection-broken-incompleteread-best-way-to-handle-exception-or-can
        self.q = Queue()
        num_worker_threads = 4

        for i in range(num_worker_threads):
            t = Thread(target=self.push_data_to_kafka)
            t.daemon = True
            t.start()
    
    def on_data(self, raw_data):
        
        # Put any new data items into the queue for processing
        self.q.put(raw_data)
        
        return True
    
    def push_data_to_kafka(self):

        # Continually loop and get new data items off the queue
        while True:
            raw_data = self.q.get()
            producer.poll(0)
            producer.produce( 
                topic = topic_name
                , value = str.encode(raw_data)
                , callback = self.callback
            )
            self.q.task_done()
    
    def on_error(self, status):
        print(status)

    def on_exception(self, exception):
        print(exception)
        return

    def callback(self, error, message):
        if error:
            print(f"Error: {message.value()}: {error.str()}")
        else:
            print(f"Sucess: {message.value()}")


class TwitterStreamer():

    def __init__(self):
        self.twitter_auth = TwitterAuth()

    def stream_tweets(self):
        listener = TwitterListener()
        auth = self.twitter_auth.authenticate()
        stream = Stream(auth, listener)

        while True:
            try:
                print('start listening...')
                stream.filter(track=tracking, stall_warnings=True, languages=['en'])
            except BaseException as e:
                print("Error in stream.filter: %s, Pausing for 5 seconds..." % str(e))
                time.sleep(5)
            

#------------------------------------------------------------------------------#
#                                                                              #
#    Run Everything                                                         ####
#                                                                              #
#------------------------------------------------------------------------------#

if __name__ == '__main__':
    ts = TwitterStreamer()
    ts.stream_tweets()
