from tweepy.streaming import StreamListener
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy import API
from tweepy import Cursor
import json
import datetime
import time
import sys

class MyStreamListener(StreamListener):
    """
    Twitter listener, collects streaming tweets and output to a file
    """
    def __init__(self, api, OUTPUT_FILENAME, stop_condition=10):
        """
        initialize the stream, with num. of tweets and saving the outputfile
        """
        
        # this line is needed to import the characteristics of the streaming API
        super(MyStreamListener, self).__init__()
        
        # to-count the number of tweets collected        
        self.num_tweets = 0
        
        # save filename
        self.filename = OUTPUT_FILENAME
        
        # stop-condition
        self.stop_condition = stop_condition
        

    def on_status(self, status):
        
        """
        this function runs each time a new bunch of tweets is retrived from the streaming
        """
        
        with open(self.filename, "a+") as f:
            tweet = status._json
            
            f.write(json.dumps(tweet) + '\n')
            #self.output.append(tweet)
            self.num_tweets += 1
        
            # Stop condition        
            if self.num_tweets <= self.stop_condition:
                return True
            else:
                return False
        

    def on_error(self, status):
        """
        function useful to handle errors. It's possible to personalize it 
        depending on the way we want to handle errors
        """
        print(status)
        return False
        

def authenticate(path_to_credentials):
    """
        Function used to authenticate the user.
        Args:
            - the path to the file where the credentials are stored
        returns:
            The authentication object, and the api connection binded to it
    """
    credentials = sys.argv[1]
    # open credentials file and set them as variables
    with open(credentials) as f:
        secrets = json.load(f)
    access_token1 = secrets["publicToken"]
    access_token_secret1 = secrets["secretToken"]
    consumer_key1 = secrets["publicKey"]
    consumer_secret1 = secrets["secretKey"]
    # authenticate
    auth = OAuthHandler(consumer_key1, consumer_secret1)
    auth.set_access_token(access_token1, access_token_secret1)
    api = API(auth_handler=auth, wait_on_rate_limit=True, wait_on_rate_limit_notify=True)
    return(auth, api)

def stream_to_file(keywords, api, output_filename='data/out.json', limit=10000):
    """
        This functions creates (or writes, in case it exists) in a file with streamed tweets.
        Args:
            - Keywords: The keywords to look for
            - Api, the api connection object
            - Output_filename: the name of the file in which to write to
            - limit: how many tweets we'd like to retrive at max (default 10k because twitter sometimes bans me for no reason)
    """
    l = MyStreamListener(api, output_filename, limit)
    # here we recall the Stream Class from Tweepy to input the authentication info and our personalized listener 
    stream = Stream(auth=api.auth, listener=l)
    stop = False
    stream.filter(
                track=keywords, 
                is_async=False, 
                languages = ["en"]
    )


def main():
    """
    Args in order:
        * File where your credentials are stored
        * Limit of tweets to scrap
    """
    output_filename = 'data/' + sys.argv[2]
    auth, api = authenticate(sys.argv[1])
    keywords = ["coronavirus", "covid", "#COVID", "#coronavirus", "#COVID19", "pandemic", "#SARS-CoV-2", "#HCoV-19", "transmission", "lockdown"]
    stream_to_file(keywords, api, output_filename=output_filename)

    
if __name__ == "__main__":
    main()