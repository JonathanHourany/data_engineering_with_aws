import os
import uuid
import tweepy
import boto3
import json
from pprint import pprint

class StreamListener(tweepy.StreamListener):
    def __init__(self, boto_client):
        super(tweepy.StreamListener, self).__init__()
        self.kinesis = boto_client

    def on_status(self, status):
        print status.txt

    def on_data(self, data):
        self.kinesis.put_record(DeliveryStreamName='twitter',
                                Record={'Data': data})

    def on_error(self, status):
        print status
        return False


def main():
    '''This program uses AWS's resource client to control the Kinesis-firehose
    instance'''

    # Set up enviromentals
    consumer_key = os.environ.get('consumerKey')
    consumer_secret = os.environ.get('consumerSecret')
    access_token = os.environ.get('accessToken')
    access_secret = os.environ.get('accessTokenSecret')
    stream_name = 'twitter'

    # Instanciate client
    client = boto3.client('firehose')

    # Get status of the delivery stream
    stream_status = client.describe_delivery_stream(DeliveryStreamName=stream_name)
    if stream_status['DeliveryStreamDescription']['DeliveryStreamStatus'] == 'ACTIVE':
        print "\n ==== KINESES ONLINE ===="

    auth = tweepy.OAuthHandler(consumer_key, consumer_secret)
    auth.set_access_token(access_token, access_secret)
    api = tweepy.API(auth)

    streamListener = StreamListener(client)
    stream = tweepy.Stream(auth=api.auth, listener=streamListener)

    try:
        stream.filter(track=['bringmesoup', 'flu', 'sickday', '\U0001f637'],
                      languages=['en'])
    finally:
        stream.disconnect()


if __name__ == '__main__':
    main()
