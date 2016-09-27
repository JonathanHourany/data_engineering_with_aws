import os
import sys
import json
import boto3
import csv
import pyspark
import random
from glob import glob
from pyspark import SparkContext

reload(sys)
sys.setdefaultencoding('utf-8')

def symptom_to_disease(tweet_txt, symptom_map):
    """Maps a reported symptom (if there is one in a tweet) to a corrisponding
    disease. If the word 'coughing' is in the tweet, for example, it could be
    mapped to 'cold'.

    Parameters
    ----------
    tweet_text: string
            Body of a tweet
    symptom_map: dict 
            Map of symptoms to corresponding diseases

    Returns
    -------
    disease: string
            Name of a disease
    None: None
            If no words in tweet_txt map to a disease, none is returned
    """
    for word in tweet_txt.split():
        if symptom_map.has_key(word):
            return symptom_map[word]
    return None


def main():
    """Retrieves tweets saved in S3 bucket and parses them using Spark.

    Twitter's API does not allow clients to both track tweets that include
    certain hashtags, and tweets that contain geo location at the same time,
    so geo locations are randomly drawn from a list an attached to tweets to
    satisfy the needs of the demo"""

    sc = SparkContext()

    with open('fake_geo.txt', 'r') as f:
        fake_geo = [geo.strip() for geo in f.readlines()]
    with open('disease-common-names.json', 'r') as f:
        diseases = json.loads(f.read())

    field_names = ['created_at', 'timestamp_ms', 'id', 'screen_name',
                  'disease', 'lat', 'lon', 'text']
    client = boto3.client('s3')
    bucket = boto3.resource('s3').Bucket('firehose-protonova')

    sympt_to_disease = {}
    for disease in diseases.keys():
        for symptom in diseases[disease]:
            sympt_to_disease[symptom] = disease

    for stream_file in bucket.objects.all():
        f = client.get_object(Bucket=stream_file.bucket_name, Key=stream_file.key)['Body']
        twitter_rdd = sc.parallelize(f.read().splitlines())
        fake_lat, fake_lon = random.choice(fake_geo).split(', ')

        # Parallizes the operation of filtering out tweets the don't meet requirments
        parsed_tweets = twitter_rdd.map(lambda tweet: json.loads(tweet)) \
                                   .filter(lambda tweet: tweet.get('text', None) is not None) \
                                   .filter(lambda tweet: tweet['text'][:2] != 'RT') \
                                   .filter(lambda tweet: any(sympt in tweet['text'].split() for sympt in sympt_to_disease.keys())) \
                                   .map(lambda tweet: {'screen_name': tweet['user']['screen_name'],
                                                       'timestamp_ms': tweet['timestamp_ms'],
                                                       'created_at': tweet['created_at'],
                                                       'id': tweet['id'],
                                                       'disease': symptom_to_disease(tweet['text'], sympt_to_disease),
                                                       'text': tweet['text'],
                                                       'lat': fake_lat,
                                                       'lon': fake_lon}) \
                                   .collect()
    with open('illness_tweets.csv', 'a') as csvfile:
        writer = csv.DictWriter(csvfile, field_names=field_names)
    for row in parsed_tweets:
        writer.writerow(row)


if __name__ == '__main__':
    main()
