"""
Twitter ETL Script
Goal:
- Authenticate with the Twitter API
- Pull recent tweets from a specific user
- Extract only the fields we care about
- Store the refined result as a CSV file
Input and output behavior remain exactly the same as the original script.
"""

# -----------------------------
# External libraries
# -----------------------------
import tweepy
# tweepy is used to authenticate with Twitter and interact with the Twitter API

import pandas as pd
# pandas is used to structure the tweet data into a table and export it as CSV

import json
# json is available for handling raw JSON if needed (tweets arrive as JSON objects)

from datetime import datetime
# datetime is useful for handling timestamps like tweet creation time

import s3fs
# s3fs allows pandas to interact with S3 directly
# (not used in this version yet, but commonly used in later pipeline steps)

# -----------------------------
# ETL function
# -----------------------------
def etl_twitter_run():
    """
    Extract:
    - Pull tweets from a specific Twitter account

    Transform:
    - Select only required fields
    - Convert raw tweet objects into a clean structure

    Load:
    - Save the refined data locally as a CSV file
    """

    # -----------------------------
    # Twitter API credentials
    # -----------------------------
    # These keys are required to authenticate with Twitter
    # In production, these should come from environment variables or a secrets manager
    access_key = ""
    access_secret = ""
    consumer_key = ""
    consumer_secret = ""

    # -----------------------------
    # Twitter authentication
    # -----------------------------
    # OAuthHandler sets up authentication using API keys
    auth = tweepy.OAuthHandler(access_key, access_secret)

    # Access token is added to complete the authentication process
    auth.set_access_token(consumer_key, consumer_secret)

    # Create the API client
    # This object is used to make requests to Twitter
    api = tweepy.API(auth)

    # -----------------------------
    # Extract tweets
    # -----------------------------
    # user_timeline pulls tweets from a specific user
    # count=200 is the maximum number allowed in one request
    # include_rts=False removes retweets
    # tweet_mode='extended' ensures we get the full tweet text
    tweets = api.user_timeline(
        screen_name='@elonmusk',
        count=200,
        include_rts=False,
        tweet_mode='extended'
    )

    # -----------------------------
    # Transform tweets
    # -----------------------------
    # This list will store cleaned tweet records
    tweet_records = []

    for tweet in tweets:
        # Extract the full tweet text from the raw JSON
        tweet_text = tweet._json["full_text"]

        # Select only the fields needed for analysis
        refined_tweet = {
            "user": tweet.user.screen_name,
            "text": tweet_text,
            "favorite_count": tweet.favorite_count,
            "retweet_count": tweet.retweet_count,
            "created_at": tweet.created_at
        }

        tweet_records.append(refined_tweet)

    # -----------------------------
    # Load data
    # -----------------------------
    # Convert the list of tweets into a DataFrame
    df = pd.DataFrame(tweet_records)

    # Write the refined tweets to a CSV file
    # Output remains the same as the original script
    df.to_csv("refined_tweets.csv")
