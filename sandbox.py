import tweepy
import pandas as pd
import numpy as np 
import re

# HELPERS: -------------------------------------------------------------

# Create a function to clean the tweets
def cleanTxt(text):
    text = re.sub('@[A-Za-z0-9]+', '', text) #Removing @mentions
    text = re.sub('#', '', text) # Removing '#' hash tag
    text = re.sub('RT[\s]+', '', text) # Removing RT
    text = re.sub('https?:\/\/\S+', '', text) # Removing hyperlink
    return text

# -----------------------------------------------------------------------

# PARAMS SET:
searchString = "Samsung"
fileName = "SamsungTemp.csv"
tweetCount = 10 # 100 Max only


# Twitter Api Credentials
consumerKey = "6oOOEuhf5y32tC13XGXyJ37Zn"
consumerSecrect = "IENqyXCIMkUkOrtFBZqlUY7YgOGfqP2w7dg97mciuYIiEjSdrB"
tweeterEnvName = "frz_Application"

auth = tweepy.AppAuthHandler(consumerKey, consumerSecrect)
api = tweepy.API(auth, wait_on_rate_limit=True)

mainDf = pd.DataFrame({'full_text': [],
                    'author_name': [],
                    'created_at': []})

# Sample Usage from tweepy docs
# tweepy search API
posts = api.search_30_day(tweeterEnvName, searchString, count=tweetCount, lang="en", tweet_mode="extended")

# Put results to pandas dataframe
# df = pd.DataFrame([tweet.user.name for tweet in posts],
#         columns=['Tweets'])
pagedf = pd.DataFrame({'full_text': [tweet.full_text for tweet in posts],
                    'author_name': [tweet.user.name for tweet in posts],
                    'created_at': [tweet.created_at for tweet in posts]})

mainDf = mainDf.append(pagedf)

# Clean the tweets
# df['full_text'] = df['full_text'].apply(cleanTxt)
mainDf.to_csv(fileName)
print(mainDf)