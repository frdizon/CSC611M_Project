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
searchString = "Samsung lang:en"
fileName = "Samsung5.csv"
tweetCount = 100 # 100 Max only


# Twitter Api Credentials
consumerKey = "6oOOEuhf5y32tC13XGXyJ37Zn"
consumerSecrect = "IENqyXCIMkUkOrtFBZqlUY7YgOGfqP2w7dg97mciuYIiEjSdrB"

auth = tweepy.AppAuthHandler(consumerKey, consumerSecrect)
api = tweepy.API(auth, wait_on_rate_limit=True)

# Sample Usage from tweepy docs
# tweepy search API
posts = api.search(searchString, count=tweetCount, lang="en")

# Put results to pandas dataframe
# df = pd.DataFrame([tweet.full_text for tweet in posts],
#         columns=['Tweets'])
df = pd.DataFrame({'text': [tweet.text for tweet in posts],
        'author_name': [tweet.user.name for tweet in posts],
        'created_at': [tweet.created_at for tweet in posts]})

# Clean the tweets
# df['Tweets'] = df['Tweets'].apply(cleanTxt)
df.to_csv(fileName)
print(df)