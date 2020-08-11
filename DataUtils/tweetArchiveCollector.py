import tweepy
import pandas as pd
import numpy as np 
import re
import pprint

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
queryString = "Samsung lang:en"
fileName = "SamsungAug0726.csv"
fromDateCollect = "202007260000"
toDateCollect = "202008012359"
# tweetCount = 10 # 100 Max only


# Twitter Api Credentials
consumerKey = "6oOOEuhf5y32tC13XGXyJ37Zn"
consumerSecrect = "IENqyXCIMkUkOrtFBZqlUY7YgOGfqP2w7dg97mciuYIiEjSdrB"
tweeterEnvName = "PoC"

auth = tweepy.AppAuthHandler(consumerKey, consumerSecrect)
api = tweepy.API(auth, wait_on_rate_limit=True)

mainDf = pd.DataFrame({'text': [],
                    'author_name': [],
                    'created_at': []})

for tweetsPage in tweepy.Cursor(api.search_30_day, tweeterEnvName, queryString, fromDate=fromDateCollect, toDate=toDateCollect).pages(100):
    pagedf = pd.DataFrame({'text': [tweet.text for tweet in tweetsPage],
                    'author_name': [tweet.user.name for tweet in tweetsPage],
                    'created_at': [tweet.created_at for tweet in tweetsPage]})
    mainDf = mainDf.append(pagedf)

# Clean the tweets
# df['full_text'] = df['full_text'].apply(cleanTxt)
mainDf.to_csv(fileName)
print(mainDf)