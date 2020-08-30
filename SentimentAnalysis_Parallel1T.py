from textblob import TextBlob
import pandas as pd
import numpy as np 
import re
import time
from threading import Thread, Lock

# HELPERS: -------------------------------------------------------------

# Create a function to get the polarity
def getPolarity(text):
    return TextBlob(text).sentiment.polarity

# Create a function to compute negative(-1), neutral(0), positive(+1) analysis
def getAnalysis(score):
    if score < 0:
        return 'Negative'
    elif score == 0:
        return 'Neutral'
    else:
        return 'Positive'

# -----------------------------------------------------------------------

# Process: -------------------------------------------------------------

# Initialize Polarity Categories count
positiveCount = 0 # n < -0.05
neutralCount = 0 # -0.05 <= n <= 0.05
negativeCount = 0 # n > 0.05

class EvaluateTweetsProcess(Thread):
    def __init__(self, lock, tweets):
        Thread.__init__(self)
        self.tweets = tweets
        self.lock = lock

    def run(self):

        global positiveCount
        global neutralCount
        global negativeCount

        for tweet in self.tweets['text']:
            polarityValue = getPolarity(tweet)
            # Critical Section Start
            self.lock.acquire()
            if -0.05 <= polarityValue and polarityValue <= 0.05:
                neutralCount += 1
            elif polarityValue < -0.05:
                negativeCount += 1
            elif polarityValue > 0.05:
                positiveCount += 1
            self.lock.release()
    # Critical Section End

# -----------------------------------------------------------------------

if __name__ == '__main__':

    # PARAMS SET:
    fileName = 'SamsungDataFinalX4.csv'
    processCount = 5

    timeStart1 = time.time()

    # Read csv, put it in dataframe
    df = pd.read_csv(fileName)

    # NLP Process Start -----------------------------------------------------

    lock = Lock()
    processList = []

    for processI in range(processCount):
        evalTweetsProcess = EvaluateTweetsProcess(lock, df[processI::processCount])
        evalTweetsProcess.start()
        processList.append(evalTweetsProcess)

    for evalProcess in processList:
        evalProcess.join()

    # Print of results (all critical section should be done first before this)
    mostPolarity = 'Positive'
    mostPolarityValue = positiveCount
    if mostPolarityValue < neutralCount:
        mostPolarityValue = neutralCount
        mostPolarity = 'Neutral'
    if mostPolarityValue < negativeCount:
        mostPolarityValue = negativeCount
        mostPolarity = 'Negative'
    totalCount = positiveCount + neutralCount + negativeCount
    print('APPROACH 1 Results:') # TO DO: Show Percentage
    print(mostPolarity + '( Positive: ' + str(positiveCount) + '(' + str((positiveCount/totalCount) * 100) + '%)' 
            + ', Neutral: ' + str(neutralCount) + '(' + str((neutralCount/totalCount) * 100) + '%)'
            ', Negative: ' + str(negativeCount) + '(' + str((negativeCount/totalCount) * 100) + '%)' + ')')
    print('Time Taken: ' + str(time.time() - timeStart1 ) + 's')
