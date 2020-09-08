from textblob import TextBlob
import pandas as pd
import numpy as np 
import re
import time
from multiprocessing import Process, Lock, Value

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

class EvaluateTweetsProcess(Process):
    def __init__(self, lock, tweets, positiveCount, neutralCount, negativeCount):
        Process.__init__(self)
        self.tweets = tweets
        self.lock = lock
        self.positiveCount = positiveCount
        self.neutralCount = neutralCount
        self.negativeCount = negativeCount

    def run(self):
        for tweet in self.tweets['text']:
            polarityValue = getPolarity(tweet)
            # Critical Section Start
            self.lock.acquire()
            if -0.05 <= polarityValue and polarityValue <= 0.05:
                self.neutralCount.value += 1
            elif polarityValue < -0.05:
                self.negativeCount.value += 1
            elif polarityValue > 0.05:
                self.positiveCount.value += 1
            self.lock.release()
    # Critical Section End

# -----------------------------------------------------------------------

if __name__ == '__main__':

    # PARAMS SET:
    fileName = 'SamsungDataFinalX4.csv'
    processCount = 8

    timeStart1 = time.time()

    # Read csv, put it in dataframe
    df = pd.read_csv(fileName)

    # NLP Process Start -----------------------------------------------------

    lock = Lock()
    processList = []

    # Initialize Polarity Categories count
    positiveCount = Value('i', 0) # n < -0.05
    neutralCount = Value('i', 0) # -0.05 <= n <= 0.05
    negativeCount = Value('i', 0) # n > 0.05

    for processI in range(processCount):
        evalTweetsProcess = EvaluateTweetsProcess(lock, df[processI::processCount], positiveCount, neutralCount, negativeCount)
        evalTweetsProcess.start()
        processList.append(evalTweetsProcess)

    for evalProcess in processList:
        evalProcess.join()

    # Print of results (all critical section should be done first before this)
    mostPolarity = 'Positive'
    mostPolarityValue = positiveCount.value
    if mostPolarityValue < neutralCount.value:
        mostPolarityValue = neutralCount.value
        mostPolarity = 'Neutral'
    if mostPolarityValue < negativeCount.value:
        mostPolarityValue = negativeCount.value
        mostPolarity = 'Negative'
    totalCount = positiveCount.value + neutralCount.value + negativeCount.value
    print('APPROACH 1 Results:') # TO DO: Show Percentage
    print(mostPolarity + '( Positive: ' + str(positiveCount.value) + '(' + str((positiveCount.value/totalCount) * 100) + '%)' 
            + ', Neutral: ' + str(neutralCount.value) + '(' + str((neutralCount.value/totalCount) * 100) + '%)'
            ', Negative: ' + str(negativeCount.value) + '(' + str((negativeCount.value/totalCount) * 100) + '%)' + ')')
    print('Time Taken: ' + str(time.time() - timeStart1 ) + 's')
