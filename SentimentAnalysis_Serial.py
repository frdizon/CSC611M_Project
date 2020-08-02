from textblob import TextBlob
import pandas as pd
import numpy as np 
import re

# HELPERS: -------------------------------------------------------------

# Create a function to get the subjectivity
def getSubjectivity(text):
    return TextBlob(text).sentiment.subjectivity

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

# PARAMS SET:
fileName = 'Microsoft.csv'

# Read csv, put it in dataframe
df = pd.read_csv(fileName)


# Calculate Max Category count (Approach 1)-------------------------------

# Set Polarity Categories count
positiveCount = 0 # n < -0.05
neutralCount = 0 # -0.05 <= n <= 0.05
negativeCount = 0 # n > 0.05

#  this can be parallelized
for tweet in df['Tweets']:
    polarityValue = getPolarity(tweet)
    # Critical Section Start
    if -0.05 <= polarityValue and polarityValue <= 0.05:
        neutralCount = neutralCount + 1
    elif polarityValue < -0.05:
        negativeCount = negativeCount + 1
    elif polarityValue > 0.05:
        positiveCount = positiveCount + 1
    # Critical Section End
# Print of results (all critical section should be done first before this)
mostPolarity = 'Positive'
mostPolarityValue = positiveCount
if mostPolarityValue < neutralCount:
    mostPolarityValue = neutralCount
    mostPolarity = 'Neutral'
if mostPolarityValue < negativeCount:
    mostPolarityValue = negativeCount
    mostPolarity = 'Negative'
print('APPROACH 1 Results:')
print(mostPolarity + '( Positive: ' + str(positiveCount) 
        + ', Neutral: ' + str(neutralCount), ', Negative: ' + str(negativeCount) + ')')

# Approach 1 END---------------------------------------------------------

# Calculate Average Polarity (Approach 2)-------------------------------

# Set Polarity Categories count

averagePolarityVal = 0

#  this can be parallelized
for tweet in df['Tweets']:
    polarityValue = getPolarity(tweet)
    # Critical Section Start
    averagePolarityVal = averagePolarityVal + polarityValue
    # Critical Section End
averagePolarityVal = averagePolarityVal / len(df.index)
# Print of results (all critical section should be done first before this)

print('APPROACH 2 Results:')
if -0.05 <= averagePolarityVal and averagePolarityVal <= 0.05:
    print("Neutral (Value: " + str(averagePolarityVal) + ')')
elif averagePolarityVal < -0.05:
    print("Negative (Value: " + str(averagePolarityVal) + ')')
elif averagePolarityVal > 0.05:
    print("Positive (Value: " + str(averagePolarityVal) + ')')

# # Approach 2 END---------------------------------------------------------