import pandas as pd

# HELPERS: -------------------------------------------------------------

def tweetDuplicateChecker(tweetsdf, tweetStr):
    t = tweetsdf[tweetsdf['Tweets'] == tweetStr]
    return len(t) > 0

# ----------------------------------------------------------------------

# PARAMS SET:
outputFileName = "SamsungMergedData.csv" # output file name
datasets = ["Samsung1.csv","Samsung2.csv","Samsung3.csv","Samsung4.csv","Samsung5.csv"] # data sets to be merged


# create output dataframe
finalTweetListDf = pd.read_csv(datasets[0]) # base dataframe which will be outputed

# Combine other Dataframes
datasets = datasets[1:] # remove the 1st dataset because its already the final list

for ds in datasets:
    finalTweetListDf = finalTweetListDf.append(pd.read_csv(ds))

# Remove dupliate tweets
finalTweetListDf.drop_duplicates(subset=['Tweets'])

# Export into a file
finalTweetListDf.to_csv(outputFileName)
print('All lines count: ' + str(len(finalTweetListDf)))