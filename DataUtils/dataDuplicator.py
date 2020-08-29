import pandas as pd

# PARAMS SET:
inputFileName = "SamsungTweetsData.csv"
outputFileName = "SamsungDataDuplicated2.csv"
numberOfDuplicates = 1

# Duplicate Process: 
finalDf = pd.read_csv(inputFileName) # base dataframe which will be outputed

for i in range(numberOfDuplicates):
    finalDf = finalDf.append(pd.read_csv(inputFileName))

# Export into a file
finalDf.to_csv(outputFileName)
print('All lines count: ' + str(len(finalDf)))