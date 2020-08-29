import pandas as pd

# PARAMS SET:
inputFileName = "SamsungDataFinal.csv"
outputFileName = "SamsungDataPart" # do not include filename extension (.csv)
itemsPerFile = 5000


# Seperate Process:

fullData = pd.read_csv(inputFileName) # base dataframe which will be outputed

index = 1
rowCount = int(len(fullData))

for offsetIndex in range(0, rowCount, itemsPerFile):
    partialData = fullData[offsetIndex : offsetIndex+itemsPerFile]
    # print(len(partialData)) # partial file row count
    # print(outputFileName + str(index) + '.csv') # partial file name
    partialData.to_csv(outputFileName + str(index) + '.csv')
    index += 1