import requests
import json
import datetime as dt
import re
import pandas as pd


# Function to split date from JOYN API into correct format - returns date in format of 5/17/2023 from format of 2023-05-17T00:00:00
def splitDate(badDate):
    splitDate = re.split("T", date)
    splitDate2 = re.split("-", splitDate[0])
    year = int(splitDate2[0])
    month = int(splitDate2[1])
    day = int(splitDate2[2])
    dateString = str(month) + "/" + str(day) + "/" + str(year)

    return dateString

# Function to authenticate JOYN API - returns idToke used as header for authorization in other API calls


def getIdToken():
    # User Token API
    url = "https://api.joyn.ai/common/user/token"
    # Payload for API - use JOYN crdentials
    payload = {
        "uname": "mtanner@kingoperating.com",
        "pwd": "Bigshow1637%"
    }
    # Headers for API - make sure to use content type of json
    headers = {
        "Content-Type": "application/json"
    }

    # dump payload into json format for correct format
    payloadJson = json.dumps(payload)
    response = requests.request(
        "POST", url, data=payloadJson, headers=headers)  # make request
    if response.status_code == 200:  # 200 = success
        print("Successful JOYN Authentication")
    else:
        print(response.status_code)

    results = response.json()  # get response in json format
    idToken = results["IdToken"]  # get idToken from response

    return idToken


# Begin Script
idToken = getIdToken()  # get idToken from authJoyn function

# set correct URL for Reading Data API JOYN - use idToken as header for authorization
url = "https://api-fdg.joyn.ai/admin/api/ReadingData?isCustom=true&entityids=15408&fromdate=2023-05-17&todate=2023-05-21&pagesize=1000&pagenumber="

pageNumber = 1  # set page number to 1
nextPage = True
totalResults = []  # create empty list to store results

while nextPage == True:  # loop through all pages of data
    url = url + str(pageNumber)  # add page number to url
    response = requests.request("GET", url, headers={"Authorization": idToken})

    if response.status_code != 200:
        print(response.status_code)

    print("Length of Response: " + str(len(response.json())))

    resultsReadingType = response.json()
    totalResults.append(resultsReadingType)

    if len(resultsReadingType) == 0:
        # triggers while loop to end when no more data is returned and length of response is 0
        nextPage = False
    pageNumber = pageNumber + 1  # increment page number by 1 for pagination
    url = url[:-1]  # removes last character from url to reset for next page

readingVolume = 0

headers = ["AssetId", "ReadingVolume", "NetworkName", "Date", "Product"]
totalAssetProduction = pd.DataFrame(columns=headers)

for i in range(0, len(totalResults)):
    for j in range(0, len(totalResults[i])):
        # JOYN unquie ID for each asset
        assetId = totalResults[i][j]["assetId"]
        # reading volume for current allocation row
        readingVolume = totalResults[i][j]["Volume"]
        # network name for current allocation row
        networkName = totalResults[i][j]["NetworkName"]
        # reading date for current allocation row
        date = totalResults[i][j]["ReadingDate"]
        dateBetter = splitDate(date)  # runs splitdate() into correct format
        product = totalResults[i][j]["Product"]
        row = [assetId, readingVolume, networkName, dateBetter, product]
        totalAssetProduction.loc[len(totalAssetProduction)] = row


totalAssetProduction.to_csv(
    r"C:\Users\mtanner\OneDrive - King Operating\Documents 1\code\kingoperating\data\totalAssetProductionJoyn.csv", index=False)
print("yay")
