# Import packages needed
from ast import keyword
from http import client
from posixpath import split
from time import strftime
from black import out
import requests
import os
from datetime import date, datetime, timedelta
import datetime as dt
import glob
import re
from dotenv import load_dotenv
import pandas as pd
import numpy as np
from combocurve_api_v1 import ServiceAccount, ComboCurveAuth
from combocurve_api_v1.pagination import get_next_page_url
import json

load_dotenv()

# connect to service account
service_account = ServiceAccount.from_file(os.getenv("API_SEC_CODE_LIVE"))
# set API Key from enviroment variable
api_key = os.getenv("API_KEY_PASS_LIVE")
# specific Python ComboCurve authentication
combocurve_auth = ComboCurveAuth(service_account, api_key)

print("Authentication Worked")


# set some date variables we will need later
dateToday = dt.datetime.today()
todayYear = dateToday.strftime("%Y")
todayMonth = dateToday.strftime("%m")
todayDay = dateToday.strftime("%d")
dateYes = dateToday - timedelta(days=1)
yesDayString = dateYes.strftime("%d")

# adding the Master Battery List for Analysis
masterBatteryList = pd.read_csv(
    r"C:\Users\mtanner\OneDrive - King Operating\Documents 1\code\kingoperating\data\masterBatteryList.csv", encoding="windows-1252"
)

masterAllocationList = pd.read_excel(
    r"C:\Users\mtanner\OneDrive - King Operating\Documents 1\code\kingoperating\data\masterWellAllocation.xlsx"
)


productionInterval = "&start=2023-01-01&end="

# Master API call to Greasebooks
url = (
    "https://integration.greasebook.com/api/v1/comments/read?apiKey="
    + str(os.getenv("GREASEBOOK_API_KEY"))
    + productionInterval
    + todayYear
    + "-"
    + todayMonth
    + "-"
    + todayDay
    + "&pageSize=250"
)

# make the API call
response = requests.request(
    "GET",
    url,
)

responseCode = response.status_code  # sets response code to the current state

# parse as json string
results = response.json()

# checks to see if the GB API call was successful
if responseCode == 200:
    print("Status Code is 200")
    print(str(len(results)) + " entries read")
else:
    print("The Status Code: " + str(response.status_code))

# gets all API's from the master allocation list
apiList = masterAllocationList["API"].tolist()
listOfBatteryIds = masterAllocationList["Id in Greasebooks"].tolist()


headerCombocurve = ["Date", "API", "Comment", "Data Source"]

# create a dataframe to hold the results
totalCommentComboCurve = pd.DataFrame(columns=headerCombocurve)

for currentRow in range(0, len(results)):
    row = results[currentRow]
    keys = list(row.items())

    message = ""
    batteryName = ""
    batteryId = 0
    dateOfComment = ""

    for idx, key in enumerate(keys):
        if key[0] == "message":
            message = row["message"]
        elif key[0] == "batteryId":
            batteryId = row["batteryId"]
        elif key[0] == "dateTime":
            dateOfComment = row["dateTime"]

    # spliting date correctly
    splitDate = re.split("T", dateOfComment)
    splitDate2 = re.split("-", splitDate[0])
    year = int(splitDate2[0])
    month = int(splitDate2[1])
    day = int(splitDate2[2])

    dateString = str(month) + "/" + str(day) + "/" + str(year)
    dateStringComboCurve = datetime.strptime(dateString, "%m/%d/%Y")

    batteryIdIndex = listOfBatteryIds.index(batteryId)
    api = apiList[batteryIdIndex]

    apiIdLength = len(str(api))

    if apiIdLength != 14:
        api = "0" + str(api)

    row = [dateStringComboCurve, str(api), str(message), "di"]

    totalCommentComboCurve.loc[len(totalCommentComboCurve)] = row


totalCommentComboCurve = totalCommentComboCurve.astype({"Date": "string"})
totalCommentComboCurve.rename(columns={
                              "Date": "date", "API": "chosenID", "Comment": "operationalTag", "Data Source": "dataSource"}, inplace=True)

totalCommentComboCurveJson = totalCommentComboCurve.to_json(orient="records")
cleanTotalCommentComboCurveJson = json.loads(totalCommentComboCurveJson)

url = "https://api.combocurve.com/v1/daily-productions"
auth_headers = combocurve_auth.get_auth_headers()  # authenticates ComboCurve

# put request to ComboCurve
response = requests.put(url, headers=auth_headers,
                        json=cleanTotalCommentComboCurveJson)

responseCode = response.status_code  # sets response code to the current state
responseText = response.text  # sets response text to the current state

print("Response Code: " + str(responseCode))  # prints response code

if "successCount" in responseText:  # checks if the response text contains successCount
    # finds the index of successCount
    # prints the successCount and the number of data points sent
    indexOfSuccessFail = responseText.index("successCount")
    print(responseText[indexOfSuccessFail:])


print("yay")
