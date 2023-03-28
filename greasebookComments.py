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

load_dotenv()

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
    print("Status( Code is 200")
    print(str(len(results)) + " entries read")
else:
    print("The Status Code: " + str(response.status_code))

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


print("yay")
