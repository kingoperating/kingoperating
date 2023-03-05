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
import json
import pandas as pd
import numpy as np
from combocurve_api_v1 import ServiceAccount, ComboCurveAuth
from combocurve_api_v1.pagination import get_next_page_url

load_dotenv()  # load enviroment variables

# connect to service account
service_account = ServiceAccount.from_file(os.getenv("API_SEC_CODE_LIVE"))
# set API Key from enviroment variable
api_key = os.getenv("API_KEY_PASS_LIVE")
# specific Python ComboCurve authentication
combocurve_auth = ComboCurveAuth(service_account, api_key)

print("Authentication Worked")

# bring in total asset production
totalAssetProduction = pd.read_csv(
    r".\kingoperating\data\comboCurveAllocatedProduction.csv")

# converts API to int (removing decimals) and then back to string for JSON
totalAssetProduction = totalAssetProduction.astype({"API": "int64"})
totalAssetProduction = totalAssetProduction.astype({"API": "string"})

# helps when uploading to ComboCurve to check for length of data (can only send 20,000 data points at a time)
print(len(totalAssetProduction))

# slices the data to only send the last 20,000 data points
totalAssetProduction = totalAssetProduction.iloc[80000:]

# drops columns that are not needed
totalAssetProduction = totalAssetProduction.drop(
    ["Oil Sold Volume", "Well Accounting Name", "Client"], axis=1)

# renames columns to match ComboCurve
totalAssetProduction.rename(
    columns={"Oil Volume": "oil", "Date": "date", "Gas Volume": "gas", "Water Volume": "water", "API": "chosenID", "Data Source": "dataSource"}, inplace=True)

# exports to json for storage
totalAssetProduction.to_json(
    r".\kingoperating\data\totalAssetsProduction.json", orient="records")

totalAssetProductionJson = totalAssetProduction.to_json(
    orient="records")  # converts to internal json format
# loads json into format that can be sent to ComboCurve
cleanTotalAssetProduction = json.loads(totalAssetProductionJson)

# prints length as final check (should be less than 20,000)
print(len(cleanTotalAssetProduction))

# test data
testData = [{
    "date": "2021-05-01",
    "chosenID": "42357337470000",
    "oil": 1,
    "gas": 73.0,
    "water": 60.0,
    "dataSource": "di"
}]


# sets url to daily production
url = "https://api.combocurve.com/v1/daily-productions"
auth_headers = combocurve_auth.get_auth_headers()  # authenticates ComboCurve

# put request to ComboCurve
response = requests.put(url, headers=auth_headers,
                        json=cleanTotalAssetProduction)

responseCode = response.status_code  # sets response code to the current state

print(responseCode)

print("yay")
