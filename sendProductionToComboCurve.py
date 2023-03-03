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
service_account = ServiceAccount.from_file(os.getenv("API_SEC_CODE"))
# set API Key from enviroment variable
api_key = os.getenv("API_KEY_PASS")
# specific Python ComboCurve authentication
combocurve_auth = ComboCurveAuth(service_account, api_key)

print("Authentication Worked")

# bring in total asset production
totalAssetProduction = pd.read_csv(
    r".\kingoperating\data\comboCurveAllocatedProduction.csv")

totalAssetProduction = totalAssetProduction.drop(
    ["Oil Sold Volume", "Well Accounting Name"], axis=1)

totalAssetProduction.rename(
    columns={"Oil Volume": "oil", "Date": "date", "Gas Volume": "gas", "Client": "customText0", "Water Volume": "water", "API": "chosenID", "Data Source": "dataSource"}, inplace=True)


totalAssetProduction.replace({"South Texas": "KOSOU", "East Texas": "KOEAS", "Gulf Coast": "KOGCT",
                             "Midcon": "KOAND", "Permian Basin": "KOPRM", "Colorado": "WELOP", "Scurry": "KOPRM", "Wyoming": "KOWYM"}, inplace=True)

totalAssetProduction.to_json(
    r".\kingoperating\data\totalAssetsProduction.json", orient="records")

totalAssetProductionJson = totalAssetProduction.to_json(orient="records")
cleanTotalAssetProduction = json.loads(totalAssetProductionJson)

keltonTestData = [{
    "date": "2021-04-01",
    "chosenID": "42483310150000",
    "oil": 0.83,
    "gas": 73.0,
    "water": 50.0,
    "dataSource": "di"
}]

url = "https://api.combocurve.com/v1/daily-productions"
auth_headers = combocurve_auth.get_auth_headers()

response = requests.put(url, headers=auth_headers, json=keltonTestData)

responseCode = response.status_code  # sets response code to the current state

# parse as json string
results = response.json()
# setting to length of results
numEntries = len(results)

if responseCode == 200:
    print("Status Code is 200")
    print(str(numEntries) + " entries read")
else:
    print("The Status Code: " + str(response.status_code))


print("yay")
