# import packages
from combocurve_api_v1 import ServiceAccount, ComboCurveAuth
from combocurve_api_v1.pagination import get_next_page_url
import requests
import numpy as np
import json
import pandas as pd
from requests.models import Response
from dotenv import load_dotenv
import os

load_dotenv()

# adding the Master Battery List for Analysis
masterAllocationList = pd.read_excel(
    r"C:\Users\mtanner\OneDrive - King Operating\Documents 1\code\kingoperating\data\masterWellAllocation.xlsx"
)

# connect to service account
service_account = ServiceAccount.from_file(os.getenv("API_SEC_CODE_LIVE"))
# set API Key from enviroment variable
api_key = os.getenv("API_KEY_PASS_LIVE")
# specific Python ComboCurve authentication
combocurve_auth = ComboCurveAuth(service_account, api_key)

projectId = "612fc3d36880c20013a885df"
scenarioId = "632e70eefcea66001337cd43"

# This code chunk gets the Monthly Cash Flow for given Scenerio
# Call Stack - Get Econ Id

auth_headers = combocurve_auth.get_auth_headers()
# URl econid
url = (
    "https://api.combocurve.com/v1/projects/"
    + projectId
    + "/scenarios/"
    + scenarioId
    + "/econ-runs"
)

response = requests.request(
    "GET", url, headers=auth_headers
)  # GET request to pull economic ID for next query

jsonStr = response.text  # convert to JSON string
dataObjBetter = json.loads(jsonStr)  # pass to data object - allows for parsing
row = dataObjBetter[0]  # sets row equal to first string set (aka ID)
econId = row["id"]  # set ID equal to variable

print(econId)  # check that varaible is passed correctly


# Reautenticated client
auth_headers = combocurve_auth.get_auth_headers()
# set new url with econRunID, skipping zero

urltwo = (
    "https://api.combocurve.com/v1/projects/"
    + projectId
    + "/scenarios/"
    + scenarioId
    + "/econ-runs/"
    + econId
    + "/one-liners"
)

resultsList = []
wellIdList = []


def process_page(response_json):
    for i in range(0, len(response_json)):
        results = response_json[i]
        wellId = results["well"]
        output = results["output"]
        wellIdList.append(wellId)
        resultsList.append(output)


has_more = True

while has_more:
    response = requests.request("GET", urltwo, headers=auth_headers)
    urltwo = get_next_page_url(response.headers)
    process_page(response.json())
    has_more = urltwo is not None

numEntries = len(resultsList)

listOfBatteryIds = masterAllocationList["Id in Greasebooks"].tolist()
wellNameAccountingList = masterAllocationList["Name in Accounting"].tolist()
accountingIdList = masterAllocationList["Subaccount"].tolist()
apiList = masterAllocationList["API"].tolist()
wellIdScenariosList = masterAllocationList["Well Id"].tolist()

headers = [
    "API",
    "Well Name",
    "Abandonment Date",
    "Gross Oil Well Head Volume",
    "Gross Gas Well Head Volume"
]

eurData = pd.DataFrame(columns=headers)

for i in range(0, numEntries):
    row = resultsList[i]
    wellId = wellIdList[i]

    if wellId not in wellIdScenariosList:
        continue
    else:
        wellIdIndex = wellIdScenariosList.index(wellId)
        apiNumber = apiList[wellIdIndex]
        wellName = wellNameAccountingList[wellIdList.index(wellId)]
        abandonmentDate = row["abandonmentDate"]
        grossOilWellHeadVolume = row["grossOilWellHeadVolume"]
        grossGasWellHeadVolume = row["grossGasWellHeadVolume"]

        printRow = {"API": apiNumber, "Well Name": wellName, "Abandonment Date": abandonmentDate,
                    "Gross Oil Well Head Volume": grossOilWellHeadVolume, "Gross Gas Well Head Volume": grossGasWellHeadVolume}

    eurData = eurData.append(printRow, ignore_index=True)

eurData.to_excel(
    r"C:\Users\mtanner\OneDrive - King Operating\Documents 1\code\kingoperating\data\eurDataworland.xlsx", index=False)

print('done')
