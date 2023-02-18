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


# 30 Day Or Full? If False - only looking at last 30 days and appending.
fullProductionPull = True
numberOfDaysToPull = 30

fileName = (
    r"C:\Users\mtanner\OneDrive - King Operating\Documents 1\code\kingoperating\data\totalAssetsProductionAllocation.csv"
)

load_dotenv()  # load ENV

# adding the Master Battery List for Analysis
masterAllocationList = pd.read_excel(
    r"C:\Users\mtanner\OneDrive - King Operating\Documents 1\code\kingoperating\data\masterWellAllocation.xlsx"
)

# set some date variables we will need later
dateToday = dt.datetime.today()
todayYear = dateToday.strftime("%Y")
todayMonth = dateToday.strftime("%m")
todayDay = dateToday.strftime("%d")
dateYes = dateToday - timedelta(days=1)
yesDayString = dateYes.strftime("%d")


# Set production interval based on boolen CHANGE BACK TO MAY 2021
if fullProductionPull == True:
    productionInterval = "&start=2022-12-01&end="
else:
    dateThirtyDays = dateToday - timedelta(days=numberOfDaysToPull)
    dateThirtyDaysYear = dateThirtyDays.strftime("%Y")
    dateThirtyDaysMonth = dateThirtyDays.strftime("%m")
    dateThirtyDaysDay = dateThirtyDays.strftime("%d")
    productionInterval = (
        "&start="
        + dateThirtyDaysYear
        + "-"
        + dateThirtyDaysMonth
        + "-"
        + dateThirtyDaysDay
        + "&end="
    )

# Master API call to Greasebooks
url = (
    "https://integration.greasebook.com/api/v1/batteries/daily-production?apiKey="
    + str(os.getenv("GREASEBOOK_API_KEY"))
    + productionInterval
    + todayYear
    + "-"
    + todayMonth
    + "-"
    + todayDay
)

# Oil Sold Accouting Check URL
date100DaysAgo = dateToday - timedelta(days=100)
date100Year = date100DaysAgo.strftime("%Y")
date100Month = date100DaysAgo.strftime("%m")
date100Day = date100DaysAgo.strftime("%d")
productionInterval = (
    "&start="
    + date100Year
    + "-"
    + date100Month
    + "-"
    + date100Day
    + "&end="
)

urlAccounting = (
    "https://integration.greasebook.com/api/v1/batteries/daily-production?apiKey="
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
# setting to length of results
numEntries = len(results)

if responseCode == 200:
    print("Status Code is 200")
    print(str(numEntries) + " entries read")
else:
    print("The Status Code: " + str(response.status_code))

# checks if we need to pull all the data or just last specificed
if fullProductionPull == False:
    # Opening Master CSV for total allocation production by Subaccount
    totalAllocatedProduction = pd.read_csv(fileName)
else:
    headerList = [
        "Date",
        "Client",
        "Subaccount",
        "Well Accounting Name",
        "Oil Volume",
        "Gas Volume",
        "Oil Sold Volume"
    ]
    totalAllocatedProduction = pd.DataFrame(
        0, index=np.arange(numEntries - 1), columns=headerList
    )

# a bunch of variables the below loop needs
wellIdList = []
wellNameList = []
wellIdOilSoldList = []
wellVolumeOilSoldList = []
gotDayData = np.full([200], False)
totalOilVolume = 0
totalGasVolume = 0
totalWaterVolume = 0

# Convert all dates to str for comparison rollup
todayYear = int(dateToday.strftime("%Y"))
todayMonth = int(dateToday.strftime("%m"))
todayDay = int(dateToday.strftime("%d"))

# if not pulling all of production - then just get the list of dates to parse
if fullProductionPull == False:
    # gets list of dates
    listOfDates = totalAllocatedProduction["Date"].to_list()
    # finds out what date is last
    lastRow = totalAllocatedProduction.iloc[len(totalAllocatedProduction) - 1]
    dateOfLastRow = lastRow["Date"]
    splitDate = re.split("/", str(dateOfLastRow))  # splits date correct
    day = int(splitDate[1])  # gets the correct day
    month = int(splitDate[0])  # gets the correct month
    year = int(splitDate[2])  # gets the correct
    referenceTime15Day = dt.date(year, month, day) - \
        timedelta(days=15)  # creates a reference time
    dateOfInterest = referenceTime15Day.strftime(
        "%#m/%#d/%Y")  # converts to string
    startingIndex = listOfDates.index(
        dateOfInterest)  # create index surrounding
else:
    startingIndex = 0

# Gets list of Battery id's that are clean for printing
listOfBatteryIds = masterAllocationList["Id in Greasebooks"].tolist()
wellNameAccountingList = masterAllocationList["Name in Accounting"].tolist()
accountingIdList = masterAllocationList["Subaccount"].tolist()
apiList = masterAllocationList["API"].tolist()
allocationList = masterAllocationList["Allocation Ratio"].tolist()

k = 0

initalSizeOfTotalAllocatedProduction = len(totalAllocatedProduction)

# MASTER loop that goes through each of the items in the response
for currentRow in range(numEntries - 1, 0, -1):
    row = results[currentRow]  # get row i in results
    keys = list(row.items())  # pull out the headers

    # set some intial variables for core logic
    oilDataExist = False
    gasDataExist = False
    waterDataExist = False
    oilSalesDataExist = False
    oilVolumeClean = 0
    gasVolumeClean = 0
    waterVolumeClean = 0
    oilSalesDataClean = 0

    # Loops through each exposed API variable. If it exisits - get to correct variable
    for idx, key in enumerate(keys):
        if key[0] == "batteryId":
            batteryId = row["batteryId"]
        elif key[0] == "batteryName":
            batteryName = row["batteryName"]
        elif key[0] == "date":
            date = row["date"]
        # if reported, set to True, otherwise leave false
        elif key[0] == "oil":
            oilDataExist = True
            oilVolumeRaw = row["oil"]
            if oilVolumeRaw == "":  # if "" means it not reported
                oilVolumeClean = 0
            else:
                oilVolumeClean = oilVolumeRaw
        elif key[0] == "mcf":  # same as oil
            gasDataExist = True
            gasVolumeRaw = row["mcf"]
            if gasVolumeRaw == "":
                gasVolumeClean = 0
            else:
                gasVolumeClean = gasVolumeRaw
        elif key[0] == "water":  # same as oil
            waterDataExist = True
            waterVolumeRaw = row["water"]
            if waterVolumeRaw == "":
                waterVolumeClean = 0
            else:
                waterVolumeClean = waterVolumeRaw
        elif key[0] == "oilSales":
            oilSalesDataExist = True
            oilSalesDataRaw = row["oilSales"]
            if oilSalesDataRaw == "":
                oilSalesDataClean = 0
            else:
                oilSalesDataClean = oilSalesDataRaw

    # spliting date correctly
    splitDate = re.split("T", date)
    splitDate2 = re.split("-", splitDate[0])
    year = int(splitDate2[0])
    month = int(splitDate2[1])
    day = int(splitDate2[2])

    # CORE LOGIC BEGINS FOR MASTER LOOP

    # Colorado set MCF to zero
    if batteryId == 25381 or batteryId == 25382:
        gasVolumeClean = 0

    subAccountId = []
    allocationRatio = []
    subAccountIdIndex = []

    batteryIndexId = [m for m, x in enumerate(
        listOfBatteryIds) if x == batteryId]

    if len(batteryIndexId) == 1:
        subAccountId = accountingIdList[batteryIndexId[0]]
        allocationRatio = allocationList[batteryIndexId[0]]
    else:
        for t in range(len(batteryIndexId)):
            subAccountId.append(accountingIdList[batteryIndexId[t]])
            allocationRatio.append(allocationList[batteryIndexId[t]])
            subAccountIdIndex.append(batteryIndexId[t])

        temp = subAccountId[0]
        for d in subAccountId:
            if (temp != d):
                same = False
            else:
                same = True

        if same == True:
            otherSubAccountId = [m for m, x in enumerate(
                accountingIdList) if x == subAccountId[0]]

            temp3 = []
            for element in otherSubAccountId:
                if element not in subAccountIdIndex:
                    temp3.append(element)

            otherBatteryIds = []

            for a in range(len(temp3)):
                otherBatteryIds.append(int(listOfBatteryIds[temp3[a]]))

    # WE ARE RIGHT HERE

    dateString = str(month) + "/" + str(day) + "/" + str(year)

    # Splits battery name up
    splitString = re.split("-|–", batteryName)
    # sets client name to client name from ETX/STX and GCT
    clientName = splitString[0]
    # if field name exisits - add the batteryName
    if clientName == "CWS":
        clientName = "KOSOU"
    elif clientName == "Peak":
        clientName = "KOEAS"
    elif clientName == "Otex":
        clientName = "KOGCT"
    elif clientName == "Midcon":
        clientName = "KOAND"
    elif clientName == "Wellman":
        clientName = "KOPRM"
    elif clientName == "Wellington":
        clientName = "WELOP"
    elif clientName == "Scurry":
        clientName = "KOPRM"

    if len(batteryIndexId) == 1:
        newRow = [dateString, clientName, str(subAccountId), str(oilVolumeClean), str(
            gasVolumeClean), str(waterVolumeClean), str(oilSalesDataClean)]

        totalAllocatedProduction.loc[startingIndex + k]
        k = k + 1

    elif len(batteryIndexId) > 1:
        for j in range(len(batteryIndexId)):
            wellOilVolume = oilVolumeClean * allocationRatio[j]
            wellGasVolume = gasVolumeClean * allocationRatio[j]
            wellWaterVolume = waterVolumeClean * allocationRatio[j]
            wellOilSalesVolume = oilSalesDataClean * allocationRatio[j]

            newRow = [dateString, clientName, str(subAccountId), str(oilVolumeClean), str(
                gasVolumeClean), str(waterVolumeClean), str(oilSalesDataClean)]

            totalAllocatedProduction.loc[startingIndex + k]
            k = k + 1


print("Done Allocation Production")
