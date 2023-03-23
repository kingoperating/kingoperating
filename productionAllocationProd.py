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
fullProductionPull = False
numberOfDaysToPull = 35

fileNameAccounting = (
    r"C:\Users\mtanner\OneDrive - King Operating\Documents 1\code\kingoperating\data\accountingAllocatedProduction.csv"
)

fileNameComboCurve = (
    r"C:\Users\mtanner\OneDrive - King Operating\Documents 1\code\kingoperating\data\comboCurveAllocatedProduction.csv")

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


# Set production interval based on boolen
if fullProductionPull == True:
    productionInterval = "&start=2021-04-01&end="
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
    totalAccountingAllocatedProduction = pd.read_csv(fileNameAccounting)
    totalComboCurveAllocatedProduction = pd.read_csv(fileNameComboCurve)
else:
    headerList = [
        "Date",
        "Client",
        "Subaccount",
        "Well Accounting Name",
        "Oil Volume",
        "Gas Volume",
        "Water Volume",
        "Oil Sold Volume"
    ]
    totalAccountingAllocatedProduction = pd.DataFrame(columns=headerList)

    headerCombocurve = [
        "Date",
        "Client",
        "API",
        "Well Accounting Name",
        "Oil Volume",
        "Gas Volume",
        "Water Volume",
        "Oil Sold Volume",
        "Data Source"
    ]
    totalComboCurveAllocatedProduction = pd.DataFrame(columns=headerCombocurve)

# a bunch of variables the below loop needs
wellIdList = []
wellNameList = []
wellIdOilSoldList = []
wellVolumeOilSoldList = []
gotDayData = np.full([200], False)
totalOilVolume = 0
totalGasVolume = 0
totalWaterVolume = 0
welopOilVolume = 0
welopGasVolume = 0
welopWaterVolume = 0
welopOilSalesVolume = 0
welopCounter = 0
adamsRanchCounter = 0
adamsRanchOilVolume = 0
adamsRanchGasVolume = 0
adamsRanchOilVolume = 0
adamsRanchOilSalesVolume = 0

# Convert all dates to str for comparison rollup
todayYear = int(dateToday.strftime("%Y"))
todayMonth = int(dateToday.strftime("%m"))
todayDay = int(dateToday.strftime("%d"))

# if not pulling all of production - then just get the list of dates to parse
if fullProductionPull == False:
    # gets list of dates
    listOfDates = totalComboCurveAllocatedProduction["Date"].to_list()
    # finds out what date is last
    lastRow = totalComboCurveAllocatedProduction.iloc[len(
        totalComboCurveAllocatedProduction) - 1]
    dateOfLastRow = lastRow["Date"]
    splitDate = re.split("/", str(dateOfLastRow))  # splits date correct
    day = int(splitDate[1])  # gets the correct day
    month = int(splitDate[0])  # gets the correct month
    year = int(splitDate[2])  # gets the correct
    referenceTimeDay = dt.date(year, month, day) - \
        timedelta(days=numberOfDaysToPull)  # creates a reference time
    dateOfInterest = referenceTimeDay.strftime(
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
allocationOilList = masterAllocationList["Allocation Ratio Oil"].tolist()
allocationGasList = masterAllocationList["Allocation Ratio Gas"].tolist()

kComboCurve = 0  # variable for loop
kAccounting = 0  # variable for loop

# gets length of totalAllocatedProduction
initalSizeOfTotalAllocatedProduction = len(totalAccountingAllocatedProduction)

lastDate = ""

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

    subAccountId = []  # empty list for subaccount id
    allocationRatioOil = []  # empty list for allocation ratio oil
    allocationRatioGas = []  # empty list for allocation ratio gas
    subAccountIdIndex = []  # empty list for index of subaccount id INDEX - used for matching

    wellAccountingName = []

    # gets all the indexs that match the battery id (sometimes 1, sometimes more)
    batteryIndexId = [m for m, x in enumerate(
        listOfBatteryIds) if x == batteryId]
    if batteryIndexId == []:
        continue
    # if only 1 index - then just get the subaccount id and allocation ratio
    if len(batteryIndexId) == 1:
        subAccountId = accountingIdList[batteryIndexId[0]]
        allocationRatioOil = allocationOilList[batteryIndexId[0]]
        allocationRatioGas = allocationGasList[batteryIndexId[0]]
        wellAccountingName = wellNameAccountingList[batteryIndexId[0]]
    else:  # if more than 1 index - then need to check if they are the same subaccount id
        # gets allocation for each subaccount
        for t in range(len(batteryIndexId)):
            subAccountId.append(accountingIdList[batteryIndexId[t]])
            allocationRatioOil.append(allocationOilList[batteryIndexId[t]])
            allocationRatioGas.append(allocationGasList[batteryIndexId[t]])
            wellAccountingName.append(
                wellNameAccountingList[batteryIndexId[t]])

    dateString = str(month) + "/" + str(day) + "/" + str(year)

    # clears the counters if the date changes
    if lastDate != dateString:
        welopCounter = 0
        welopGasVolume = 0
        welopOilVolume = 0
        welopWaterVolume = 0
        welopOilSalesVolume = 0
        adamsRanchCounter = 0
        adamsRanchGasVolume = 0
        adamsRanchOilSalesVolume = 0
        adamsRanchOilVolume = 0
        adamsRanchWaterVolume = 0

    # Splits battery name up
    splitString = re.split("-|–", batteryName)
    # sets client name to client name from ETX/STX and GCT
    clientName = splitString[0]
    # if field name exisits - add the batteryName
    if clientName == "CWS ":
        clientName = "KOSOU"
    elif clientName == "Peak ":
        clientName = "KOEAS"
    elif clientName == "Otex ":
        clientName = "KOGCT"
    elif clientName == "Midcon ":
        clientName = "KOAND"
    elif clientName == "Wellman ":
        clientName = "KOPRM"
    elif clientName == "Wellington ":
        clientName = "WELOP"
    elif clientName == "Scurry ":
        clientName = "KOPRM"
    elif clientName == "Wyoming":
        clientName = "KOWYM"

    # CORE LOGIC FOR WELLOP
    if len(batteryIndexId) == 1:
        if batteryId != 23012 and batteryId != 23011:
            newRow = [dateString, clientName, str(subAccountId), str(wellAccountingName), str(oilVolumeClean), str(
                gasVolumeClean), str(waterVolumeClean), str(oilSalesDataClean)]
            newRowComboCurve = [dateString, clientName, str(apiList[batteryIndexId[0]]), str(wellAccountingName), str(oilVolumeClean), str(
                gasVolumeClean), str(waterVolumeClean), str(oilSalesDataClean), "di"]

            totalAccountingAllocatedProduction.loc[startingIndex +
                                                   kAccounting] = newRow  # sets new row to accounting
            totalComboCurveAllocatedProduction.loc[startingIndex +
                                                   kComboCurve] = newRowComboCurve  # sets new row to combo curve
            kComboCurve = kComboCurve + 1  # counter for combo curve
            kAccounting = kAccounting + 1  # counter for accounting

        if batteryId == 23012 or batteryId == 23011:
            adamsRanchOilVolume = adamsRanchOilVolume + oilVolumeClean
            adamsRanchGasVolume = adamsRanchGasVolume + gasVolumeClean
            adamsRanchWaterVolume = adamsRanchWaterVolume + waterVolumeClean
            adamsRanchOilSalesVolume = adamsRanchOilSalesVolume + oilSalesDataClean
            adamsRanchCounter = adamsRanchCounter + 1

        # Handles Adams Ranch and the case where 2 wells feed into 1 subaccount but 2 batteries
        if adamsRanchCounter == 2:
            newRow = [dateString, clientName, str(subAccountId), str(wellAccountingName), str(adamsRanchOilVolume), str(
                adamsRanchGasVolume), str(adamsRanchWaterVolume), str(adamsRanchOilSalesVolume)]
            totalAccountingAllocatedProduction.loc[startingIndex +
                                                   kAccounting] = newRow
            kAccounting = kAccounting + 1
            adamsRanchCounter = 0

    elif len(batteryIndexId) > 1:
        for j in range(len(batteryIndexId)):
            wellOilVolume = oilVolumeClean * allocationRatioOil[j]/100
            wellGasVolume = gasVolumeClean * allocationRatioGas[j]/100
            wellWaterVolume = waterVolumeClean * allocationRatioOil[j]/100
            wellOilSalesVolume = oilSalesDataClean * allocationRatioOil[j]/100

            # YOU ARE HERE
            if batteryId != 25381 and batteryId != 25382:
                newRow = [dateString, clientName, str(apiList[batteryIndexId[j]]), str(wellAccountingName[j]), str(wellOilVolume), str(
                    wellGasVolume), str(wellWaterVolume), str(wellOilSalesVolume), "di"]
                junk = 0
            else:
                newRow = [dateString, clientName, "0" + str(apiList[batteryIndexId[j]]), str(wellAccountingName[j]), str(wellOilVolume), str(
                    wellGasVolume), str(wellWaterVolume), str(wellOilSalesVolume), "di"]

            totalComboCurveAllocatedProduction.loc[startingIndex +
                                                   kComboCurve] = newRow
            kComboCurve = kComboCurve + 1

            if batteryId != 25381 and batteryId != 25382 and batteryId != 23012 and batteryId != 23011:
                newRow = [dateString, clientName, str(subAccountId[j]), str(wellAccountingName[j]), str(wellOilVolume), str(
                    wellGasVolume), str(wellWaterVolume), str(wellOilSalesVolume)]
                totalAccountingAllocatedProduction.loc[startingIndex +
                                                       kAccounting] = newRow
                kAccounting = kAccounting + 1

        if batteryId == 25381 or batteryId == 25382:
            welopOilVolume = welopOilVolume + oilVolumeClean
            welopGasVolume = welopGasVolume + gasVolumeClean
            welopWaterVolume = welopWaterVolume + waterVolumeClean
            welopOilSalesVolume = welopOilSalesVolume + oilSalesDataClean
            welopCounter = welopCounter + 1

        if welopCounter == 2:
            newRow = [dateString, clientName, str(subAccountId[0]), str(subAccountId[0]), str(welopOilVolume), str(
                welopGasVolume), str(welopWaterVolume), str(welopOilSalesVolume)]
            totalAccountingAllocatedProduction.loc[startingIndex +
                                                   kAccounting] = newRow
            kAccounting = kAccounting + 1
            welopCounter = 0

    lastDate = dateString

totalAccountingAllocatedProduction.to_csv(
    r".\kingoperating\data\accountingAllocatedProduction.csv", index=False)
totalComboCurveAllocatedProduction.to_csv(
    r".\kingoperating\data\comboCurveAllocatedProduction.csv", index=False)
totalComboCurveAllocatedProduction.to_json(
    r".\kingoperating\data\comboCurveAllocatedProduction.json", orient="records")


print("Check 2: Allocation Done")
