from enverus_developer_api import DeveloperAPIv3
from dotenv import load_dotenv
import requests
import os
import pandas as pd
from datetime import date, datetime, timedelta
import datetime as dt
import re

load_dotenv()

# gets relvent date values
dateToday = dt.datetime.today()
todayYear = dateToday.strftime("%Y")
todayMonth = dateToday.strftime("%m")
todayDay = dateToday.strftime("%d")

todayYear = int(dateToday.strftime("%Y"))
todayMonth = int(dateToday.strftime("%m"))
todayDay = int(dateToday.strftime("%d"))

# authentic the Enverus API
d3 = DeveloperAPIv3(secret_key=os.getenv('ENVERUS_API'))

# Checks mos recent data from Browning Oil FLuvanna Unit
for row in d3.query("production", API_UWI_14_UNFORMATTED="42033325890000"):
    totalProdMonths = row['ProducingDays']
    totalOil = row['Prod_OilBBL']
    updateDate = row['UpdatedDate']
    if "T" in updateDate:
        index = updateDate.index("T")
        updateDateBetter = updateDate[0:index]
    print("Date Of Browning Well Update: " + updateDateBetter)
    print("Daily Oil Rate: " + str(totalOil/totalProdMonths))

dateList = []
browningDate = []
apiList = []

# UPDATES - WORKING DAILY
for row in d3.query("detected-well-pads", ENVOperator="BROWNING OIL", ENVBasin="MIDLAND"):
    updateDateWells = row['UpdatedDate']
    if "T" in updateDateWells:
        index = updateDateWells.index("T")
        updateDateWells = updateDateWells[0:index]
        dateList.append(updateDateWells)

for i in range(0, len(dateList)):
    stringDate = dateList[i]
    splitDate = re.split("-", stringDate)
    day = int(splitDate[2])  # gets the correct day
    month = int(splitDate[1])  # gets the correct month
    year = int(splitDate[0])  # gets the correct

    if year == todayYear and month == todayMonth and day == todayDay:
        for row in d3.query("wells", ENVOperator="BROWNING OIL", ENVBasin="MIDLAND"):
            apiNumber = row['API_UWI_14_Unformatted']
            browningDates = row['UpdatedDate']
            apiList.append(apiNumber)
            browningDate.append(browningDates)
        print("SOMETHING HAS BEEN UPDATED.....")
