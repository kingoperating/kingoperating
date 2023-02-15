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
from runAfe import nameOfWell

pathOfAfe = r".\kingops\data\afe" + "\\" + nameOfWell
budgetRawString = pathOfAfe + "\\" + nameOfWell + "AfeOg.xlsx"
actualSpendString = pathOfAfe + "\\" + nameOfWell + "ActualSpend.xlsx"

# import files needed
budgetRawFile = pd.read_excel(budgetRawString)
masterMatchFile = pd.read_excel(
    r".\kingops\data\afe\welldriveWolfepakMatch.xlsx")
actualWellCostWolfepak = pd.read_excel(actualSpendString)

# create lists from files
welldriveBudgetAccounts = masterMatchFile["Code WellDrive"].tolist()
wolfepakActualAccounts = masterMatchFile["Code WolfePak"].tolist()
wolfepakActualDesc = masterMatchFile["Description WolfePak"].tolist()

actualAccountCodeList = actualWellCostWolfepak["Account"].tolist()
actualAccountDescList = actualWellCostWolfepak["{Account Desc}"].tolist()
actualCostList = actualWellCostWolfepak["Amount"].tolist()

# create empty lists needed for loop
outputData = []
accountCodeInBudgetList = []

filePointerString = pathOfAfe + "\\" + nameOfWell + "AfeActualVarience.csv"

fp = open(filePointerString, "w")

headerString = "Account,Account Description,AFE Budget,Actual Spend,Varience\n"
fp.write(headerString)

# Master loop in order to match budget to actual
for i in range(0, len(budgetRawFile)):
    # Budget row
    rowAfeBudgetRaw = budgetRawFile.iloc[i]
    afeBudgetAccountCode = rowAfeBudgetRaw["Account"]
    afeBudgetCost = rowAfeBudgetRaw["Cost"]

    # gets index of buget account code in actual account code
    indexBudget = welldriveBudgetAccounts.index(afeBudgetAccountCode)
    # matches with actual account code
    actualAccountCode = wolfepakActualAccounts[indexBudget]
    accountActualOccurrences = [j for j, x in enumerate(
        actualAccountCodeList) if x == actualAccountCode]

    # handling multiple transactions with same account code
    if accountActualOccurrences == []:
        actualCostClean = 0
    else:
        actualCostClean = 0
        for m in range(0, len(accountActualOccurrences)):
            actualCostClean += actualCostList[accountActualOccurrences[m]]

    indexDesc = wolfepakActualAccounts.index(actualAccountCode)

    outputData.append([actualAccountCode, wolfepakActualDesc[indexDesc],
                      afeBudgetCost, actualCostClean])

    accountCodeInBudgetList.append(actualAccountCode)


outputData.sort(key=lambda x: x[0])
counter = 0

trigger = True
while trigger == True:
    account = outputData[counter][0]
    occurences = [j for j, x in enumerate(outputData) if x[0] == account]
    budgetCost = 0
    for m in range(0, len(occurences)):
        budgetCost += outputData[occurences[m]][2]

    actualCost = outputData[counter][3]
    varience = budgetCost - actualCost

    description = str(outputData[counter][1])
    betterDesc = description.replace(",", "")
    printString = (
        str(account)
        + ","
        + str(betterDesc)
        + ","
        + str(budgetCost)
        + ","
        + str(actualCost)
        + ","
        + str(varience)
        + "\n"
    )
    fp.write(printString)

    counter = occurences[len(occurences) - 1] + 1

    if counter == len(outputData):
        trigger = False


outputDataExtra = []

for i in range(0, len(actualWellCostWolfepak)):
    accountCodeActual = actualWellCostWolfepak.iloc[i]["Account"]
    if accountCodeActual not in accountCodeInBudgetList:
        accountActualOccurrences = [j for j, x in enumerate(
            actualAccountCodeList) if x == accountCodeActual]

        actualCostClean = 0
        for m in range(0, len(accountActualOccurrences)):
            actualCostClean += actualCostList[accountActualOccurrences[m]]

        outputDataExtra.append(
            [accountCodeActual, actualWellCostWolfepak.iloc[i]["{Account Desc}"], 0, actualCostClean])


counter = 0
trigger = True
while trigger == True:
    account = outputDataExtra[counter][0]
    occurences = [j for j, x in enumerate(outputDataExtra) if x[0] == account]

    budgetCost = 0

    actualCost = outputDataExtra[counter][3]
    varience = budgetCost - actualCost

    description = str(outputDataExtra[counter][1])
    betterDesc = description.replace(",", "")
    printString = (
        str(account)
        + ","
        + str(betterDesc)
        + ","
        + str(budgetCost)
        + ","
        + str(actualCost)
        + ","
        + str(varience)
        + "\n"
    )
    fp.write(printString)

    counter = occurences[len(occurences) - 1] + 1

    if counter == len(outputDataExtra):
        trigger = False


fp.close()


print("Done with AFE Varience")
