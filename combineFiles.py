import pandas as pd

listOfWellsExcel = pd.read_excel(
    r"C:\Users\mtanner\OneDrive - King Operating\Documents 1\code\kingoperating\data\afe\listOfWells.xlsx", sheet_name="Sheet1")
wellNames = listOfWellsExcel["Well Name"].tolist()

afeActualHeaders = ["Account", "Account Description",
                    "AFE Budget", "Actual Spend", "Varience"]
dailyItemCostHeaders = ["Date", "Account Number",
                        "Depth", "Daily Cost", "Description", "Well Name"]
afeActualDataframe = pd.DataFrame(columns=afeActualHeaders)
dailyItemCostDataframe = pd.DataFrame(columns=dailyItemCostHeaders)

for wellName in wellNames:
    pathOfWellName = r"C:\Users\mtanner\OneDrive - King Operating\Documents 1\code\kingoperating\data\afe" + "\\" + wellName

    # Start AFE Actual Variance
    pathofAfeActualVariance = pathOfWellName + "\\" + \
        wellName + "AfeActualVarience.csv"
    afeVarianceWellData = pd.read_csv(pathofAfeActualVariance)
    length = len(afeVarianceWellData)
    wellNameList = [wellName for i in range(length)]
    afeVarianceWellData["Well Name"] = wellNameList
    afeActualDataframe = pd.concat(
        [afeActualDataframe, afeVarianceWellData], axis=0)

    pathOfDailyItemCost = pathOfWellName + "\\" + wellName + "dailyItemCost.csv"
    dailyItemCost = pd.read_csv(pathOfDailyItemCost)
    length = len(dailyItemCost)
    wellNameList = [wellName for i in range(length)]


print("yay")
