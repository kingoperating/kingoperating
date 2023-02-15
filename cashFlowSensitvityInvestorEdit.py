import pandas as pd
import re

cleanInvestor = pd.read_csv(
    r".\kingops\data\fundTwoModel.csv"
)
investorPaymentFp = open(
    r".\kingops\data\fundTwoModelInvestor.csv",
    "w",
)
headerString = (
    "Orginal Production Rev Date,Investor Payment Date," +
    "Amount ($),Price Deck\n"
)
investorPaymentFp.write(headerString)
for i in range(0, len(cleanInvestor)):
    row = cleanInvestor.iloc[i]
    dateStr = row["Date"]
    payment = row["Projected Investor Cash Flow"]
    priceDeck = row["Price Deck"]
    splitDate = re.split("-", dateStr)
    if len(splitDate) == 3:
        year = int(splitDate[0])
        month = int(splitDate[1])
        day = int(splitDate[2])
    else:
        splitDate = re.split("/", dateStr)
        year = int(splitDate[2])
        month = int(splitDate[0])
        day = int(splitDate[1])
    if month <= 10:
        newMonth = month + 2
        newYear = year
    elif month == 11:
        newMonth = 1
        newYear = year + 1
    elif month == 12:
        newMonth = 2
        newYear = year + 1
    if day < 10:
        if newMonth < 10:
            investorPayDateString = (
                str(newYear) + "-0" + str(newMonth) + "-0" + str(day)
            )
        else:
            investorPayDateString = str(
                newYear) + "-" + str(newMonth) + "-0" + str(day)
    else:
        if newMonth < 10:
            investorPayDateString = str(
                newYear) + "-0" + str(newMonth) + "-" + str(day)
        else:
            investorPayDateString = str(
                newYear) + "-" + str(newMonth) + "-" + str(day)
    investorPaymentFp.write(
        dateStr
        + ","
        + investorPayDateString
        + ","
        + str(payment)
        + ","
        + priceDeck
        + "\n"
    )
investorPaymentFp.close()
print("done")
