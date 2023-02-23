# Import greasebook variables

from productionFinalGreasebook import (
    dateToday,
    yesTotalOilVolume,
    yesTotalGasVolume,
    oilChangeDaily,
    gasChangeDaily,
    twoDayGasVolume,
    twoDayOilVolume,
    notReportedListOil,
    notReportedListGas,
    dateTwoDaysAgo,
    pumperNotReportedList,
    listOfBatteryIds,
    goodBatteryNames,
    pumperNames,
    read342OilProd,
    read342GasProd,
    thurman23VOilProd,
    thurman23VGasProd,
    irs531OilProd,
    irs531GasProd,
    pshigoda752GasProd,
    pshigoda752OilProd,
    wellVolumeOilSoldList,
    wellVolumeOilSoldListRound,
    prettyNameWellOilSoldList,
    monthlyOilSales
)

# Important packages needed
import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import os.path
from dotenv import load_dotenv
from datetime import datetime
import pandas as pd


# Load the .env
load_dotenv()
# create today's string
todayDateString = dateToday.strftime("%m/%d/%Y")
twoDayAgoDateString = dateTwoDaysAgo.strftime("%B %d, %Y")

if "Pshigoda 752-1H" in notReportedListOil:
    pshigoda752OilProd = "Not Reported"
if "Pshigoda 752-1H" in notReportedListGas:
    pshigoda752GasProd = "Not Reported"

if "Irvin Sisters 53 1H" in notReportedListOil:
    irs531OilProd = "Not Reported"
if "Irvin Sisters 53 1H" in notReportedListGas:
    irs531GasProd = "Not Reported"

if "Thurman #23V-2" in notReportedListOil:
    thurman23VOilProd = "Not Reported"
if "Thurman #23V-2" in notReportedListGas:
    thurman23VGasProd = "Not Reported"

if "Read 34-2H" in notReportedListOil:
    read342OilProd = "Not Reported"
if "Read 34-2H" in notReportedListGas:
    read342GasProd = "Not Reported"


# create function to send email
def send_email(email_recipient, email_subject, email_message):
    email_sender = os.getenv("USERNAME_KING")
    msg = MIMEMultipart()
    msg["From"] = email_sender
    msg["To"] = email_recipient
    msg["Subject"] = email_subject
    msg.attach(MIMEText(email_message, "plain"))

    productionFileGood = (r".\kingoperating\data\totalAssetsProduction.csv")
    oilGasReportedFile = (r".\kingoperating\data\yesterdayWellReport.csv")

    # OPENS EACH ATTACHMENTS

    with open(productionFileGood, "rb") as attachment:
        # Add file as application/octet-stream
        # Email client can usually download this automatically as attachment
        part = MIMEBase("application", "octet-stream")
        part.set_payload(attachment.read())
    with open(oilGasReportedFile, "rb") as attachment:
        # Add file as application/octet-stream
        # Email client can usually download this automatically as attachment
        partTwo = MIMEBase("application", "octet-stream")
        partTwo.set_payload(attachment.read())

    # Encode file in ASCII characters to send by email
    encoders.encode_base64(part)
    encoders.encode_base64(partTwo)

    # Add header as key/value pair to attachment part
    part.add_header(
        "Content-Disposition",
        f"attachment; filename= totalAssetProduction.csv",
    )
    partTwo.add_header(
        "Content-Disposition",
        f"attachment; filename= twoDayAgoWellReport.csv",
    )

    # ATTACHES EACH FILE TO EMAIL
    msg.attach(part)
    msg.attach(partTwo)

    text = msg.as_string()

    try:
        server = smtplib.SMTP("smtp.office365.com", 587)
        server.ehlo()
        server.starttls()
        server.login(os.getenv("USERNAME_KING"), os.getenv("PASSWORD_KING"))
        server.sendmail(email_sender, email_recipient, text)
        print("email sent")
        server.quit()
    except:
        print("SMPT server connection error")

    return True


# import links
dashboardLink = os.getenv("DASHBOARD_URL")
wellList = os.getenv("MASTER_BATTERY_LIST")

# Body of the email mesasge

message = "L.D Brown Unit is down - engineering is aware and troubleshooting \n\n"

message = message + (
    "Oil production: "
    + str(round(twoDayOilVolume, 1))
    + " bbl \n\n"
    + "Gas production: "
    + str(round(twoDayGasVolume, 1))
    + " mcf \n\n"
    + "Read 34-2H Production - 50% WI\n"
    + "   "
    + str(read342OilProd)
    + " bbl \n"
    + "   "
    + str(read342GasProd)
    + " mcf \n"
    + "Thurman 23V #2 Production\n"
    + "   "
    + str(thurman23VOilProd)
    + " bbl\n"
    + "   "
    + str(thurman23VGasProd)
    + " mcf"
    + "\nIrvin Sisters 53M-#1H\n"
    + "   "
    + str(irs531OilProd)
    + " bbl\n"
    + "   "
    + str(irs531GasProd)
    + " mcf"
    + str("\n")
    + "Pshigoda 752-#1H\n"
      + "   "
    + str(pshigoda752OilProd)
    + " bbl\n"
    + "   "
    + str(pshigoda752GasProd)
    + " mcf"
)

message = message + "\n\n" + "Monthly Oil Sales Highlights" + \
    "\n" + "-------------------------------------" + "\n"

message = message + "Monthly Total: " + str(round(monthlyOilSales, 2)) + " bbl"

message = message + "\nWell Oil Sold List:"

for i in range(0, len(wellVolumeOilSoldList)):
    message = message + "\n  " + prettyNameWellOilSoldList[i]
    message = message + " -- " + \
        str(wellVolumeOilSoldListRound[i]) + " bbl"

message = message + \
    "\n\nView the Dashboard in Teams (KOC Field Operations) PowerBi Mobile Application or here (if numbers are not updated, try again in 30 min or reply to this email): " + dashboardLink

message = message + "\n\n" + "Not Reported List by Pumper Route" + \
    "\n" + "----------------------------------------------------" + "\n"


# loops over notReportedList and orgainzes by pumper for easy viewing
for i in range(0, len(pumperNotReportedList)):
    pumper = pumperNotReportedList[i]
    message = message + "Pumper Name: " + pumperNotReportedList[i]
    for j in range(0, len(notReportedListOil)):
        name = notReportedListOil[j]
        index = goodBatteryNames.index(name)
        if pumperNames[index] == pumper:
            message = message + "\n    " + name
    for m in range(0, len(notReportedListGas)):
        name = notReportedListGas[m]
        index = goodBatteryNames.index(name)
        if pumperNames[index] == pumper:
            if name not in notReportedListOil:
                message = message + "\n    " + name
    message = message + "\n"

# email subject
subject = "Daily Production Report KOP Assets - " + twoDayAgoDateString
print(subject)

# Potenital users to send to
michaelTanner = os.getenv("MICHAEL_TANNER")
jayYoung = os.getenv("JAY_YOUNG")
rexGifford = os.getenv("REX_GIFFORD")
chandlerKnox = os.getenv("CHANDLER_KNOX")
paulGerome = os.getenv("PAUL_GEROME")
craigHaesly = os.getenv("CRAIG_HAESLY")
peterSnell = os.getenv("PETER_SNELL")
garretStacey = os.getenv("GARRET_STACEY")
grahamPatterson = os.getenv("GRAHAM_PATTERSON")
wesMinshall = os.getenv("WES_MINSHALL")

# LIST TO SEND TO
send_email(
    michaelTanner,
    subject,
    message,
)

# LIST TO SEND TO
send_email(
    wesMinshall,
    subject,
    message,
)

send_email(
    rexGifford,
    subject,
    message,
)

send_email(
    paulGerome,
    subject,
    message,
)

send_email(
    garretStacey,
    subject,
    message,
)


send_email(
    chandlerKnox,
    subject,
    message,
)

send_email(
    peterSnell,
    subject,
    message,
)

send_email(
    grahamPatterson,
    subject,
    message,
)

print("All Email's Sent")
