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
totalAssetProduction = pd.read_csv(r".\kingops\data\totalAssetsProduction.csv")

totalAssetProduction.rename(
    columns={"Oil Volume": "oil", "Date": "date", "Gas Volume": "gas", "Client": "customText0"}, inplace=True)

totalAssetProduction.replace({"South Texas": "KOSOU", "East Texas": "KOEAS", "Gulf Coast": "KOGCT",
                             "Midcon": "KOAND", "Permian Basin": "KOPRM", "Colorado": "WELOP", "Scurry": "KOPRM"}, inplace=True)

totalAssetProduction.to_json(
    r".\kingops\data\totalAssetsProduction.json", orient="records")


print("yay")
