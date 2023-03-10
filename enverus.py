from enverus_developer_api import DeveloperAPIv3
from dotenv import load_dotenv
import requests
import os
import pandas as pd

load_dotenv()

d3 = DeveloperAPIv3(secret_key=os.getenv('ENVERUS_API'))

secert_key = os.getenv('ENVERUS_API')

test = requests.post(
    'https://api.enverus.com/v3/direct-access/tokens')

test.json()

r = requests.get('https://api.enverus.com/v3/direct-access/production',
                 headers={'Authorization': test})

table = pd.DataFrame(
    d3.query("wells", envoperator="KING OPERATING CORPORATION"))

lengthOfTable = len(table)

for row in d3.query("wells", envoperator="KING OPERATING CORPORATION"):
    print(row)

print("yay")
