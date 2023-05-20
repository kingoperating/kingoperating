import requests
import json

url = "https://api.joyn.ai/common/user/token"

payload = {
    "uname": "mtanner@kingoperating.com",
    "pwd": "Bigshow1637%"
}
headers = {
    "Content-Type": "application/json"
}

payloadJson = json.dumps(payload)
response = requests.request("POST", url, data=payloadJson, headers=headers)
print(response.status_code)

results = response.json()

idToken = results["IdToken"]

url = "https://api-fdg.joyn.ai/admin/api/ReadingData?isCustom=true&entityids=15408&fromdate=2023-05-17&todate=2023-05-19&pagesize=1000&pagenumber=1"

response = requests.request("GET", url, headers={"Authorization": idToken})

print(response.status_code)

resultsReadingType = response.json()

print("yay")
