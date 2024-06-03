import requests
import json

url = " https://api.covidtracking.com/v1/us/daily.json"
response = requests.get(url)
data = response.json()

with open ('daily.json', 'w') as file:
    json.dump(data, file)
