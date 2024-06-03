import requests
import json

url = "https://api.covidtracking.com/v1/us/20200501.json"

response = requests.get(url)

if response.status_code == 200:
    data = response.json()
    with open('data_covid.json', 'w') as file:
        json.dump(data, file)
    print("Datos guardados exitosamente.")
else:
    print("Error en la solicitud: ", response.status_code)
