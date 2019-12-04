import requests
import pandas as pd
import time
import json

with open('golemio_api_key.json') as f:
    GOLEMIO_API_KEY = json.load(f)['X-Access-Token']


def main():
    headers = {
        'X-Access-Token': GOLEMIO_API_KEY,
    }

    n = 0
    while True:
        # print(n)
        # uri = f'https://api.golemio.cz/v1/gtfs/stops?limit=100&offset={n}'  # stations
        # uri = f'https://api.golemio.cz/v1/gtfs/trips?limit=1&offset={n}&stopId=U363Z1P'  # trips through Malvazinky
        uri = f'https://api.golemio.cz/v1/gtfs/stoptimes/U363Z1P?date=2019-12-04&limit=1&offset={n}&includeStop=true'
        # uri = f'https://api.golemio.cz/v1/vehiclepositions?limit=1&offset={n}&includePositions=true'  # vehicle positions
        json_response = requests.get(uri, headers=headers).json()  # stations

        n += len(json_response)
        if len(json_response) == 0:
            break
        # n += len(json_response['features'])
        # ids = [x for x in json_response['features'] if x['properties']['stop_name'] == 'Malvazinky']
        # if ids:
        #     print(ids)
        print(json_response)
        # break

    # U363Z1P - Malvazinky


if __name__ == '__main__':
    main()

