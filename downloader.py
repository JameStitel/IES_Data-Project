import requests
import pandas as pd
import time
import json


class GolemioApi:
    def __init__(self, api_key_path: str):
        self.api_key = self._load_api_key(api_key_path)
        self.headers = {'X-Access-Token': self.api_key}
        self.limit_per_page = 1000
        self.base_uri = 'https://api.golemio.cz/v1/'

    @staticmethod
    def _load_api_key(api_key_path: str) -> str:
        with open(api_key_path) as f:
            api_key = json.load(f)['X-Access-Token']
        return api_key

    def _download_page(self, endpoint: str, offset: int, debug: bool = False, **kwargs):
        parameters = ''
        for arg, value in kwargs.items():
            parameters += f'&{arg}={value}'
        uri = f'{self.base_uri}{endpoint}?limit={self.limit_per_page}&offset={offset}{parameters}'
        response = requests.get(uri, headers=self.headers)
        if debug:
            print(f'code: {response.status_code}, text: {response.text}')
        if str(response.status_code)[0] != '2':
            raise ConnectionError(f'Request failed with status code: {response.status_code}')
        return response.json()

    def _download_all_pages(self, endpoint: str, features: bool):
        n = 0
        while True:
            json_response = self._download_page(endpoint, offset=n)
            json_response = json_response['features'] if features else json_response
            n += len(json_response)
            if len(json_response) == 0:
                break
            yield json_response

    def download_all_stations(self):
        endpoint = 'gtfs/stops'
        json_responses = self._download_all_pages(endpoint, features=True)
        all_stations = []
        for response in json_responses:
            all_stations.extend(response)
        with open('data/all_stations.json', 'w') as f:
            json.dump(all_stations, f, ensure_ascii=True, indent=4)





"""
def main():
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
"""

if __name__ == '__main__':
    my_api_key_path = 'golemio_api_key.json'
    golemio = GolemioApi(my_api_key_path)
    golemio.download_all_stations()

