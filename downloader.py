from typing import Generator

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
        self.all_stations_path = 'data/all_stations.json'
        self.all_stations_ids_path = 'data/all_stations_ids.json'

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

    def _download_all_pages(self, endpoint: str, features: bool) -> Generator:
        n = 0
        while True:
            json_response = self._download_page(endpoint, offset=n)
            json_response = json_response['features'] if features else json_response
            n += len(json_response)
            if len(json_response) == 0:
                break
            yield json_response

    @staticmethod
    def _save_into_json(data: list or dict, file_path: str):
        with open(file_path, 'w') as output_f:
            json.dump(data, output_f, ensure_ascii=False, indent=4)

    def download_all_stations(self):
        endpoint = 'gtfs/stops'
        json_responses = self._download_all_pages(endpoint, features=True)
        all_stations = []
        for response in json_responses:
            all_stations.extend(response)
        self._save_into_json(all_stations, self.all_stations_path)

    @staticmethod
    def _save_into_ids_dict(all_ids: dict, parent_station_id: str, station_id: str, station_location: dict,
                            station_name: str) -> dict:
        if parent_station_id:  # ~ if is a child station
            if parent_station_id in all_ids:  # ~ if this child station`s parent is already saved
                if station_id not in all_ids[parent_station_id]['children']:  # prevent duplication of children
                    all_ids[parent_station_id]['children'].append(station_id)
            else:  # ~ if no parent for this child station was found, then save the parent reference
                all_ids[parent_station_id] = {
                    'name': None,
                    'location': None,
                    'children': [station_id],
                }
        else:  # ~ if this is the parent station itself
            if station_id in all_ids:  # ~ if there was empty reference to parent created from children station
                all_ids[station_id]['name'] = station_name
                all_ids[station_id]['location'] = station_location
            else:
                all_ids[station_id] = {
                    'name': station_name,
                    'location': station_location,
                    'children': [],
                }
        return all_ids

    def filter_station_ids_enriched(self):  # todo maybe aggregate not only based on parent stations, but &&same names??
        with open(self.all_stations_path) as input_f:
            all_stations = json.load(input_f)
        all_ids = {}
        for station in all_stations:
            parent_station_id = station['properties']['parent_station']
            station_id = station['properties']['stop_id']
            station_location = {
                'lat': station['properties']['stop_lat'],
                'lon': station['properties']['stop_lon'],
            }
            station_name = station['properties']['stop_name']
            all_ids = self._save_into_ids_dict(all_ids, parent_station_id, station_id, station_location, station_name)
        self._save_into_json(all_ids, self.all_stations_ids_path)

    # def count_stop_times_per_day(self):
    #     with open(self.all_stations_ids_path) as input_f:
    #         all_ids = json.load(input_f)





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
    # golemio.download_all_stations()
    golemio.filter_station_ids_enriched()
