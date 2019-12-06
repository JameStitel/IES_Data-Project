from typing import Generator

import grequests
import requests
import pandas as pd
import time
import json

# class AsyncRequests:
#     def __init__(self, urls: list):
#         self.urls = urls
#
#     def exception(self, request, exception):
#         print("Problem: {}: {}".format(request.url, exception))
#
#     def async(self):
#         results = grequests.map((grequests.get(u) for u in self.urls), exception_handler=self.exception, size=5)
#         print(results)

class GolemioApi:
    def __init__(self, api_key_path: str):
        self.api_key = self._load_api_key(api_key_path)
        self.headers = {'X-Access-Token': self.api_key}
        self.limit_per_page = 1000
        self.base_uri = 'https://api.golemio.cz/v1/'
        self.all_stations_path = 'data/all_stations.json'
        self.all_stations_ids_path = 'data/all_stations_ids.json'
        self.all_stop_count_path = 'data/all_stop_count'  # need to append '_date.json'

    @staticmethod
    def _load_api_key(api_key_path: str) -> str:
        with open(api_key_path) as f:
            api_key = json.load(f)['X-Access-Token']
        return api_key

    def _download_page(self, endpoint: str, offset: int, debug: bool = False, **kwargs):
        parameters = ''
        for arg, value in kwargs.items():
            parameters += f'{arg}={value}&'
        uri = f'{self.base_uri}{endpoint}?{parameters}limit={self.limit_per_page}&offset={offset}'
        response = requests.get(uri, headers=self.headers)
        if debug:
            print(f'code: {response.status_code}, text: {response.text}')
        if str(response.status_code)[0] != '2':
            raise ConnectionError(f'Request failed with status code: {response.status_code}')
        return response.json()

    def _download_all_pages(self, endpoint: str, features: bool, debug: bool = False, **kwargs) -> Generator:
        n = 0
        while True:
            json_response = self._download_page(endpoint, offset=n, debug=debug, **kwargs)
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

    def _get_stop_count_for_station_per_day(self, station_id: str, date: str) -> int:
        endpoint = f'gtfs/stoptimes/{station_id}'
        kwargs = {
            'date': date
        }
        json_responses = self._download_all_pages(endpoint, features=False, debug=False, **kwargs)
        stop_count = 0
        for response in json_responses:
            stop_count += len(response)
        return stop_count

    def count_stop_times_per_day(self, date: str):
        with open(self.all_stations_ids_path) as input_f:
            all_ids = json.load(input_f)
        n_done = 0
        n_all = len(all_ids)
        for station, properties in all_ids.items():  # TODO wayyyyyyy toooo slow, batch requests?
            stop_count = self._get_stop_count_for_station_per_day(station, date)
            for child_station in properties['children']:
                stop_count += self._get_stop_count_for_station_per_day(child_station, date)
            properties['stop_count'] = stop_count
            n_done += 1
            if n_done % 100 == 0:
                print(f'Stops counted for {n_done} stations out of {n_all}')
        self._save_into_json(all_ids, f'{self.all_stop_count_path}_{date}.json')


if __name__ == '__main__':
    my_api_key_path = 'golemio_api_key.json'
    golemio = GolemioApi(my_api_key_path)
    # golemio.download_all_stations()
    # golemio.filter_station_ids_enriched()
    golemio.count_stop_times_per_day('2019-12-06')
