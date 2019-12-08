import os
from typing import Generator, Tuple

import grequests
import requests
import time
import json
from itertools import islice


class GolemioApi:
    def __init__(self, api_key_path: str):
        self.api_key = self._load_api_key(api_key_path)
        self.headers = {'X-Access-Token': self.api_key}
        self.limit_per_page = 1000
        self.base_uri = 'https://api.golemio.cz/v1/'
        self.all_stations_path = 'data/all_stations.json'
        self.all_stations_ids_path = 'data/all_stations_ids.json'
        self.all_stop_count_path = 'data/all_stop_count'  # need to append '_date.json'
        self.parent_ids_with_count_path = 'data/final-stations_with_count.json'
        self.__counted_stops = {}

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

    def _build_list_of_uris_for_count_stop(self, all_ids: dict, date: str) -> list:
        uris = []
        offset = 0

        kwargs = {
            'date': date
        }
        parameters = ''
        for arg, value in kwargs.items():
            parameters += f'{arg}={value}&'

        for station, properties in all_ids.items():
            station_id = station
            endpoint = f'gtfs/stoptimes/{station_id}'
            uri = f'{self.base_uri}{endpoint}?{parameters}limit={self.limit_per_page}&offset={offset}'
            uris.append(uri)
            for child_station in properties['children']:
                station_id = child_station
                endpoint = f'gtfs/stoptimes/{station_id}'
                uri = f'{self.base_uri}{endpoint}?{parameters}limit={self.limit_per_page}&offset={offset}'
                uris.append(uri)
        return uris

    def _build_list_of_uris_for_count_stop_cont(self, date: str) -> list:
        uris = []
        kwargs = {
            'date': date
        }
        parameters = ''
        for arg, value in kwargs.items():
            parameters += f'{arg}={value}&'

        for station in self.__remaining_async:
            station_id = station['stop_id']
            offset = station['offset']
            endpoint = f'gtfs/stoptimes/{station_id}'
            uri = f'{self.base_uri}{endpoint}?{parameters}limit={self.limit_per_page}&offset={offset}'
            uris.append(uri)
        return uris

    @staticmethod
    def __exception(request, exception):
        print("Problem: {}: {}".format(request.url, exception))

    def __callback(self, res, **kwargs):
        responses = res.json()
        n = len(responses)
        if n and self.__counted_stops.get(responses[0]['stop_id']):
            self.__remaining_async.append(
                {
                    'stop_id': [responses[0]['stop_id']],
                    'offset': n + self.__counted_stops[responses[0]['stop_id']],
                }
            )
            self.__counted_stops[responses[0]['stop_id']] += n
        elif n:
            self.__counted_stops[responses[0]['stop_id']] = n
            self.__remaining_async.append(
                {
                    'stop_id': [responses[0]['stop_id']],
                    'offset': n,
                }
            )

    def __async_requests(self, urls):
        self.__remaining_async = []
        req = (grequests.get(u, headers=self.headers, hooks=dict(response=self.__callback)) for u in urls)
        grequests.map(req, exception_handler=self.__exception, size=5)

    @staticmethod
    def _split_dict_into_n_sized_chunks(d: dict, n: int) -> Generator:
        it = iter(d)
        for i in range(0, len(d), n):
            yield {k: d[k] for k in islice(it, n)}

    def count_stop_times_per_day(self, date: str):
        with open(self.all_stations_ids_path) as input_f:
            all_ids = json.load(input_f)
        n = 1
        for chunk in self._split_dict_into_n_sized_chunks(all_ids, 4000):
            urls = self._build_list_of_uris_for_count_stop(chunk, date)
            self.__async_requests(urls)
            while self.__remaining_async:
                urls = self._build_list_of_uris_for_count_stop_cont(date)
                self.__async_requests(urls)
            self._save_into_json(self.__counted_stops, f'{self.all_stop_count_path}_{date}_{n}.json')

            n += 1
            self.__counted_stops = {}
            print('Sleeping...')
            time.sleep(30)
            print('Sleeping done...')

    @staticmethod
    def _copy_dict_without_keys(d: dict, invalid_keys: list) -> dict:
        return {x: d[x] for x in d if x not in invalid_keys}

    @staticmethod
    def _get_child_parent_dict(all_ids: dict) -> dict:
        child_parent = {}
        for parent_station, properties in all_ids.items():
            for child_station in properties['children']:
                child_parent[child_station] = parent_station
        return child_parent

    def _aggregate_stop_count_per_file(self, stops: dict, all_ids: dict, all_stops: dict) -> dict:
        child_parent = self._get_child_parent_dict(all_ids)
        for station, count in stops.items():
            station = child_parent[station] if station not in all_ids else station
            if station in all_stops:
                all_stops[station] += count
            else:
                all_stops[station] = count
        return all_stops

    def aggregate_stop_count(self, date: str) -> Tuple[dict, dict]:
        with open(self.all_stations_ids_path) as input_f:
            all_ids = json.load(input_f)
        all_stops = {}
        for file in os.scandir('data'):
            if f'{self.all_stop_count_path.split("/")[1]}_{date}' in file.name:
                with open(file.path) as stops_f:
                    stops = json.load(stops_f)
                    all_stops = self._aggregate_stop_count_per_file(stops, all_ids, all_stops)
        return all_stops, all_ids

    def _assign_stop_count_per_file(self, all_ids: dict, all_stops: dict, parent_stations_with_count: dict):
        for station, properties in all_ids.items():
            __temp_properties = self._copy_dict_without_keys(all_ids[station], ['children'])
            __temp_properties['count'] = all_stops.get(station, 0)
            parent_stations_with_count[station] = __temp_properties
        return parent_stations_with_count

    def assign_stop_count(self, date: str):
        parent_ids_with_count = {}
        all_stops, all_ids = self.aggregate_stop_count(date)
        parent_ids_with_count = self._assign_stop_count_per_file(all_ids, all_stops, parent_ids_with_count)
        self._save_into_json(parent_ids_with_count, self.parent_ids_with_count_path)
        # TODO assign date somewhere to the file


if __name__ == '__main__':
    my_api_key_path = 'golemio_api_key.json'
    my_date = '2019-12-07'
    golemio = GolemioApi(my_api_key_path)
    # golemio.download_all_stations()
    # golemio.filter_station_ids_enriched()
    # golemio.count_stop_times_per_day(my_date)
    golemio.assign_stop_count(my_date)

