import json
import os
import time
from itertools import islice
from typing import Generator, Tuple

import grequests
import requests


class GolemioApiDownloader:
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

    def _download_page(self, endpoint: str, offset: int, debug: bool = False, **kwargs) -> dict:
        """
        Download one page of data for the selected endpoint of Golemio API with the selected parameters as arguments.
        Page size set by attribute `limit_per_page`, by default to 1000.
        :param endpoint: the selected endpoint to download data from
        :param offset: skip this number of first items (sth. like paging)
        :param debug: set to True if you need to see responses in the console
        :param kwargs: selected parameters for querying the endpoint
        :return: dict with data from response
        """
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
        """
        Download all pages of data for the selected endpoint of Golemio API with the selected parameters as arguments.
        Page size set by attribute `limit_per_page`, by default to 1000.
        :param endpoint: the selected endpoint to download data from
        :param features: bool whether the data coming in response from Golemio API is under features key
        :param debug: set to True if you need to see responses in the console
        :param kwargs: selected parameters for querying the endpoint
        :return: Generator yielding the data for individual pages
        """
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
        """
        Step 1 of the GolemioApiDonwloader. Download all available public transport stops from the Golemio API,
        and save with all information into json file named `all_stations`.
        """
        endpoint = 'gtfs/stops'
        json_responses = self._download_all_pages(endpoint, features=True)
        all_stations = []
        for response in json_responses:
            all_stations.extend(response)
        self._save_into_json(all_stations, self.all_stations_path)

    @staticmethod
    def _save_parents_into_ids_dict(all_ids: dict, children: dict, parent_station_id: str, station_id: str,
                                    station_location: dict, station_name: str) -> Tuple[dict, dict]:
        """
        Save only the parent stations into a dict containing just the required information.
        :param all_ids: dict containing the already saved parent stations
        :param children: dict containing the already saved child stations
        :param parent_station_id: id of the parent station if this is a child station
        :param station_id: id of this station
        :param station_location: location of this station
        :param station_name: name of this station
        :return: dict containing the parent stations as all_ids and dict containing the child stations
        """
        if parent_station_id:  # ~ if is a child station
            if parent_station_id in children:  # ~ if this child station`s parent is already saved in children
                if station_id not in children[parent_station_id]['children']:  # prevent duplication of children
                    children[parent_station_id]['children'].append(station_id)
            else:  # ~ if no parent for this child station was found, then save the parent
                children[parent_station_id] = {
                    'children': [station_id],
                }
        else:  # ~ if this is the parent station itself
            all_ids[station_id] = {
                'name': station_name,
                'location': station_location,
                'children': [],
            }
        return all_ids, children

    @staticmethod
    def _save_children_into_ids_dict(all_ids: dict, children: dict) -> dict:
        """
        Extend the dict of parent stations (all_ids) by all their children and children of children.
        :param all_ids: the parent stations dict
        :param children: the child stations dict
        :return: parent station dict complemented by all the child stations
        """
        children_of_children = {}
        child_parent = {}
        for parent_station_id, children in children.items():
            if parent_station_id in all_ids:
                all_ids[parent_station_id]['children'] = children['children']
                this_children = {child: parent_station_id for child in children['children']}
                child_parent = {**child_parent, **this_children}
            else:
                children_of_children[parent_station_id] = children['children']
        for parent_station_id, children in children_of_children.items():
            in_first = set(all_ids[child_parent[parent_station_id]]['children'])
            in_second = set(children)
            in_second_but_not_in_first = in_second - in_first
            all_ids[child_parent[parent_station_id]]['children'].extend(in_second_but_not_in_first)
        return all_ids

    def filter_station_ids_enriched(self):
        """
        Step 2 of the GolemioApiDonwloader. Transform information about all stations into a json named
        `all_stations_ids` containing just the required information for parent stations with list of child stations.
        """
        with open(self.all_stations_path) as input_f:
            all_stations = json.load(input_f)
        all_ids = {}
        children = {}
        for station in all_stations:
            parent_station_id = station['properties']['parent_station']
            station_id = station['properties']['stop_id']
            station_location = {
                'lat': station['properties']['stop_lat'],
                'lon': station['properties']['stop_lon'],
            }
            station_name = station['properties']['stop_name']
            all_ids, children = self._save_parents_into_ids_dict(all_ids, children, parent_station_id, station_id,
                                                                 station_location, station_name)
        all_ids = self._save_children_into_ids_dict(all_ids, children)
        self._save_into_json(all_ids, self.all_stations_ids_path)

    def _build_list_of_urls_for_count_stop(self, all_ids: dict, date: str) -> list:
        """
        Build list of urls for async requests to retrieve number of stops by all public transportation for all the
        stations and the selected date from Golemio API.
        :param all_ids: dict of all the stations in parent-children format
        :param date: the selected date
        :return: list of urls
        """
        urls = []
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
            urls.append(uri)
            for child_station in properties['children']:
                station_id = child_station
                endpoint = f'gtfs/stoptimes/{station_id}'
                uri = f'{self.base_uri}{endpoint}?{parameters}limit={self.limit_per_page}&offset={offset}'
                urls.append(uri)
        return urls

    def _build_list_of_urls_for_count_stop_cont(self, date: str) -> list:
        """
        Build list of urls for async requests to retrieve number of stops by all public transportation for all the
        stations and the selected date from Golemio API.
        THIS BUILDS URIS LIST FROM THE REMAINING requests after the last async requesting of the data!
        :param date: the selected date
        :return: list of urls
        """
        urls = []
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
            urls.append(uri)
        return urls

    @staticmethod
    def __exception(request: requests.models.Response, exception):
        """
        Exception handling of each individual request.
        :param request: the request
        :param exception: the exception of the request
        """
        print("Problem: {}: {}".format(request.url, exception))

    def __callback(self, res: requests.models.Response, **kwargs):
        """
        Callback for each of the individual requests. Add the number of stops for the individual station into
        `self.__counted_stops` dict and append data for requesting the next page of stops into the `__remaining_async`.
        :param res: the response
        :param kwargs: the kwargs
        """
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

    def __async_requests(self, urls: list):
        """
        Async request the urls, handling responses with `__callback` and `__exception`.
        :param urls: list of the urls to request
        """
        self.__remaining_async = []
        req = (grequests.get(u, headers=self.headers, hooks=dict(response=self.__callback)) for u in urls)
        grequests.map(req, exception_handler=self.__exception, size=5)

    @staticmethod
    def _split_dict_into_n_sized_chunks(d: dict, n: int) -> Generator:
        it = iter(d)
        for i in range(0, len(d), n):
            yield {k: d[k] for k in islice(it, n)}

    def count_stop_times_per_day(self, date: str):
        """
        Step 3 of the GolemioApiDonwloader. Download all stop counts for all the stations from `all_stations_ids.json`
        and save into json files by the selected date, `all_stop_count_{date}_{page_number}.
        :param date: the selected date
        """
        with open(self.all_stations_ids_path) as input_f:
            all_ids = json.load(input_f)
        n = 1
        for chunk in self._split_dict_into_n_sized_chunks(all_ids, 4000):
            urls = self._build_list_of_urls_for_count_stop(chunk, date)
            self.__async_requests(urls)
            while self.__remaining_async:
                urls = self._build_list_of_urls_for_count_stop_cont(date)
                self.__async_requests(urls)
            self._save_into_json(self.__counted_stops, f'{self.all_stop_count_path}_{date}_{n}.json')

            n += 1
            self.__counted_stops = {}
            print('Sleeping...')
            time.sleep(30)  # prevent possible timeout
            print('Sleeping done...')

    @staticmethod
    def _copy_dict_without_keys(d: dict, invalid_keys: list) -> dict:
        return {x: d[x] for x in d if x not in invalid_keys}

    @staticmethod
    def _get_child_parent_dict(all_ids: dict) -> dict:
        """
        Get dict of child-parent relations in the format: {child1: parent1, child2: parent1, child3: parent2, ...}.
        :param all_ids: all parent stations with their children
        :return: dict in the mentioned format
        """
        child_parent = {}
        for parent_station, properties in all_ids.items():
            for child_station in properties['children']:
                child_parent[child_station] = parent_station
        return child_parent

    def _aggregate_stop_count_per_file(self, stops: dict, all_ids: dict, all_stops: dict) -> dict:
        """
        Aggregate the stop count for the selected file.
        :param stops: stop count from the selected file
        :param all_ids: all parent stations with their children as in the `all_stations_ids.json`
        :param all_stops: CURRENT all parent stations dict with the corresponding stop count
        :return: UPDATED all parent stations dict with the corresponding stop count
        """
        child_parent = self._get_child_parent_dict(all_ids)
        for station, count in stops.items():
            station = child_parent[station] if station not in all_ids else station
            if station in all_stops:
                all_stops[station] += count
            else:
                all_stops[station] = count
        return all_stops

    def aggregate_stop_count(self, date: str) -> Tuple[dict, dict]:
        """
        Aggregate all stop count (including that of child stations) for all the parent stations for the selected date.
        :param date: the selected date
        :return: dict of all the parent stations and the corresponding aggregated stop count; and dict of all parent
        stations with their children as in the `all_stations_ids.json`
        """
        with open(self.all_stations_ids_path) as input_f:
            all_ids = json.load(input_f)
        all_stops = {}
        for file in os.scandir('data'):
            if f'{self.all_stop_count_path.split("/")[1]}_{date}' in file.name:
                with open(file.path) as stops_f:
                    stops = json.load(stops_f)
                    all_stops = self._aggregate_stop_count_per_file(stops, all_ids, all_stops)
        return all_stops, all_ids

    def _assign_stop_count(self, all_ids: dict, all_stops: dict, parent_stations_with_count: dict, date: str,
                           initial: bool) -> dict:
        """
        Create dict of parent stations with the necessary information if initial, otherwise take the already loaded
        data and enriched by aggregated stop count for the selected date
        :param all_ids: all parent stations with their children as in the `all_stations_ids.json`
        :param all_stops: all parent stations dict with the corresponding stop count
        :param parent_stations_with_count: the dict containing already assigned stop counts or empty dict
        :param date: the selected date
        :param initial: bool whether there are already any data for stop counts
        :return: dict of updated (or newly created if `initial=False`) assigned stop counts
        """
        for station, properties in all_ids.items():
            if initial:
                __temp_properties = self._copy_dict_without_keys(all_ids[station], ['children'])
                __temp_properties['count'] = {date: all_stops.get(station, 0)}
                parent_stations_with_count[station] = __temp_properties
            else:
                parent_stations_with_count[station]['count'][date] = all_stops.get(station, 0)
        return parent_stations_with_count

    def assign_stop_count(self, date: str, initial: bool):
        """
        Step 3 of the GolemioApiDonwloader. Assign the aggregated already downloaded all stop counts for the selected
        date to all the parent stations from `all_stations_ids.json`,
         and save into json file named `final-stations_with_count`.
        :param date: the selected date
        :param initial: bool whether there are already any data for stop counts or this is the initial assignment
        """
        if initial:
            parent_ids_count = {}
        else:
            with open(self.parent_ids_with_count_path) as f:
                parent_ids_count = json.load(f)
        all_stops, all_ids = self.aggregate_stop_count(date)
        parent_ids_count = self._assign_stop_count(all_ids, all_stops, parent_ids_count, date, initial)
        self._save_into_json(parent_ids_count, self.parent_ids_with_count_path)


if __name__ == '__main__':
    my_api_key_path = 'golemio_api_key.json'  # path to your Golemio API key
    my_date = '2020-01-02'
    golemio = GolemioApiDownloader(my_api_key_path)
    # golemio.download_all_stations()
    # golemio.filter_station_ids_enriched()
    # golemio.count_stop_times_per_day(my_date)
    # golemio.assign_stop_count(my_date, initial=False)
