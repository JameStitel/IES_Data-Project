import pandas as pd
import json
import plotly.express as px


class Visualizer:
    @staticmethod
    def load_data() -> dict:
        with open('data/final-stations_with_count.json') as data:
            json_data = json.load(data)
        return json_data

    @staticmethod
    def reformat_data(json_data: dict, date: str) -> list:
        new_json_data = [{
            'id': station_id,
            'name': data['name'],
            'latitude': data['location']['lat'],
            'longitude': data['location']['lon'],
            'stop_count': data['count'][date],
        } for station_id, data in json_data.items() if data['location']]
        return new_json_data

    def get_possible_dates(self) -> list:
        return list(next(iter(self.load_data().values()))['count'].keys())

    def plot(self, date):
        json_data = self.reformat_data(self.load_data(), date)
        df = pd.DataFrame(json_data)
        max_stop_count = df['stop_count'].max()

        fig = px.density_mapbox(df, lat='latitude', lon='longitude', z='stop_count', hover_name='name', radius=15,
                                color_continuous_scale='inferno', color_continuous_midpoint=max_stop_count/2.4,
                                center=dict(lat=49.80, lon=15.20), zoom=7, mapbox_style="open-street-map")
        fig.show()


if __name__ == '__main__':
    visualizer = Visualizer()
    # print(visualizer.get_possible_dates())
    visualizer.plot('2020-01-02')
    # visualizer.plot('2019-12-07')
