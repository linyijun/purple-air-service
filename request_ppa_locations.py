import json
import requests
import pandas as pd

API_KEY = ''


def get_purple_air_locations():

    fields = ['latitude', 'longitude', 'altitude', 'primary_id_a', 'primary_key_a', 'primary_id_b', 'primary_key_b']
    fields = ','.join(fields)
    purple_air_location_url = f"https://api.purpleair.com/v1/sensors?api_key={API_KEY}" \
                              f"&fields={fields}&location_type=0&private=0"
    purple_air_location_json = requests.get(purple_air_location_url)
    data = json.loads(purple_air_location_json.text)
    purple_air_locations = pd.DataFrame(data['data'], columns=data['fields'])
    return purple_air_locations


def get_purple_air_locations_ca(locations):

    locations = locations[locations['longitude'] >= -124.409591]
    locations = locations[locations['longitude'] <= -114.131211]
    locations = locations[locations['latitude'] >= 32.534156]
    locations = locations[locations['latitude'] <= 42.009518]
    return locations


def get_purple_air_locations_la_county(locations):

    locations = locations[locations['longitude'] >= -118.666]
    locations = locations[locations['longitude'] <= -117.800]
    locations = locations[locations['latitude'] >= 33.800]
    locations = locations[locations['latitude'] <= 34.800]
    return locations


if __name__ == '__main__':

    sensor_locations = get_purple_air_locations()
    sensor_locations.to_csv('./data/sensor_locations.csv', index=False)
    # ca_sensor_locations = get_purple_air_locations_ca(sensor_locations)
    # ca_sensor_locations.to_csv('./data_backup/ca_sensor_locations.csv', index=False)
    la_sensor_locations = get_purple_air_locations_la_county(sensor_locations)
    la_sensor_locations.to_csv('./data/la_county_sensor_locations.csv', index=False)
