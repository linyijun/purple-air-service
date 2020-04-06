import json
import requests

from data_models.location_model import PurpleAirLocation
from methods.db_operations import create_table, insert_locations


def get_purple_air_locations(purple_air_location_url):

    purple_air_location_json = requests.get(purple_air_location_url)
    data = json.loads(purple_air_location_json.text)
    purple_air_locations = []

    for item in data['results']:
        if item.get('Lon') is not None and item.get('Lat') is not None:
            item_parser = {
                'sensor_id': item.get('ID'),
                'parent_id': item['ParentID'] if item.get('ParentID') is not None else item['ID'],
                'label': item.get('Label'),
                'channel': 'b' if item.get('ParentID') is not None else 'a',
                'device_location_type': item.get('DEVICE_LOCATIONTYPE'),
                'thingspeak_primary_id': item.get('THINGSPEAK_PRIMARY_ID'),
                'thingspeak_primary_id_read_key': item.get('THINGSPEAK_PRIMARY_ID_READ_KEY'),
                'thingspeak_second_id': item.get('THINGSPEAK_SECONDARY_ID'),
                'thingspeak_second_id_read_key': item.get('THINGSPEAK_SECONDARY_ID_READ_KEY'),
                'lon': item.get('Lon'),
                'lat': item.get('Lat')
            }
            purple_air_locations.append(item_parser)
    return purple_air_locations


def main():

    purple_air_location_url = 'https://www.purpleair.com/json'
    purple_air_locations = get_purple_air_locations(purple_air_location_url)

    # create_table(PurpleAirLocation)
    insert_locations(purple_air_locations, PurpleAirLocation)


if __name__ == '__main__':
    main()
