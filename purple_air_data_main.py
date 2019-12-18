import pandas as pd
import pytz

from data_models.common_db import session
from data_models.location_model import LosAngelesPurpleAirOutsideLocationView
from data_models.purple_air_model import *
from methods.db_operations import create_table, insert_data
from methods.request_data import *


def get_locations(location_table_obj):

    locations = session.query(location_table_obj.parent_id,
                              location_table_obj.id,
                              location_table_obj.channel,
                              location_table_obj.thingspeak_primary_id,
                              location_table_obj.thingspeak_primary_id_read_key,
                              location_table_obj.thingspeak_second_id,
                              location_table_obj.thingspeak_second_id_read_key).all()

    return locations


def main(config):

    # From time to time request
    start_time = config['START_TIME']
    end_time = config['END_TIME']
    tz = pytz.timezone('America/Los_Angeles')
    time_list = pd.date_range(start=start_time, end=end_time, freq='10min')
    time_list = [tz.localize(x) for x in time_list]
    time_list = [x.astimezone(pytz.utc) for x in time_list]
    datetime_list = [t.to_pydatetime() for t in time_list]

    locations = get_locations(LosAngelesPurpleAirOutsideLocationView)
    create_table(config['DATA_OBJ'])

    period = 6 * 24 * 4
    for loc in locations:
        print('Start the request {}.'.format(loc))
        purple_air_data = multiple_instance_request(loc, datetime_list, period)
        print('Total number of records: {}'.format(len(purple_air_data)))
        insert_data(purple_air_data, config['DATA_OBJ'])


if __name__ == '__main__':

    query = {
        1: ['2018-01-01 00:00:00', '2018-02-01 00:00:00', LosAngelesPurpleAir201801],
        2: ['2018-02-01 00:00:00', '2018-03-01 00:00:00', LosAngelesPurpleAir201802],
        3: ['2018-03-01 00:00:00', '2018-04-01 00:00:00', LosAngelesPurpleAir201803],
        4: ['2018-04-01 00:00:00', '2018-05-01 00:00:00', LosAngelesPurpleAir201804],
        5: ['2018-05-01 00:00:00', '2018-06-01 00:00:00', LosAngelesPurpleAir201805],
        6: ['2018-06-01 00:00:00', '2018-07-01 00:00:00', LosAngelesPurpleAir201806],
        7: ['2018-07-01 00:00:00', '2018-08-01 00:00:00', LosAngelesPurpleAir201807],
        8: ['2018-08-01 00:00:00', '2018-09-01 00:00:00', LosAngelesPurpleAir201808],
        9: ['2018-09-01 00:00:00', '2018-10-01 00:00:00', LosAngelesPurpleAir201809],
        11: ['2018-10-01 00:00:00', '2018-11-01 01:00:00', LosAngelesPurpleAir201810],
        10: ['2018-11-01 00:00:00', '2018-12-01 00:00:00', LosAngelesPurpleAir201811],
        12: ['2018-12-01 00:00:00', '2019-01-01 00:00:00', LosAngelesPurpleAir201812]
    }

    for i in [12]:
        conf = {
            'START_TIME': query[i][0],
            'END_TIME': query[i][1],
            'DATA_OBJ': query[i][2]
        }
        main(conf)