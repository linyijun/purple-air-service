import pandas as pd
import pytz

from data_models.common_db import session
from data_models.location_model import LosAngelesPurpleAirOutsideLocationView
from data_models.purple_air_model import *
from methods.db_operations import create_table, insert_ppa_data
from methods.request_data import multiple_instance_request


def get_locations(location_table_obj):
    locations = session.query(location_table_obj.sensor_id,
                              location_table_obj.parent_id,
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

    # get available purple air sensor locations
    locations = get_locations(LosAngelesPurpleAirOutsideLocationView)

    # create new table for the data object
    create_table(config['DATA_OBJ'])

    # request p/home/yijun/notebooks/training_dataeriod every 4 days (4 * 24 * 60 < max(8200))
    period = 6 * 24 * 4
    for loc in locations:
        print('Start the request {}.'.format(loc))
        purple_air_data = multiple_instance_request(loc, datetime_list, period)
        print('Total number of records: {}'.format(len(purple_air_data)))
        insert_ppa_data(purple_air_data, config['DATA_OBJ'])


if __name__ == '__main__':

    query = {
        # 1: ['2018-01-01 00:00:00', '2018-02-01 00:00:00', LosAngelesPurpleAir201801],
        # 2: ['2018-02-01 00:00:00', '2018-03-01 00:00:00', LosAngelesPurpleAir201802],
        # 3: ['2018-03-01 00:00:00', '2018-04-01 00:00:00', LosAngelesPurpleAir201803],
        # 4: ['2018-04-01 00:00:00', '2018-05-01 00:00:00', LosAngelesPurpleAir201804],
        # 5: ['2018-05-01 00:00:00', '2018-06-01 00:00:00', LosAngelesPurpleAir201805],
        # 6: ['2018-06-01 00:00:00', '2018-07-01 00:00:00', LosAngelesPurpleAir201806],
        # 7: ['2018-07-01 00:00:00', '2018-08-01 00:00:00', LosAngelesPurpleAir201807],
        # 8: ['2018-08-01 00:00:00', '2018-09-01 00:00:00', LosAngelesPurpleAir201808],
        # 9: ['2018-09-01 00:00:00', '2018-10-01 00:00:00', LosAngelesPurpleAir201809],
        # 10: ['2018-10-01 00:00:00', '2018-11-01 00:00:00', LosAngelesPurpleAir201810],
        # 11: ['2018-11-01 00:00:00', '2018-12-01 00:00:00', LosAngelesPurpleAir201811],
        # 12: ['2018-12-01 00:00:00', '2019-01-01 00:00:00', LosAngelesPurpleAir201812]
        # 1: ['2019-01-01 00:00:00', '2019-02-01 00:00:00', LosAngelesPurpleAir201901],
        # 2: ['2019-02-01 00:00:00', '2019-03-01 00:00:00', LosAngelesPurpleAir201902],
        # 3: ['2019-03-01 00:00:00', '2019-04-01 00:00:00', LosAngelesPurpleAir201903],
        # 4: ['2019-04-01 00:00:00', '2019-05-01 00:00:00', LosAngelesPurpleAir201904],
        # 5: ['2019-05-01 00:00:00', '2019-06-01 00:00:00', LosAngelesPurpleAir201905],
        # 6: ['2019-06-01 00:00:00', '2019-07-01 00:00:00', LosAngelesPurpleAir201906],
        # 7: ['2019-07-01 00:00:00', '2019-08-01 00:00:00', LosAngelesPurpleAir201907],
        # 8: ['2019-08-01 00:00:00', '2019-09-01 00:00:00', LosAngelesPurpleAir201908],
        # 9: ['2019-09-01 00:00:00', '2019-10-01 00:00:00', LosAngelesPurpleAir201909],
        10: ['2019-10-01 00:00:00', '2019-10-20 00:00:00', LosAngelesPurpleAir201910],
        # 11: ['2019-11-01 00:00:00', '2019-12-01 00:00:00', LosAngelesPurpleAir201911],
        # 12: ['2019-12-01 00:00:00', '2020-01-01 00:00:00', LosAngelesPurpleAir201912],
        # 1: ['2020-01-01 00:00:00', '2020-02-01 00:00:00', LosAngelesPurpleAir202001],
        # 2: ['2020-02-01 00:00:00', '2020-03-01 00:00:00', LosAngelesPurpleAir202002],
        # 3: ['2020-03-01 00:00:00', '2020-04-01 00:00:00', LosAngelesPurpleAir202003],
        # 4: ['2020-04-01 00:00:00', '2020-05-01 00:00:00', LosAngelesPurpleAir202004],
        # 5: ['2020-05-01 00:00:00', '2020-06-01 00:00:00', LosAngelesPurpleAir202005],
        # 6: ['2020-06-01 00:00:00', '2020-07-01 00:00:00', LosAngelesPurpleAir202006],
        # 7: ['2020-07-01 00:00:00', '2020-08-01 00:00:00', LosAngelesPurpleAir202007],
        # 8: ['2020-08-01 00:00:00', '2020-09-01 00:00:00', LosAngelesPurpleAir202008],
        # 9: ['2020-09-01 00:00:00', '2020-10-01 00:00:00', LosAngelesPurpleAir202009],
        # 10: ['2020-10-01 00:00:00', '2020-11-01 01:00:00', LosAngelesPurpleAir202010],
        # 11: ['2020-11-01 00:00:00', '2020-12-01 00:00:00', LosAngelesPurpleAir202011],
        # 12: ['2020-12-01 00:00:00', '2021-01-01 00:00:00', LosAngelesPurpleAir202012]
    }

    for i in range(1, 4):
        conf = {
            'START_TIME': query[i][0],
            'END_TIME': query[i][1],
            'DATA_OBJ': query[i][2]
        }
        main(conf)
