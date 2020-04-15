import datetime

import pandas as pd
import pytz

from data_models.common_db import session
from data_models.location_model import LosAngelesPurpleAirOutsideLocationView
from data_models.purple_air_model import *
from methods.db_operations import create_table, insert_ppa_data
from methods.request_data import multiple_instance_request
from utils import *


def get_locations(location_table_obj):
    locations = session.query(location_table_obj.sensor_id,
                              location_table_obj.parent_id,
                              location_table_obj.channel,
                              location_table_obj.thingspeak_primary_id,
                              location_table_obj.thingspeak_primary_id_read_key,
                              location_table_obj.thingspeak_second_id,
                              location_table_obj.thingspeak_second_id_read_key).all()
    return locations


def get_current_time():

    tz = pytz.timezone('America/Los_Angeles')
    current_time = tz.localize(datetime.datetime.now().replace(minute=0, second=0, microsecond=0, tzinfo=None))
    return current_time


def time_to_time_request(start_time, end_time, data_obj):

    # From time to time request
    tz = pytz.timezone('America/Los_Angeles')
    time_list = pd.date_range(start=start_time, end=end_time, freq='10min')
    time_list = [tz.localize(x) for x in time_list]
    time_list = [x.astimezone(pytz.utc) for x in time_list]
    time_list = [t.to_pydatetime() for t in time_list]

    # create new table for the data object
    create_table(data_obj)

    # get available purple air sensor locations
    locations = get_locations(LosAngelesPurpleAirOutsideLocationView)

    # request p/home/yijun/notebooks/training_dataeriod every 4 days (4 * 24 * 60 < max(8200))
    period = int(60 / 10 * 24 * 4)
    for loc in locations:
        print('Start the request {}.'.format(loc))
        purple_air_data = multiple_instance_request(loc, time_list, period)
        print('Total number of records: {}'.format(len(purple_air_data)))
        insert_ppa_data(purple_air_data, data_obj)


def one_time_request(start_time, end_time, data_obj):

    time_list = pd.date_range(start=start_time, end=end_time, closed='left', freq='10min')
    time_list = [x.astimezone(pytz.utc) for x in time_list]
    time_list = [t.to_pydatetime() for t in time_list]

    if start_time.day == 1 and start_time.hour == 0:
        create_table(data_obj)

    # get available purple air sensor locations
    locations = get_locations(LosAngelesPurpleAirOutsideLocationView)

    purple_air_data = []
    for loc in locations:
        print('Start the request {}.'.format(loc))
        purple_air_data += multiple_instance_request(loc, time_list, 4 * 24 * 60)
    print('Total number of records: {}'.format(len(purple_air_data)))
    insert_ppa_data(purple_air_data, data_obj)


if __name__ == '__main__':

    # year = 2018
    # query = PURPLE_AIR_SET_2018
    # for month in range(1, 12):
    #     month_str = str(month).rjust(2, '0')
    #     next_month_str = str(month % 12 + 1).rjust(2, '0')
    #     next_year = year if month != 12 else year + 1
    #     time_to_time_request(f'{year}-{month}-01', f'{next_year}-{next_month_str}-01', query[month])
    #
    # year = 2019
    # query = PURPLE_AIR_SET_2019
    # for month in range(1, 10):
    #     month_str = str(month).rjust(2, '0')
    #     next_month_str = str(month % 12 + 1).rjust(2, '0')
    #     next_year = year if month != 12 else year + 1
    #     time_to_time_request(f'{year}-{month}-01', f'{next_year}-{next_month_str}-01', query[month])

    # year = 2020
    # query = PURPLE_AIR_SET_2020
    # for month in range(4, 5):
    #     month_str = str(month).rjust(2, '0')
    #     next_month_str = str(month % 12 + 1).rjust(2, '0')
    #     next_year = year if month != 12 else year + 1
    #     time_to_time_request(f'{year}-{month}-01', f'{next_year}-{next_month_str}-01', query[month])
    #
    query = PURPLE_AIR_SET_2020
    end_time = get_current_time()
    start_time = end_time - datetime.timedelta(hours=1)
    one_time_request(start_time, end_time, query[start_time.month])


