import pytz
import pandas as pd
import numpy as np
from datetime import timedelta

from data_models.common_db import session
from methods.db_operations import create_table
from utils import *


def main(config):

    start_time = config['START_TIME']
    end_time = config['END_TIME']
    tz = pytz.timezone('America/Los_Angeles')
    time_list = pd.date_range(start=start_time, end=end_time, freq='H')
    time_list = [tz.localize(x) for x in time_list]
    table_obj = config['TABLE']
    new_table_obj = config['NEW_TABLE']

    fields = ['pm1_atm', 'pm2_5_atm', 'pm10_atm', 'pm1_cf_1', 'pm2_5_cf_1', 'pm10_cf_1', 'p_0_3um_cnt', 'p_0_5um_cnt',
               'p_1_0um_cnt', 'p_2_5um_cnt', 'p_5um_cnt', 'p_10um_cnt', 'rssi', 'temperature', 'humidity']

    for i, t in enumerate(time_list[:-1]):

        sql_statement = session.query(table_obj).filter(table_obj.timestamp >= time_list[i], table_obj.timestamp < time_list[i + 1])
        df = pd.read_sql(sql_statement.statement, session.bind)[['sensor_id', 'channel'] + fields]

        if len(df) == 0:
            continue

        def preprocessing(x):
            x_mean, x_std = x.mean(skipna=True), x.std(skipna=True)
            x_left, x_right = x_mean - x_std, x_mean + x_std
            new_x = ((x >= x_left) & (x <= x_right)) * x
            new_x = new_x.replace({0: np.nan})
            return new_x.mean(skipna=True)

        agg_df = df.groupby(['sensor_id', 'channel']).apply(lambda x: preprocessing(x[fields])).round(5)
        agg_df = agg_df[fields].reset_index()
        agg_df = agg_df.replace({np.nan: None})

        agg_data = []
        for _, row in agg_df.iterrows():
            agg_data_obj = new_table_obj(
                sensor_id=row['sensor_id'],
                channel=row['channel'],
                timestamp=time_list[i],
                pm1_atm=row['pm1_atm'],
                pm2_5_atm=row['pm2_5_atm'],
                pm10_atm=row['pm10_atm'],
                pm1_cf_1=row['pm1_cf_1'],
                pm2_5_cf_1=row['pm2_5_cf_1'],
                pm10_cf_1=row['pm10_cf_1'],
                p_0_3um_cnt=row['p_0_3um_cnt'],
                p_0_5um_cnt=row['p_0_5um_cnt'],
                p_1_0um_cnt=row['p_1_0um_cnt'],
                p_2_5um_cnt=row['p_2_5um_cnt'],
                p_5um_cnt=row['p_5um_cnt'],
                p_10um_cnt=row['p_10um_cnt'],
                rssi=row['rssi'],
                temperature=row['temperature'],
                humidity=row['humidity'])
            agg_data.append(agg_data_obj)
        session.add_all(agg_data)
        session.commit()
    print('Finish one table.')


if __name__ == '__main__':

    # year = 2018
    # new_table_obj = LosAngelesPurpleAirHourly2018
    # purple_air_set = PURPLE_AIR_SET_2018
    #
    # create_table(new_table_obj)
    #
    # for month in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]:
    #     month_str = str(month).rjust(2, '0')
    #     next_month_str = str(month % 12 + 1).rjust(2, '0')
    #     next_year = year if month != 12 else year + 1
    #     config = {
    #         'TABLE': purple_air_set[month],
    #         'START_TIME': f'{year}-{month_str}-01',
    #         'END_TIME': f'{next_year}-{next_month_str}-01',
    #         'NEW_TABLE': new_table_obj
    #     }
    #     main(config)
    #
    # year = 2019
    # new_table_obj = LosAngelesPurpleAirHourly2019
    # purple_air_set = PURPLE_AIR_SET_2019
    #
    # create_table(new_table_obj)
    #
    # for month in [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]:
    #     month_str = str(month).rjust(2, '0')
    #     next_month_str = str(month % 12 + 1).rjust(2, '0')
    #     next_year = year if month != 12 else year + 1
    #     config = {
    #         'TABLE': purple_air_set[month],
    #         'START_TIME': f'{year}-{month_str}-01',
    #         'END_TIME': f'{next_year}-{next_month_str}-01',
    #         'NEW_TABLE': new_table_obj
    #     }
    #     main(config)

    year = 2020
    new_table_obj = LosAngelesPurpleAirHourly2020
    purple_air_set = PURPLE_AIR_SET_2020

    create_table(new_table_obj)

    for month in [1, 3]:
        month_str = str(month).rjust(2, '0')
        next_month_str = str(month % 12 + 1).rjust(2, '0')
        next_year = year if month != 12 else year + 1
        config = {
            'TABLE': purple_air_set[month],
            'START_TIME': f'{year}-{month_str}-01',
            'END_TIME': f'{next_year}-{next_month_str}-01',
            'NEW_TABLE': new_table_obj
        }
        main(config)
