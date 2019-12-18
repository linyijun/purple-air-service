import pytz
import pandas as pd
import numpy as np

from data_models.common_db import session
from data_models.purple_air_model import *
from methods.db_operations import *


def main(config):

    start_time = config['START_TIME']
    end_time = config['END_TIME']
    tz = pytz.timezone('America/Los_Angeles')
    time_list = pd.date_range(start=start_time, end=end_time, freq='H')
    time_list = [tz.localize(x) for x in time_list]

    create_table(config['NEW_TABLE'])
    fields = ['sensor_id', 'channel', 'pm1_atm', 'pm2_5_atm', 'pm10_atm', 'pm1_cf_1', 'pm2_5_cf_1',
              'pm10_cf_1', 'p_0_3um_cnt', 'p_0_5um_cnt', 'p_1_0um_cnt', 'p_2_5um_cnt', 'p_5um_cnt',
              'p_10um_cnt', 'rssi', 'temperature', 'humidity']

    agg_data = []
    table_df = pd.read_sql(session.query(config['TABLES']).statement, session.bind)

    for i, t in enumerate(time_list[:-1]):
        df = table_df[(table_df['timestamp'] >= time_list[i]) & (table_df['timestamp'] < time_list[i + 1])]

        if len(df) == 0:
            continue

        df = df[fields].groupby(['sensor_id', 'channel']).mean().round(5)
        df = df.reset_index()
        df['timestamp'] = t

        for _, row in df.iterrows():
            agg_data_obj = LosAngelesPurpleAirHourly2018(
                sensor_id=row['sensor_id'],
                channel=row['channel'],
                timestamp=t,
                pm1_atm=row['pm1_atm'] if not pd.isna(row['pm1_atm']) else None,
                pm2_5_atm=row['pm2_5_atm'] if not pd.isna(row['pm2_5_atm']) else None,
                pm10_atm=row['pm10_atm'] if not pd.isna(row['pm10_atm']) else None,
                pm1_cf_1=row['pm1_cf_1'] if not pd.isna(row['pm1_cf_1']) else None,
                pm2_5_cf_1=row['pm2_5_cf_1'] if not pd.isna(row['pm2_5_cf_1']) else None,
                pm10_cf_1=row['pm10_cf_1'] if not pd.isna(row['pm10_cf_1']) else None,
                p_0_3um_cnt=row['p_0_3um_cnt'] if not pd.isna(row['p_0_3um_cnt']) else None,
                p_0_5um_cnt=row['p_0_5um_cnt'] if not pd.isna(row['p_0_5um_cnt']) else None,
                p_1_0um_cnt=row['p_1_0um_cnt'] if not pd.isna(row['p_1_0um_cnt']) else None,
                p_2_5um_cnt=row['p_2_5um_cnt'] if not pd.isna(row['p_2_5um_cnt']) else None,
                p_5um_cnt=row['p_5um_cnt'] if not pd.isna(row['p_5um_cnt']) else None,
                p_10um_cnt=row['p_10um_cnt'] if not pd.isna(row['p_10um_cnt']) else None,
                rssi=row['rssi'] if not pd.isna(row['rssi']) else None,
                temperature=row['temperature'] if not pd.isna(row['temperature']) else None,
                humidity=row['humidity'] if not pd.isna(row['humidity']) else None)
            agg_data.append(agg_data_obj)
        session.add_all(agg_data)
        session.commit()
    print('Finish one table.')


if __name__ == '__main__':

    query = [
        [LosAngelesPurpleAir201801, '2018-01-01 00:00:00', '2018-02-01 00:00:00'],
        [LosAngelesPurpleAir201802, '2018-02-01 00:00:00', '2018-03-01 00:00:00'],
        [LosAngelesPurpleAir201803, '2018-03-01 00:00:00', '2018-04-01 00:00:00'],
        [LosAngelesPurpleAir201804, '2018-04-01 00:00:00', '2018-05-01 00:00:00'],
        [LosAngelesPurpleAir201805, '2018-05-01 00:00:00', '2018-06-01 00:00:00'],
        [LosAngelesPurpleAir201806, '2018-06-01 00:00:00', '2018-07-01 00:00:00'],
        [LosAngelesPurpleAir201807, '2018-07-01 00:00:00', '2018-08-01 00:00:00'],
        [LosAngelesPurpleAir201808, '2018-08-01 00:00:00', '2018-09-01 00:00:00'],
        [LosAngelesPurpleAir201809, '2018-09-01 00:00:00', '2018-10-01 00:00:00'],
        [LosAngelesPurpleAir201810, '2018-10-01 00:00:00', '2018-11-01 00:00:00'],
        [LosAngelesPurpleAir201811, '2018-11-01 00:00:00', '2018-12-01 00:00:00'],
        [LosAngelesPurpleAir201812, '2018-12-01 00:00:00', '2019-01-01 00:00:00'],

    ]

    for q in query:
        config = {
            'TABLES': q[0],
            'START_TIME': q[1],
            'END_TIME': q[2],
            'NEW_TABLE': LosAngelesPurpleAirHourly2018
        }
        main(config)
