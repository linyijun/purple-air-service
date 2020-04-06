import pytz
import pandas as pd

from data_models.common_db import session
from data_models.purple_air_model import *
from methods.db_operations import create_table
from utils import *


def main(config):

    start_time = config['START_TIME']
    end_time = config['END_TIME']
    tz = pytz.timezone('America/Los_Angeles')
    time_list = pd.date_range(start=start_time, end=end_time, freq='H')
    time_list = [tz.localize(x) for x in time_list]
    table_obj = config['NEW_TABLE']

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

        for _, row in df.iterrows():
            agg_data_obj = table_obj(
                sensor_id=row['sensor_id'],
                channel=row['channel'],
                timestamp=time_list[i],
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

    year = 2018
    new_table_obj = LosAngelesPurpleAirHourly2018
    purple_air_set = PURPLE_AIR_SET_2018

    # create_table(new_table_obj)

    for month in [1, 2, 3]:
        month_str = str(month).rjust(2, '0')
        next_month_str = str(month % 12 + 1).rjust(2, '0')
        next_year = year if month != 12 else year + 1
        config = {
            'TABLES': purple_air_set[month],
            'START_TIME': f'{year}-{month_str}-01',
            'END_TIME': f'{next_year}-{next_month_str}-01',
            'NEW_TABLE': new_table_obj
        }
        main(config)
