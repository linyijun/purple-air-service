import json
import time
import datetime
import pytz
import requests
import pandas as pd
import numpy as np
import os


INTERVAL = 60


def to_float(s):
    try:
        return float(s)
    except Exception as e:
        print(e)
        return None


def round_time(dt, round_scale=60*INTERVAL):
    seconds = (dt.replace(tzinfo=None) - dt.min).seconds
    rounding = (seconds + round_scale / 2) // round_scale * round_scale
    rounding = dt + datetime.timedelta(0, rounding - seconds, -dt.microsecond)
    return rounding.replace(tzinfo=pytz.utc)


def to_avg(input_dict):
    output_dict = dict()
    for k, v in input_dict.items():
        new_v = [float(i) for i in v if i is not None and i > 0]
        if len(new_v) > 0:
            output_dict[k] = np.round(np.median(np.array(new_v)), 3)
    return output_dict


def one_time_request(url):

    for i in range(3):  # the maximum attempts is three times
        try:
            json_data = requests.get(url)
            data = json.loads(json_data.text)
            return data.get('channel'), data.get('feeds')

        except Exception as e:
            print('Fail to fetch {}: Attempt [{}/3].'.format(e, i + 1))
            time.sleep(60)
            continue
    return None


def one_instance_request(id_, key_, start, end):

    url = f'https://thingspeak.com/channels/{id_}/feeds.json?api_key={key_}&start={start}&end={end}'
    channel, json_data = one_time_request(url)

    assert channel['field2'] == 'PM2.5 (ATM)', 'the field 2 do not match'
    assert channel['field6'] == 'Temperature', 'the field 6 do not match'
    assert channel['field7'] == 'Humidity', 'the field 7 do not match'

    data = []
    if json_data is not None and len(json_data) > 0:
        for d in json_data:
            rounded_time = round_time(datetime.datetime.strptime(d['created_at'], '%Y-%m-%dT%H:%M:%SZ'))
            data.append([rounded_time, to_float(d['field2']), to_float(d['field6']), to_float(d['field7'])])
    return data


def multiple_instance_request(sensor, time_list, period):

    pm25_dict = {t: [] for t in time_list}
    temperature_dict = {t: [] for t in time_list}
    humidity_dict = {t: [] for t in time_list}

    for i in range(0, len(time_list), period):

        start, end = time_list[i: i + period + 1][0], time_list[i: i + period + 1][-1]
        start = str(start).split(' ')[0] + '%20' + str(start).split(' ')[1][:-6]
        end = str(end).split(' ')[0] + '%20' + str(end).split(' ')[1][:-6]

        primary_a = one_instance_request(sensor['primary_id_a'], sensor['primary_key_a'], start, end)

        for item in primary_a:
            t = item[0]
            pm25_dict[t].append(item[1])
            temperature_dict[t].append(item[2])
            humidity_dict[t].append(item[3])
        print('Finish the request {}.'.format(str(time_list[i])))

    pm25_dict = to_avg(pm25_dict)
    temperature_dict = to_avg(temperature_dict)
    humidity_dict = to_avg(humidity_dict)

    sensor_data = [pm25_dict, temperature_dict, humidity_dict]
    return sensor_data


def time_to_time_request(start_time, end_time, locations, f):

    # From time to time request
    tz = pytz.timezone('America/Los_Angeles')
    time_list = pd.date_range(start=start_time, end=end_time, freq=f'{INTERVAL}min')
    time_list = [tz.localize(x) for x in time_list]
    time_list_utc = [x.astimezone(pytz.utc) for x in time_list]

    period = int(60 / INTERVAL * 24 * 4)
    for _, row in locations.iterrows():
        sensor = {'sensor_index': row['sensor_index'],
                  'primary_id_a': row['primary_id_a'],
                  'primary_key_a': row['primary_key_a']}

        print('Start the request {}.'.format(sensor['sensor_index']))
        sensor_data = multiple_instance_request(sensor, time_list_utc, period)

        for i in range(len(time_list)):
            t, t_utc = time_list[i], time_list_utc[i]
            data = [str(row['sensor_index']), str(t)]
            for item in sensor_data:
                data.append(str(item[t_utc]) if item.get(t_utc) is not None else '')
            f.write(','.join(data) + '\n')
        time.sleep(1)


if __name__ == '__main__':

    locations = pd.read_csv('./data/la_county_sensor_locations.csv')
    root = '/data/yijun/DeepLATTE/purple-air'
    
    times = []
    for year in [2021, 2022]:
        for month in range(1, 13):
            start = f'{year}-{month:02d}-01'
            if month == 12:
                end = f'{year + 1}-01-01'
            else:
                end = f'{year}-{month + 1:02d}-01'
            times.append((start, end))
     
    for start, end in times[:19]:
        print('-----------------', start, end, '-----------------')
        file = open(os.path.join(root, 'la_county_sensor_observations_{}.csv'.format(start.split('-')[0] + start.split('-')[1])), 'a')
        file.write('sensor_id,timestamp,pm2.5,temperature,humidity\n')
        time_to_time_request(start_time=start, end_time=end, locations=locations, f=file)
