import time

import datetime
import json
import pytz
import requests


def round_time(dt, round_scale=60*10):
    seconds = (dt.replace(tzinfo=None) - dt.min).seconds
    rounding = (seconds + round_scale / 2) // round_scale * round_scale
    rounding = dt + datetime.timedelta(0, rounding - seconds, -dt.microsecond)
    return rounding.replace(tzinfo=pytz.utc)


def to_float(s):
    try:
        return float(s)
    except Exception as e:
        print(e)
        return None


def to_avg(l):
    return round(sum(l) / len(l), 5) if len(l) != 0 else None


class Primary:
    def __init__(self, sensor_id, channel, input_data):
        self.sensor_id = sensor_id
        self.channel = channel
        self.timestamp = round_time(datetime.datetime.strptime(input_data['created_at'], '%Y-%m-%dT%H:%M:%SZ'))
        self.pm1_atm = to_float(input_data.get('field1'))
        self.pm2_5_atm = to_float(input_data.get('field2'))
        self.pm10_atm = to_float(input_data.get('field3'))
        self.rssi = to_float(input_data.get('field5')) if channel == 'a' else None
        self.temperature = to_float(input_data.get('field6')) if channel == 'a' else None
        self.humidity = to_float(input_data.get('field7')) if channel == 'a' else None
        self.pm2_5_cf_1 = to_float(input_data.get('field8'))


class Primary10Min:
    def __init__(self, input_data):
        self.sensor_id = input_data[0].sensor_id
        self.channel = input_data[0].channel
        self.timestamp = input_data[0].timestamp
        self.pm1_atm = []
        self.pm2_5_atm = []
        self.pm10_atm = []
        self.rssi = []
        self.temperature = []
        self.humidity = []
        self.pm2_5_cf_1 = []
        self.avg10min(input_data)

    def avg10min(self, input_data):
        for obj in input_data:
            if obj.pm1_atm is not None and obj.pm1_atm > 0:
                self.pm1_atm.append(obj.pm1_atm)
            if obj.pm2_5_atm is not None and obj.pm2_5_atm > 0:
                self.pm2_5_atm.append(obj.pm2_5_atm)
            if obj.pm10_atm is not None and obj.pm10_atm > 0:
                self.pm10_atm.append(obj.pm10_atm)
            if obj.rssi is not None:
                self.rssi.append(obj.rssi)
            if obj.temperature is not None and obj.temperature > 0:
                self.temperature.append(obj.temperature)
            if obj.humidity is not None and obj.humidity > 0:
                self.humidity.append(obj.humidity)
            if obj.pm2_5_cf_1 is not None and obj.pm2_5_cf_1 > 0:
                self.pm2_5_cf_1.append(obj.pm2_5_cf_1)
        self.pm1_atm = to_avg(self.pm1_atm)
        self.pm2_5_atm = to_avg(self.pm2_5_atm)
        self.pm10_atm = to_avg(self.pm10_atm)
        self.rssi = to_avg(self.rssi)
        self.temperature = to_avg(self.temperature)
        self.humidity = to_avg(self.humidity)
        self.pm2_5_cf_1 = to_avg(self.pm2_5_cf_1)


class Secondary:
    def __init__(self, sensor_id, channel, input_data):
        self.sensor_id = sensor_id
        self.channel = channel
        self.timestamp = round_time(datetime.datetime.strptime(input_data['created_at'], '%Y-%m-%dT%H:%M:%SZ'))
        self.p_0_3um_cnt = to_float(input_data.get('field1'))
        self.p_0_5um_cnt = to_float(input_data.get('field2'))
        self.p_1_0um_cnt = to_float(input_data.get('field3'))
        self.p_2_5um_cnt = to_float(input_data.get('field4'))
        self.p_5um_cnt = to_float(input_data.get('field5'))
        self.p_10um_cnt = to_float(input_data.get('field6'))
        self.pm1_cf_1 = to_float(input_data.get('field7'))
        self.pm10_cf_1 = to_float(input_data.get('field8'))


class Secondary10Min:
    def __init__(self, input_data):
        self.sensor_id = input_data[0].sensor_id
        self.channel = input_data[0].channel
        self.timestamp = input_data[0].timestamp
        self.p_0_3um_cnt = []
        self.p_0_5um_cnt = []
        self.p_1_0um_cnt = []
        self.p_2_5um_cnt = []
        self.p_5um_cnt = []
        self.p_10um_cnt = []
        self.pm1_cf_1 = []
        self.pm10_cf_1 = []
        self.avg10min(input_data)

    def avg10min(self, input_data):
        for obj in input_data:
            if obj.p_0_3um_cnt is not None and obj.p_0_3um_cnt > 0:
                self.p_0_3um_cnt.append(obj.p_0_3um_cnt)
            if obj.p_0_5um_cnt is not None and obj.p_0_5um_cnt > 0:
                self.p_0_5um_cnt.append(obj.p_0_5um_cnt)
            if obj.p_1_0um_cnt is not None and obj.p_1_0um_cnt > 0:
                self.p_1_0um_cnt.append(obj.p_1_0um_cnt)
            if obj.p_2_5um_cnt is not None and obj.p_2_5um_cnt > 0:
                self.p_2_5um_cnt.append(obj.p_2_5um_cnt)
            if obj.p_5um_cnt is not None and obj.p_5um_cnt > 0:
                self.p_5um_cnt.append(obj.p_5um_cnt)
            if obj.p_10um_cnt is not None and obj.p_10um_cnt > 0:
                self.p_10um_cnt.append(obj.p_10um_cnt)
            if obj.pm1_cf_1 is not None and obj.pm1_cf_1 > 0:
                self.pm1_cf_1.append(obj.pm1_cf_1)
            if obj.pm10_cf_1 is not None and obj.pm10_cf_1 > 0:
                self.pm10_cf_1.append(obj.pm10_cf_1)
        self.p_0_3um_cnt = to_avg(self.p_0_3um_cnt)
        self.p_0_5um_cnt = to_avg(self.p_0_5um_cnt)
        self.p_1_0um_cnt = to_avg(self.p_1_0um_cnt)
        self.p_2_5um_cnt = to_avg(self.p_2_5um_cnt)
        self.p_5um_cnt = to_avg(self.p_5um_cnt)
        self.p_10um_cnt = to_avg(self.p_10um_cnt)
        self.pm1_cf_1 = to_avg(self.pm1_cf_1)
        self.pm10_cf_1 = to_avg(self.pm10_cf_1)


def multiple_instance_request(loc, time_list, period):

    primary_data_dict, secondary_data_dict = {t: [] for t in time_list}, {t: [] for t in time_list}

    for i in range(0, len(time_list), period):
        primary_data, secondary_data = one_instance_request(loc, time_list[i: i + period + 1])
        for d in primary_data:
            if primary_data_dict.get(d.timestamp) is not None:
                primary_data_dict[d.timestamp].append(d)
        for d in secondary_data:
            if secondary_data_dict.get(d.timestamp) is not None:
                secondary_data_dict[d.timestamp].append(d)
        print('Finish the request {}.'.format(str(time_list[i])))

    purple_air_data = merge_data(primary_data_dict, secondary_data_dict, time_list[:-1])
    return purple_air_data


def one_instance_request(loc, time_list):

    start_time_str = str(time_list[0]).split(' ')[0] + '%20' + str(time_list[0]).split(' ')[1][:-6]
    end_time_str = str(time_list[-1]).split(' ')[0] + '%20' + str(time_list[-1]).split(' ')[1][:-6]

    primary_data, secondary_data = [], []
    sensor_id, parent_id, channel, primary_id, primary_key, secondary_id, secondary_key = loc

    # primary
    url = 'https://thingspeak.com/channels/{id}/feeds.json?api_key={api_key}&start={start}&end={end}'\
        .format(id=primary_id, api_key=primary_key, start=start_time_str, end=end_time_str)
    json_data = one_time_request(url)
    if json_data is not None and len(json_data) > 0:
        for d in json_data:
            primary_data.append(Primary(sensor_id, channel, d))

    # secondary
    url = 'https://thingspeak.com/channels/{id}/feeds.json?api_key={api_key}&start={start}&end={end}' \
        .format(id=secondary_id, api_key=secondary_key, start=start_time_str, end=end_time_str)
    json_data = one_time_request(url)
    if json_data is not None and len(json_data) > 0:
        for d in json_data:
            secondary_data.append(Secondary(sensor_id, channel, d))

    return primary_data, secondary_data


def merge_data(primary_data_dict, secondary_data_dict, time_list):

    all_data = []
    for t in time_list:
        tmp = {}
        p, s = primary_data_dict[t], secondary_data_dict[t]
        if p is not None and len(p) > 0:
            avg_p = Primary10Min(p)
            tmp['sensor_id'] = avg_p.sensor_id
            tmp['channel'] = avg_p.channel
            tmp['timestamp'] = t
            tmp['pm1_atm'] = avg_p.pm1_atm
            tmp['pm2_5_atm'] = avg_p.pm2_5_atm
            tmp['pm10_atm'] = avg_p.pm10_atm
            tmp['temperature'] = avg_p.temperature
            tmp['humidity'] = avg_p.humidity
            tmp['rssi'] = avg_p.rssi
            tmp['pm2_5_cf_1'] = avg_p.pm2_5_cf_1

        if s is not None and len(s) > 0:
            avg_s = Secondary10Min(s)
            tmp['sensor_id'] = avg_s.sensor_id
            tmp['channel'] = avg_s.channel
            tmp['timestamp'] = t
            tmp['pm1_cf_1'] = avg_s.pm1_cf_1
            tmp['pm10_cf_1'] = avg_s.pm10_cf_1
            tmp['p_0_3um_cnt'] = avg_s.p_0_3um_cnt
            tmp['p_0_5um_cnt'] = avg_s.p_0_5um_cnt
            tmp['p_1_0um_cnt'] = avg_s.p_1_0um_cnt
            tmp['p_2_5um_cnt'] = avg_s.p_2_5um_cnt
            tmp['p_5um_cnt'] = avg_s.p_5um_cnt
            tmp['p_10um_cnt'] = avg_s.p_10um_cnt

        if len(tmp) > 0:
            all_data.append(tmp)
    return all_data


def one_time_request(url):

    for i in range(3):  # the maximum attempts is three times
        try:
            json_data = requests.get(url)
            data = json.loads(json_data.text)
            return data.get('feeds')

        except Exception as e:
            print('Fail to fetch {}: Attempt [{}/3].'.format(e, i + 1))
            time.sleep(60)
            continue
    return None
