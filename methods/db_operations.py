from data_models.common_db import engine, session


def create_table(table_obj):

    try:
        table_obj.__table__.drop(bind=engine, checkfirst=True)
        table_obj.__table__.create(bind=engine)
        return

    except Exception as e:
        print(e)
        exit(-1)


def insert_locations(data, location_table_obj):
    data_obj = []
    for item in data:
        obj = location_table_obj(
            sensor_id=item.get('sensor_id'),
            parent_id=item.get('parent_id'),
            channel=item.get('channel'),
            label=item.get('label'),
            device_location_type=item.get('device_location_type'),
            thingspeak_primary_id=item.get('thingspeak_primary_id'),
            thingspeak_primary_id_read_key=item.get('thingspeak_primary_id_read_key'),
            thingspeak_second_id=item.get('thingspeak_second_id'),
            thingspeak_second_id_read_key=item.get('thingspeak_second_id_read_key'),
            lon=item.get('lon'),
            lat=item.get('lat'),
            location='SRID=4326;POINT({} {})'.format(item.get('lon'), item.get('lat')))
        data_obj.append(obj)
    session.add_all(data_obj)
    session.commit()


def insert_ppa_data(data, data_table_obj):
    data_obj = []
    for item in data:
        if len(item) == 0 or item.get('sensor_id') is None:
            continue
        obj = data_table_obj(
            sensor_id=item.get('sensor_id'),
            channel=item.get('channel'),
            timestamp=item.get('timestamp'),
            pm1_atm=item.get('pm1_atm'),
            pm2_5_atm=item.get('pm2_5_atm'),
            pm10_atm=item.get('pm10_atm'),
            pm1_cf_1=item.get('pm1_cf_1'),
            pm2_5_cf_1=item.get('pm2_5_cf_1'),
            pm10_cf_1=item.get('pm10_cf_1'),
            # pm1_atm=item.get('pm1_cf_1'),  # this is before 2019/10/20
            # pm2_5_atm=item.get('pm2_5_cf_1'),
            # pm10_atm=item.get('pm10_cf_1'),
            # pm1_cf_1=item.get('pm1_atm'),
            # pm2_5_cf_1=item.get('pm2_5_atm'),
            # pm10_cf_1=item.get('pm10_atm'),
            p_0_3um_cnt=item.get('p_0_3um_cnt'),
            p_0_5um_cnt=item.get('p_0_5um_cnt'),
            p_1_0um_cnt=item.get('p_1_0um_cnt'),
            p_2_5um_cnt=item.get('p_2_5um_cnt'),
            p_5um_cnt=item.get('p_5um_cnt'),
            p_10um_cnt=item.get('p_10um_cnt'),
            rssi=item.get('rssi'),
            temperature=item.get('temperature'),
            humidity=item.get('humidity'))
        data_obj.append(obj)
    session.add_all(data_obj)
    session.commit()
