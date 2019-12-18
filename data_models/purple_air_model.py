from sqlalchemy import Column, Integer, DateTime, REAL, BigInteger, Text
from data_models.common_db import Base


class PurpleAirTemplate(object):
    __table_args__ = {'schema': 'air_quality_purple_air'}

    uid = Column(BigInteger, primary_key=True, autoincrement=True)
    sensor_id = Column(Integer, nullable=False, index=True)
    channel = Column(Text, nullable=False)
    timestamp = Column(DateTime(timezone=True), nullable=False, index=True)
    pm1_atm = Column(REAL)
    pm2_5_atm = Column(REAL)
    pm10_atm = Column(REAL)
    pm1_cf_1 = Column(REAL)
    pm2_5_cf_1 = Column(REAL)
    pm10_cf_1 = Column(REAL)
    p_0_3um_cnt = Column(REAL)
    p_0_5um_cnt = Column(REAL)
    p_1_0um_cnt = Column(REAL)
    p_2_5um_cnt = Column(REAL)
    p_5um_cnt = Column(REAL)
    p_10um_cnt = Column(REAL)
    rssi = Column(REAL)
    temperature = Column(REAL)
    humidity = Column(REAL)


class LosAngelesPurpleAir201801(PurpleAirTemplate, Base):
    __tablename__ = 'los_angeles_purple_air_201801'


class LosAngelesPurpleAir201802(PurpleAirTemplate, Base):
    __tablename__ = 'los_angeles_purple_air_201802'


class LosAngelesPurpleAir201803(PurpleAirTemplate, Base):
    __tablename__ = 'los_angeles_purple_air_201803'


class LosAngelesPurpleAir201804(PurpleAirTemplate, Base):
    __tablename__ = 'los_angeles_purple_air_201804'


class LosAngelesPurpleAir201805(PurpleAirTemplate, Base):
    __tablename__ = 'los_angeles_purple_air_201805'


class LosAngelesPurpleAir201806(PurpleAirTemplate, Base):
    __tablename__ = 'los_angeles_purple_air_201806'


class LosAngelesPurpleAir201807(PurpleAirTemplate, Base):
    __tablename__ = 'los_angeles_purple_air_201807'


class LosAngelesPurpleAir201808(PurpleAirTemplate, Base):
    __tablename__ = 'los_angeles_purple_air_201808'


class LosAngelesPurpleAir201809(PurpleAirTemplate, Base):
    __tablename__ = 'los_angeles_purple_air_201809'


class LosAngelesPurpleAir201810(PurpleAirTemplate, Base):
    __tablename__ = 'los_angeles_purple_air_201810'


class LosAngelesPurpleAir201811(PurpleAirTemplate, Base):
    __tablename__ = 'los_angeles_purple_air_201811'


class LosAngelesPurpleAir201812(PurpleAirTemplate, Base):
    __tablename__ = 'los_angeles_purple_air_201812'


class LosAngelesPurpleAirHourly2018(PurpleAirTemplate, Base):
    __tablename__ = 'los_angeles_purple_air_hourly_2018'
