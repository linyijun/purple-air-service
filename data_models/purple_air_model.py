from sqlalchemy import Column, Integer, DateTime, REAL, BigInteger, Text
from data_models.common_db import Base


class PurpleAirTemplate(object):
    __table_args__ = {'schema': 'purple_air'}

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


class LosAngelesPurpleAir201901(PurpleAirTemplate, Base):
    __tablename__ = 'los_angeles_purple_air_201901'


class LosAngelesPurpleAir201902(PurpleAirTemplate, Base):
    __tablename__ = 'los_angeles_purple_air_201902'


class LosAngelesPurpleAir201903(PurpleAirTemplate, Base):
    __tablename__ = 'los_angeles_purple_air_201903'


class LosAngelesPurpleAir201904(PurpleAirTemplate, Base):
    __tablename__ = 'los_angeles_purple_air_201904'


class LosAngelesPurpleAir201905(PurpleAirTemplate, Base):
    __tablename__ = 'los_angeles_purple_air_201905'


class LosAngelesPurpleAir201906(PurpleAirTemplate, Base):
    __tablename__ = 'los_angeles_purple_air_201906'


class LosAngelesPurpleAir201907(PurpleAirTemplate, Base):
    __tablename__ = 'los_angeles_purple_air_201907'


class LosAngelesPurpleAir201908(PurpleAirTemplate, Base):
    __tablename__ = 'los_angeles_purple_air_201908'


class LosAngelesPurpleAir201909(PurpleAirTemplate, Base):
    __tablename__ = 'los_angeles_purple_air_201909'


class LosAngelesPurpleAir201910(PurpleAirTemplate, Base):
    __tablename__ = 'los_angeles_purple_air_201910'


class LosAngelesPurpleAir201911(PurpleAirTemplate, Base):
    __tablename__ = 'los_angeles_purple_air_201911'


class LosAngelesPurpleAir201912(PurpleAirTemplate, Base):
    __tablename__ = 'los_angeles_purple_air_201912'


class LosAngelesPurpleAirHourly2019(PurpleAirTemplate, Base):
    __tablename__ = 'los_angeles_purple_air_hourly_2019'


class LosAngelesPurpleAir202001(PurpleAirTemplate, Base):
    __tablename__ = 'los_angeles_purple_air_202001'


class LosAngelesPurpleAir202002(PurpleAirTemplate, Base):
    __tablename__ = 'los_angeles_purple_air_202002'


class LosAngelesPurpleAir202003(PurpleAirTemplate, Base):
    __tablename__ = 'los_angeles_purple_air_202003'


class LosAngelesPurpleAir202004(PurpleAirTemplate, Base):
    __tablename__ = 'los_angeles_purple_air_202004'


class LosAngelesPurpleAir202005(PurpleAirTemplate, Base):
    __tablename__ = 'los_angeles_purple_air_202005'


class LosAngelesPurpleAir202006(PurpleAirTemplate, Base):
    __tablename__ = 'los_angeles_purple_air_202006'


class LosAngelesPurpleAir202007(PurpleAirTemplate, Base):
    __tablename__ = 'los_angeles_purple_air_202007'


class LosAngelesPurpleAir202008(PurpleAirTemplate, Base):
    __tablename__ = 'los_angeles_purple_air_202008'


class LosAngelesPurpleAir202009(PurpleAirTemplate, Base):
    __tablename__ = 'los_angeles_purple_air_202009'


class LosAngelesPurpleAir202010(PurpleAirTemplate, Base):
    __tablename__ = 'los_angeles_purple_air_202010'


class LosAngelesPurpleAir202011(PurpleAirTemplate, Base):
    __tablename__ = 'los_angeles_purple_air_202011'


class LosAngelesPurpleAir202012(PurpleAirTemplate, Base):
    __tablename__ = 'los_angeles_purple_air_202012'


class LosAngelesPurpleAirHourly2020(PurpleAirTemplate, Base):
    __tablename__ = 'los_angeles_purple_air_hourly_2020'
