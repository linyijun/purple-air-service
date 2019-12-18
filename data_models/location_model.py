from sqlalchemy import Column, Integer, DateTime, REAL, Text, Float
from geoalchemy2 import Geometry
from data_models.common_db import Base


class PurpleAirLocationTemplate(object):
    __table_args__ = {'schema': 'air_quality_purple_air'}

    id = Column(Integer, nullable=False, index=True, primary_key=True)
    parent_id = Column(Integer, nullable=False, index=True)
    channel = Column(Text)
    label = Column(Text)
    device_location_type = Column(Text)
    thingspeak_primary_id = Column(Integer, nullable=False, index=True)
    thingspeak_primary_id_read_key = Column(Text)
    thingspeak_second_id = Column(Integer, nullable=False, index=True)
    thingspeak_second_id_read_key = Column(Text)
    lat = Column(Float(53), nullable=False)
    lon = Column(Float(53), nullable=False)
    location = Column(Geometry('POINT', srid=4326), nullable=False)


class PurpleAirLocation(PurpleAirLocationTemplate, Base):
    __tablename__ = 'purple_air_locations'


class LosAngelesPurpleAirOutsideLocationView(PurpleAirLocationTemplate, Base):
    __tablename__ = 'los_angeles_purple_air_outside_locations_view'
