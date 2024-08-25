from sqlalchemy.orm import relationship, declarative_base
from sqlalchemy import Column, Integer, String, Float, ForeignKey, DateTime

Base = declarative_base()


class CitiesInfo(Base):
    __tablename__ = "cities_table"

    id = Column(Integer, primary_key=True, autoincrement=True)
    city = Column(String, nullable=False, unique=True)
    region = Column(String, nullable=False)
    latitude = Column(Float, nullable=False)
    longitude = Column(Float, nullable=False)

    weather_info = relationship("WeatherInfo", back_populates="city_info")


class WeatherInfo(Base):
    __tablename__ = "weather_info_table"

    id = Column(Integer, primary_key=True, autoincrement=True)
    city = Column(Integer, ForeignKey("cities_table.id"), nullable=False)
    time = Column(DateTime, nullable=False)
    temperature = Column(Float)
    humidity = Column(Float)
    wind_speed = Column(Float)

    city_info = relationship("CitiesInfo", back_populates="weather_info")
