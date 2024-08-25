from pydantic import BaseModel
from datetime import datetime


class CitiesSchema(BaseModel):
    city: str
    region: str
    latitude: float
    longitude: float


class WeatherSchema(BaseModel):
    city: str
    time: datetime
    temperature: float
    humidity: float
    wind_speed: float
