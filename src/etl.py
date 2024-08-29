import os
import pandas as pd
import requests
import json
from logger import Logger
from datetime import datetime
from pathlib import Path
from dotenv import load_dotenv
from database import init_db, get_session
from schemas import WeatherSchema, CitiesSchema
from models import WeatherInfo, CitiesInfo

load_dotenv()
logger = Logger(source="etl")

def extract(city: str) -> None:
    city = city.replace(" ", "+")
    api_url = (
        f"http://api.weatherapi.com/v1/current.json?key={os.getenv('API_KEY')}&q={city}"
    )
    try:
        response = requests.get(api_url)
    except requests.exceptions.RequestException as e:
        logger.log(level='error', message=f'An error occurred while fetching weather data: \n({e})')
        raise Exception("An error occurred while fetching weather data")

    if response.status_code == requests.codes.ok:
        data_path = Path("./data/raw_data")
        os.makedirs(data_path, exist_ok=True)

        timestamp = int(datetime.timestamp(datetime.now()))
        with open(data_path.joinpath(f"data_{timestamp}.json"), "w") as f:
            json.dump(response.json(), f)
    else:
        logger.log(level='error', message=f'Failed to fetch data. Status code: {response.status_code}')
        raise Exception(f"Failed to fetch data. Status code: {response.status_code}")

def join_data() -> None:
    raw_data_path = Path("./data/raw_data")
    transformed_data_path = Path("./data/transformed_data")
    os.makedirs(transformed_data_path, exist_ok=True)

    try:
        data_list = [f for f in os.listdir(raw_data_path) if f.endswith(".json")]
    except:
        logger.log(level='error', message='Failed to load raw data')
        raise Exception("Failed to load raw data")

    df_list = []

    for file_name in data_list:
        tmp_df = pd.read_json(raw_data_path.joinpath(file_name))
        tmp = pd.DataFrame(
            {
                "city": [tmp_df["location"]["name"]],
                "region": [tmp_df["location"]["region"]],
                "country": [tmp_df["location"]["country"]],
                "latitude": [tmp_df["location"]["lat"]],
                "longitude": [tmp_df["location"]["lon"]],
                "time": [tmp_df["location"]["localtime"]],
                "temperature": [tmp_df["current"]["temp_c"]],
                "humidity": [tmp_df["current"]["humidity"]],
                "wind_speed": [tmp_df["current"]["wind_mph"]],
            }
        )
        df_list.append(tmp)
    df_final = pd.concat(df_list, ignore_index=True)
    csv_file = transformed_data_path.joinpath("data.csv")
    if csv_file.exists():
        data = pd.read_csv(csv_file)
        data = pd.concat([data, df_final], ignore_index=True)
        data.to_csv(csv_file, index=False)
    else:
        df_final.to_csv(csv_file, index=False)

def transform() -> None:
    transformed_data_path = Path("./data/transformed_data")
    data_file = transformed_data_path.joinpath("data.csv")

    if not data_file.exists():
        logger.log(level='error', message='Transformed data not found')
        raise Exception("Transformed data not found.")

    df = pd.read_csv(data_file)

    cities_df = df[
        ["city", "region", "country", "latitude", "longitude"]
    ].drop_duplicates()
    weather_df = df[["city", "time", "temperature", "humidity", "wind_speed"]]

    cities_file = transformed_data_path.joinpath("cities.csv")
    values_file = transformed_data_path.joinpath("values.csv")

    cities_df.to_csv(cities_file, index=False)
    weather_df.to_csv(values_file, index=False)
    logger.log(level='info', message=f"Data transformation complete. 'cities.csv and 'values.csv' saved in {transformed_data_path}")

def load() -> None:
    transformed_data_path = Path("./data/transformed_data")
    cities_df = pd.read_csv(transformed_data_path.joinpath("cities.csv"))
    weather_df = pd.read_csv(transformed_data_path.joinpath("values.csv"))

    init_db()

    with next(get_session()) as session:
        for _, row in cities_df.iterrows():
            instance = session.query(CitiesInfo).filter_by(
                city=row.city,
                region=row.region,
                latitude=row.latitude,
                longitude=row.longitude
            ).first()

            if not instance:
                val_data = CitiesSchema(**row.to_dict())
                instance = CitiesInfo(**val_data.model_dump())
                session.add(instance)
        session.commit()
        logger.log(level='info', message='Cities data successfully created in the database.')

    with next(get_session()) as session:
        for _, row in weather_df.iterrows():
            instance = session.query(WeatherInfo).filter_by(
                city=row.city,
                time=row.time,
                temperature=row.temperature,
                humidity=row.humidity,
                wind_speed=row.wind_speed
            ).first()

            if not instance:
                val_data = WeatherSchema(**row.to_dict())
                instance = WeatherInfo(**val_data.model_dump())
                session.add(instance)
        session.commit()
        logger.log(level='info', message='Weather data successfully created in the database')

if __name__ == "__main__":
    city_list = ["Cachoeira do Arari", "Belem", "Manaus", "Sao Paulo"]
    for city in city_list:
        extract(city=city)
    join_data()
    transform()
    load()
