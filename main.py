import pandas as pd
import pydantic # automatic data validation
import requests
import json
import os
from datetime import datetime
import uuid


def load_countries():
    with open('capitals.json', 'r') as f:
        countries_data = json.load(f)

    return(pd.DataFrame.from_dict(countries_data, orient='index', columns=['name', 'official_name', 'capital', 'cca2', 'ccn3', 'latitude', 'longitude']).reset_index(drop=True))

def read_api_key():
    global API_KEY
    API_KEY = os.getenv("WEATHER_API_KEY")
    print(API_KEY)

def get_current_weather(countries, timestamp):
    url = "https://api.openweathermap.org/data/3.0/onecall?lat={lat}&lon={lon}&units=metric&exclude=hourly,daily&appid={API_KEY}"

    weather_dict = {}
    weather_type_dict = {}

    for _, row in countries.iterrows():
        url_formatted = url.format(lat=row['latitude'], lon=row['longitude'], API_KEY=API_KEY)
        print(url_formatted)
        response = requests.get(url_formatted).json()
        execution_date_hour = datetime.fromtimestamp(timestamp)

        # transfer to Airflow
        folder_path = f"./data/raw/8_hours/{execution_date_hour.year}/{execution_date_hour.month}/{execution_date_hour.day}/{execution_date_hour.hour}/"
        os.makedirs(folder_path, exist_ok=True)
    

        with open(f"./data/raw/8_hours/{execution_date_hour.year}/{execution_date_hour.month}/{execution_date_hour.day}/{execution_date_hour.hour}/{row['name']}.json", 'w') as f:
            json.dump(response, f)

        id = str(uuid.uuid4()).replace('-','')
        weather:dict = response['current']
        weather['id'] = id
        weather['timestamp'] = timestamp
        weather_types = weather.pop('weather')
        # weather_type_dict[row['name']] = weather_type
        # weather['weather_id'] = weather_type['id']
        weather_dict[id] = weather

        for type in weather_types:
            type['weather_id'] = id
            # type_id = str(uuid.uuid4()).replace('-','')
            type['type_id'] = type['id']
            # type['id'] = type_id
            weather_type_dict[id] = type[['weather_id', 'type_id']]

    weather_df = pd.DataFrame.from_dict(weather_dict, orient='index')[['id', 'temp', 'feels_like', 'pressure', 'humidity', 'dew_point', 'uvi', 'clouds', 'visibility', 'wind_speed', 'wind_deg', 'timestamp']]
    # weather_type_df = pd.DataFrame.from_dict(weather_type_dict, orient='index')
    print(weather_df)
    weather_type_df = pd.DataFrame.from_dict(weather_type_dict, orient='index')
    print(weather_type_df)
        



def main(**kwargs):
    print(kwargs)
    read_api_key()
    countries = load_countries()
    execution_time = kwargs['execution_time']
    print(execution_time)
    # execution_date_hour = datetime.fromtimestamp(execution_time)
    # year = execution_date_hour.year
    # month = execution_date_hour.month
    # day = execution_date_hour.day
    # hour = execution_date_hour.hour
 

    # every 8 hours read current data, at 6am, 2pm, 10pm. 
    # at 6am read also data for previous day
    get_current_weather(countries, execution_time)





if __name__ == '__main__':
    # main(execution_date = int(datetime.now().timestamp()))
    main(execution_time = 1757851200) #2pm