from datetime import datetime, timedelta
import requests, json, os
import polars as pl

time_format = '%Y-%m-%dT%H:%M:%S'

PATH = os.path.dirname(__file__)


def weather_api(latitude: str, longitude: str, date_from: datetime, forecast_range: int)->dict:
    weather_url = f"https://weather.visualcrossing.com/VisualCrossingWebServices/rest/services/timeline/{latitude}%2C{longitude}/{date_from.date()}/{(date_from+timedelta(days=forecast_range)).date()}?unitGroup=metric&elements=datetime%2Chumidity%2Ccloudcover%2Csolarradiation&key=WRWE2JJGJSMVERULYB3G55DC7&contentType=json"
    response = requests.get(weather_url)
    if response.status_code==200:
        return response.json()


def parse_weather_api_response(data: dict) -> pl.DataFrame:
    df = pl.DataFrame(schema={'timestamp':pl.Datetime, 'humidity':pl.Float64, 'cloudcover':pl.Float64, 'solarradiation':pl.Float64})
    for day in data['days']:
        for hour in day['hours']:
            timestamp = datetime.strptime(day['datetime']+'T'+hour['datetime'], time_format)
            data = {'timestamp':timestamp, 'humidity':hour['humidity'], 'cloudcover':hour['cloudcover'], 'solarradiation':hour['solarradiation']}
            df = df.vstack(pl.DataFrame(data))
    return df


def query_weather(latitude: str, longitude: str, date_from: datetime, forecast_range: int=7) -> pl.DataFrame:
    file_name = f"{date_from.date()}-{(date_from+timedelta(days=forecast_range)).date()}_{round(latitude,2)},{round(longitude,2)}"
    file_path = os.path.join(PATH, f'../data/bronze/weather/{file_name}')
    if os.path.exists(file_path):
        df = pl.read_parquet(file_path)
    else:
        data = weather_api(latitude=latitude, longitude=longitude, date_from=date_from, forecast_range=forecast_range)
        df = parse_weather_api_response(data)
        df.write_parquet(file_path)
    return df

if __name__ == "__main__":
    latitude=59.7266804377451
    longitude=10.8655261797295
    date_from = datetime.now()
    forecast_range = 7
    weather_df = query_weather(latitude=latitude, longitude=longitude, date_from=datetime.now(), forecast_range=forecast_range)
