import requests
import pandas as pd
import json
from datetime import datetime
import s3fs

WA_Endpoint_Current = "http://api.weatherapi.com/v1/current.json"
WA_Endpoint_Forecast = "http://api.weatherapi.com/v1/forecast.json"
API_KEY = "YOUR_API_KEY"
# You may use latlong.net
LAT_LONG = "YOUR_COORDINATES"
DAYS = 5


PARAMETERS = {
    "key": API_KEY,
    "q": LAT_LONG,
    "days": DAYS
}

def run_weather_etl():
    response = requests.get(url=WA_Endpoint_Forecast, params=PARAMETERS)
    response.raise_for_status()
    weather_data = response.json()

    forecasts = weather_data["forecast"]["forecastday"]

    weather_forecast_list = []
    for forecast in forecasts:
        refined_forecast = {
            "date": forecast["date"],
            "maxtemp_c": forecast["day"]["maxtemp_c"],
            "mintemp_c": forecast["day"]["mintemp_c"],
            "maxwind_kph": forecast["day"]["maxwind_kph"],
            "avgvis_km": forecast["day"]["avgvis_km"],
            "avghumidity": forecast["day"]["avghumidity"],
            "daily_will_it_rain": forecast["day"]["daily_will_it_rain"],
            "daily_chance_of_rain": forecast["day"]["daily_chance_of_rain"],
            "uv": forecast["day"]["uv"],
            "condition": forecast["day"]["condition"]["text"],
            "sunrise": forecast["astro"]["sunrise"],
            "sunset": forecast["astro"]["sunset"]
        }
        weather_forecast_list.append(refined_forecast)

    df = pd.DataFrame(weather_forecast_list)
    df.to_csv("s3://weather-data-project-airflow/weather_forecast_data.csv")
