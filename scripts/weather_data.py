import requests
from elasticsearch import Elasticsearch
from datetime import datetime

# OpenWeather API configuration
API_KEY = '9a5d364c490ce5a5e74c309a5f7862f7'
ELASTICSEARCH_HOST = 'https://localhost:9200'  # Elasticsearch cluster address

# Connect to Elasticsearch
es = Elasticsearch(
    [ELASTICSEARCH_HOST],
    http_auth=('elastic', 'Lon67ail5'),
    verify_certs=True,
    ca_certs='/etc/elasticsearch/elastic-certificate-ca.pem'
)

# List of cities to track
cities = ['New York', 'Phoenix', 'Sunrise', 'Salt Lake City', 'London', 'Gurgaon', 'Singapore']


# Thresholds for extreme weather conditions
EXTREME_TEMP_THRESHOLD = 303.15
HIGH_WIND_SPEED_THRESHOLD = 10.0

def fetch_weather_data(city):
    """
    Fetches weather data for a given city from the OpenWeather API.
    """
    url = f'http://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}'
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching data for {city}: {response.status_code}")
        return None

def transform_data(data, city):
    """
    Transforms raw weather data into the desired format for Elasticsearch.
    """
    temperature = data['main']['temp']
    feels_like_temperature = data['main']['feels_like']
    wind_speed = data['wind'].get('speed', 0.0)
    wind_gust = data['wind'].get('gust', 0.0)
    wind_direction = data['wind'].get('deg', 0)
    
    # Determine if conditions are extreme
    is_extreme_temperature = temperature > EXTREME_TEMP_THRESHOLD
    is_high_wind_speed = wind_speed > HIGH_WIND_SPEED_THRESHOLD

    # Geographical location (latitude, longitude)
    geo_location = {
        'lat': data['coord']['lat'],
        'lon': data['coord']['lon']
    }

    # Prepare the document to be indexed
    doc = {
        'city': city,
        'timestamp': datetime.utcnow(),
        'temperature': temperature,
        'feels_like_temperature': feels_like_temperature,
        'humidity': data['main']['humidity'],
        'pressure': data['main']['pressure'],
        'visibility': data.get('visibility', 0),
        'cloud_coverage': data['clouds']['all'],
        'wind_speed': wind_speed,
        'wind_gust': wind_gust,
        'wind_direction': wind_direction,
        'rain_volume_last_hour': data.get('rain', {}).get('1h', 0.0),
        'rain_volume_last_3_hours': data.get('rain', {}).get('3h', 0.0),
        'snow_volume_last_hour': data.get('snow', {}).get('1h', 0.0),
        'snow_volume_last_3_hours': data.get('snow', {}).get('3h', 0.0),
        'weather_description': data['weather'][0]['description'],
        'weather_condition_code': data['weather'][0]['id'],
        'sunrise': datetime.utcfromtimestamp(data['sys']['sunrise']),
        'sunset': datetime.utcfromtimestamp(data['sys']['sunset']),
        'is_extreme_temperature': is_extreme_temperature,
        'is_high_wind_speed': is_high_wind_speed,
        'geo_location': geo_location
    }
    return doc

def index_data_to_elasticsearch(doc):
    """
    Indexes the transformed weather data into Elasticsearch.
    """
    doc['call'] = 0

    res = es.index(index='weather_data', body=doc)
    print(f"Indexed document for {doc['city']} with result: {res['result']}")

if __name__ == "__main__":
    # Fetch, transform, and index weather data for each city
    for city in cities:
        weather_data = fetch_weather_data(city)
        if weather_data:
            transformed_data = transform_data(weather_data, city)
            index_data_to_elasticsearch(transformed_data)