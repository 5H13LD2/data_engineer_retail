"""
Weather API Fetcher
Kumukuha ng weather data mula sa OpenWeatherMap API
para sa mga store locations ng retail pipeline.
"""

import requests
import json
import os
from datetime import datetime


def fetch_weather(city_name):
    """Fetch current weather data para sa isang city."""
    API_KEY = os.environ.get("OPENWEATHER_API_KEY")
    if not API_KEY:
        raise ValueError("❌ OPENWEATHER_API_KEY is missing from environment variables!")
    BASE_URL = "http://api.openweathermap.org/data/2.5/weather"

    params = {
        "q": city_name,
        "appid": API_KEY,
        "units": "metric"
    }

    response = requests.get(BASE_URL, params=params)

    if response.status_code == 200:
        return response.json()
    else:
        print(f"❌ Error fetching data for {city_name}: {response.status_code}")
        return None


def fetch_all_weather(cities):
    """Fetch weather data para sa lahat ng cities."""
    weather_results = []

    for city in cities:
        print(f"  Fetching weather for {city}...")
        data = fetch_weather(city)
        if data:
            weather_results.append({
                "city": city,
                "temp": data["main"]["temp"],
                "feels_like": data["main"]["feels_like"],
                "humidity": data["main"]["humidity"],
                "pressure": data["main"]["pressure"],
                "condition": data["weather"][0]["main"],
                "description": data["weather"][0]["description"],
                "wind_speed": data["wind"]["speed"],
                "timestamp": datetime.now().isoformat()
            })
            print(f"    ✅ {city}: {data['main']['temp']}°C, {data['weather'][0]['main']}")

    return weather_results


def fetch_weather_for_cities(cities, output_file="weather_data.json"):
    """
    Main entry point — called by the Airflow DAG.
    Fetches weather data for all cities and saves to output_file.
    """
    print(f"🌤️  Fetching weather data for {len(cities)} cities...")
    weather_results = fetch_all_weather(cities)

    # Ensure output directory exists
    output_dir = os.path.dirname(output_file)
    if output_dir:
        os.makedirs(output_dir, exist_ok=True)

    with open(output_file, "w") as f:
        json.dump(weather_results, f, indent=2)

    print(f"✅ {len(weather_results)} cities saved to {output_file}")
    return output_file


if __name__ == "__main__":
    # Store locations from our retail data
    cities = [
        "Manila", "Cebu City", "Davao City", "Quezon City", "Makati",
        "Pasig", "Taguig", "Iloilo City", "Bacolod", "Cagayan de Oro",
        "Zamboanga", "Baguio", "General Santos", "Caloocan", "Las Pinas"
    ]

    print("🌤️  Fetching weather data for all store locations...\n")
    weather_results = fetch_all_weather(cities)

    # I-save bilang JSON para i-upload sa S3 later
    output_dir = os.path.join(os.path.dirname(__file__), "output")
    os.makedirs(output_dir, exist_ok=True)

    output_file = os.path.join(output_dir, "weather_data.json")
    with open(output_file, "w") as f:
        json.dump(weather_results, f, indent=2)

    print(f"\n✅ Weather data fetched successfully!")
    print(f"   {len(weather_results)} cities saved to {output_file}")
