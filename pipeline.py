import requests
import psycopg2
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()
print("🌤️ Weather Pipeline Prague - Starting...")

# Open-Meteo API (Prague coordinates)
url = "https://api.open-meteo.com/v1/forecast?latitude=50.08&longitude=14.44&hourly=temperature_2m,precipitation&daily=temperature_2m_max,temperature_2m_min&timezone=Europe%2FPrague"
data = requests.get(url).json()
print(f"📡 Downloaded {len(data['hourly']['time'])} hours of weather data")

# PostgreSQL connection
conn = psycopg2.connect(
    host=os.getenv("DB_HOST"),
    port=os.getenv("DB_PORT"),
    dbname=os.getenv("DB_NAME"),
    user=os.getenv("DB_USER"),
    password=os.getenv("DB_PASS")
)

cur = conn.cursor()

# Create tables
cur.execute("""
CREATE TABLE IF NOT EXISTS weather_hourly (
    timestamp TIMESTAMPTZ PRIMARY KEY,
    temperature_c FLOAT,
    precipitation_mm FLOAT
);
CREATE TABLE IF NOT EXISTS weather_daily (
    date DATE PRIMARY KEY,
    temp_max_c FLOAT,
    temp_min_c FLOAT
);
""")
print("✅ Tables created/verified")

# Insert hourly data
hourly_times = data['hourly']['time']
hourly_temps = data['hourly']['temperature_2m']
hourly_precip = data['hourly']['precipitation']

for t, temp, precip in zip(hourly_times, hourly_temps, hourly_precip):
    cur.execute("INSERT INTO weather_hourly VALUES (%s, %s, %s) ON CONFLICT DO NOTHING", (t, temp, precip))

# Insert daily data
daily_dates = data['daily']['time']
daily_max = data['daily']['temperature_2m_max']
daily_min = data['daily']['temperature_2m_min']

for date, tmax, tmin in zip(daily_dates, daily_max, daily_min):
    cur.execute("INSERT INTO weather_daily VALUES (%s, %s, %s) ON CONFLICT DO NOTHING", (date, tmax, tmin))

conn.commit()
cur.close()
conn.close()

print("✅ Pipeline COMPLETED! Data saved to PostgreSQL!")
print("📊 Check results: docker exec -it weather_db psql -U postgres -c 'SELECT COUNT(*) FROM weather_hourly'")
