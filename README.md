**ETL:** Open-Meteo API → Docker PostgreSQL (168 hourly records)

##Quick Start
```bash
docker run -d --name weather_db -e POSTGRES_PASSWORD=12345 -p 5432:5432 postgres:15
python3 -m venv venv && source venv/bin/activate
pip install -r requirements.txt
python pipeline.py
