# weather_collector.py  (Py3.5-safe)
from __future__ import print_function
import time
import json
import requests
from kafka import KafkaProducer

BROKER = "localhost:9092"
TOPIC  = "agriculture.weather.data"

# 3 champs fictifs autour de l'Iowa (lat/lon)
FIELDS = [
    {"field_id": "field_A", "lat": 41.8781, "lon": -93.0977},
    {"field_id": "field_B", "lat": 41.8855, "lon": -93.1045},
    {"field_id": "field_C", "lat": 41.8660, "lon": -93.0900},
]

# Open-Meteo (pas de clé) - timezone UTC
BASE_URL = "https://api.open-meteo.com/v1/forecast"
HOURLY   = "temperature_2m,relative_humidity_2m,precipitation"

def build_url(lat, lon):
    params = {
        "latitude": str(lat),
        "longitude": str(lon),
        "hourly": HOURLY,
        "forecast_days": "1",
        "timezone": "UTC"
    }
    # Construire l'URL "à la main" pour Py3.5 sans urllib.parse fancy
    query = "&".join(["%s=%s" % (k, v) for (k, v) in params.items()])
    return BASE_URL + "?" + query

def fetch_hourly(lat, lon):
    url = build_url(lat, lon)
    try:
        r = requests.get(url, timeout=10)
        if r.status_code != 200:
            print("HTTP", r.status_code, "for", url)
            return None
        js = r.json()
        hourly = js.get("hourly", {})
        times = hourly.get("time", []) or []
        temps = hourly.get("temperature_2m", []) or []
        hums  = hourly.get("relative_humidity_2m", []) or []
        precs = hourly.get("precipitation", []) or []
        out = []
        N = min(len(times), len(temps), len(hums), len(precs))
        for i in range(N):
            # record météo horaire
            out.append({
                "time_iso_utc": times[i],
                "temperature_2m": temps[i],
                "relative_humidity_2m": hums[i],
                "precipitation": precs[i],
                "lat": lat,
                "lon": lon
            })
        return out
    except Exception as e:
        print("fetch error:", e)
        return None

if __name__ == "__main__":
    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8")
    )
    # Pull une fois maintenant (on peut boucler si on veut un polling régulier)
    total = 0
    for f in FIELDS:
        arr = fetch_hourly(f["lat"], f["lon"])
        if not arr:
            continue
        for rec in arr:
            rec["field_id"] = f["field_id"]
            producer.send(TOPIC, rec)
            total += 1
        # petite pause entre champs
        time.sleep(0.5)
    producer.flush()
    print("weather records sent:", total)
