#sensor_simulator.py  (Py3.5)
from __future__ import print_function
from kafka import KafkaProducer
import json, time, random, math

BROKER = "localhost:9092"
TOPIC = "agriculture.sensor.data"

FIELDS = ["field_A", "field_B", "field_C"]
SENSORS = ["soil_moisture", "temperature", "humidity", "ph_level"]

def rand_coord(base_lat, base_lon, radius_km=1.0):
    # dispersion simple en degres ~ radius_km
    dlat = (random.random() - 0.5) * (radius_km / 111.0) * 2.0
    dlon = (random.random() - 0.5) * (radius_km / 111.0) * 2.0 / math.cos(base_lat * math.pi/180.0)
    return base_lat + dlat, base_lon + dlon

if __name__ == "__main__":
    p = KafkaProducer(bootstrap_servers=BROKER,
                      value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                      retries=3)
    base_lat, base_lon = 41.8781, -93.0977  # Iowa approx
    i = 0
    while True:
        field = random.choice(FIELDS)
        stype = random.choice(SENSORS)
        lat, lon = rand_coord(base_lat, base_lon, 2.0)

        if stype == "soil_moisture":
            value = random.randint(10, 80)
        elif stype == "temperature":
            value = round(random.uniform(10.0, 35.0), 1)
        elif stype == "humidity":
            value = random.randint(20, 95)
        else:
            value = round(random.uniform(5.5, 7.5), 2)

        msg = {
            "field_id": field,
            "sensor_type": stype,
            "timestamp": int(time.time()),
            "value": value,
            "location": {"lat": lat, "lon": lon}
        }
        p.send(TOPIC, msg)
        i += 1
        if i % 20 == 0:
            p.flush()
            print("sent %d sensor msgs" % i)
        time.sleep(1)

