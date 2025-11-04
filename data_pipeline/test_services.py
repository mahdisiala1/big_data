# test_services.py  (Py3.5 friendly, ASCII only)
from __future__ import print_function
from kafka import KafkaProducer
import requests
import json
import socket

KAFKA_BROKER = "localhost:9092"
USDA = {
    "base_url": "https://quickstats.nass.usda.gov/api/api_GET/",
    "key": "EB184EA0-521F-3B0A-ABF2-1E568A06C96A"
}

# Hadoop 2.x UI par défaut sur 50070 (tu l'as confirmé avec ss)
HDFS_PORTS = [50070, 9870]

def test_kafka():
    try:
        p = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )
        p.send("test-topic", {"ping": "hello"})
        p.flush()
        print("Kafka OK")
        return True
    except Exception as e:
        print("Kafka ERROR:", e)
        return False

def test_hdfs():
    ok = False
    for port in HDFS_PORTS:
        try:
            # test TCP
            s = socket.create_connection(("localhost", port), timeout=3)
            s.close()
            # test HTTP (via requests)
            r = requests.get("http://localhost:%d" % port, timeout=5)
            if r.status_code == 200:
                print("HDFS UI OK on port %d" % port)
                ok = True
                break
            else:
                print("HDFS UI HTTP status %d on port %d" % (r.status_code, port))
        except Exception:
            # Essaie le prochain port
            pass
    if not ok:
        print("HDFS NOT accessible on ports 50070/9870")
    return ok

def test_usda():
    try:
        params = {
            "key": USDA["key"],
            "commodity_desc": "CORN",
            "statisticcat_desc": "YIELD",
            "agg_level_desc": "STATE",
            "year__GE": "2020",
            "format": "JSON",
        }
        r = requests.get(USDA["base_url"], params=params, timeout=15)
        r.raise_for_status()
        js = r.json()
        data = js.get("data", [])
        n = len(data)
        print("USDA OK: %d rows" % n)
        return n > 0
    except Exception as e:
        print("USDA ERROR:", e)
        return False

if __name__ == "__main__":
    print("== Smoke tests (Py3.5 / ASCII) ==")
    test_kafka()
    test_hdfs()
    test_usda()

