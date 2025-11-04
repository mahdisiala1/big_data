# consumer_test.py (Py3.5, robuste aux non-JSON)
from __future__ import print_function
from kafka import KafkaConsumer
import json

BROKER = "localhost:9092"
TOPIC = "test-json"   # ou agriculture.crop.data

def safe_json(b):
    try:
        return json.loads(b.decode("utf-8"))
    except Exception:
        # retourne brut si pas du JSON (évite crash)
        try:
            return {"_raw": b.decode("utf-8"), "_parse": "error"}
        except Exception:
            return {"_raw": "<binary>", "_parse": "error"}

if __name__ == "__main__":
    c = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BROKER,
        # ne pas désérialiser ici; on fait nous-mêmes dans la boucle
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="test-consumer-2"
    )
    n = 0
    for msg in c:
        val = safe_json(msg.value)
        print("got:", val)
        n += 1
        if n >= 3:
            break
    print("done")

