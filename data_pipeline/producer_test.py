
from __future__ import print_function
from kafka import KafkaProducer
import json, time

BROKER = "localhost:9092"
TOPIC = "test-json"

if __name__ == "__main__":
    p = KafkaProducer(bootstrap_servers=BROKER,
                      value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                      retries=3)
    for i in range(3):
        msg = {"i": i, "msg": "hello"}
        p.send(TOPIC, msg)
        print("sent:", msg)
        time.sleep(0.2)
    p.flush()
    print("done")

