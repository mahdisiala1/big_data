# hdfs_sink_crop.py  (Py3.5, create-or-append)
from __future__ import print_function
from kafka import KafkaConsumer
from hdfs import InsecureClient
import json, datetime, io, os

BROKER = "localhost:9092"
TOPIC = "agriculture.crop.data"
HDFS_WEB = "http://localhost:50070"  # Hadoop 2.x
BATCH_SIZE = 50

def hdfs_path_for_now():
    ts = datetime.datetime.utcnow().strftime("%Y/%m/%d/%H")
    return "/agriculture/raw/crop-data/%s/batch.jsonl" % ts

def safe_dump(obj):
    try:
        return json.dumps(obj)
    except Exception:
        return json.dumps({"_parse":"error"})

if __name__ == "__main__":
    c = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BROKER,
        value_deserializer=lambda b: json.loads(b.decode("utf-8")),
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        group_id="hdfs-sink-crop-1"
    )
    client = InsecureClient(HDFS_WEB, user="root")
    buf = io.StringIO()
    n = 0

    for msg in c:
        buf.write(safe_dump(msg.value) + "\n")
        n += 1

        if n % BATCH_SIZE == 0:
            path = hdfs_path_for_now()
            d = os.path.dirname(path)
            client.makedirs(d)

            # create-or-append
            exists = client.status(path, strict=False) is not None
            data = buf.getvalue()
            if exists:
                client.write(path, data=data, overwrite=False, append=True)
            else:
                client.write(path, data=data, overwrite=True)

            buf = io.StringIO()
            print("wrote %d lines -> %s" % (n, path))

