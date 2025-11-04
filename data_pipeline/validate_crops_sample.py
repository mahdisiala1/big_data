# validate_crops_sample.py  (Py3.5)
from __future__ import print_function
from hdfs import InsecureClient
import json, datetime
from collections import Counter

HDFS_WEB = "http://localhost:50070"
SAMPLE_N = 200

def head_lines(client, hdfs_path, n=200):
    out = []
    with client.read(hdfs_path, encoding="utf-8") as reader:
        for i, line in enumerate(reader):
            if i >= n: break
            out.append(line.rstrip("\n"))
    return out

def is_floaty(s):
    try:
        float(str(s).replace(",", "").strip())
        return True
    except Exception:
        return False

if __name__ == "__main__":
    client = InsecureClient(HDFS_WEB, user="root")
    hour_path = "/agriculture/raw/crop-data/%s/batch.jsonl" % datetime.datetime.utcnow().strftime("%Y/%m/%d/%H")
    st = client.status(hour_path, strict=False)
    if not st:
        print("No file at:", hour_path)
        raise SystemExit(0)

    lines = head_lines(client, hour_path, SAMPLE_N)
    total = len(lines)
    empty_val = 0
    non_numeric = 0
    states = Counter()
    crops = Counter()

    for ln in lines:
        try:
            js = json.loads(ln)
        except Exception:
            non_numeric += 1
            continue
        v = js.get("Value", "")
        if v in ("", None):
            empty_val += 1
        elif not is_floaty(v):
            non_numeric += 1

        stname = js.get("state_name") or js.get("location_desc") or ""
        if stname:
            states[stname] += 1
        crop = js.get("commodity_desc") or ""
        if crop:
            crops[crop] += 1

    print("Sample size:", total)
    print("Empty Value:", empty_val)
    print("Non-numeric Value:", non_numeric)
    print("Top states:", list(states.most_common(5)))
    print("Top crops:", list(crops.most_common(5)))
    print("HDFS path:", hour_path)
