# Smart Agriculture — Étudiant 1 (Ingestion)

- USDA (réel) → Kafka → HDFS (JSONL, par heure UTC)
- Capteurs simulés → Kafka → HDFS (JSONL, par heure UTC)

## Fichiers
- `test_services.py` : tests Kafka/HDFS/USDA
- `usda_collector.py` : collecte USDA → Kafka (`agriculture.crop.data`)
- `sensor_simulator.py` : capteurs simulés → Kafka (`agriculture.sensor.data`)
- `hdfs_sink_crop.py` : Kafka → HDFS `/agriculture/raw/crop-data/YYYY/MM/DD/HH/batch.jsonl`
- `hdfs_sink_sensor.py` : Kafka → HDFS `/agriculture/raw/sensor-data/YYYY/MM/DD/HH/batch.jsonl`
- `scripts/start_all.sh` : démarre ZK/Kafka + topics (legacy, --zookeeper)
- `scripts/run_ingestion.sh` : lance les sinks HDFS

## Lancement
1) `./start-hadoop.sh`
2) `./scripts/start_all.sh`
3) `python3 test_services.py`
4) `python3 usda_collector.py`  (USDA → Kafka)
5) `python3 sensor_simulator.py` (capteurs → Kafka)
6) `./scripts/run_ingestion.sh`  (Kafka → HDFS)

## Vérifs HDFS
- `hdfs dfs -ls -R /agriculture/raw/crop-data | head`
- `hdfs dfs -cat /agriculture/raw/crop-data/$(date -u +%Y/%m/%d/%H)/batch.jsonl | head -n 5`
- `hdfs dfs -du -h  /agriculture/raw/crop-data/$(date -u +%Y/%m/%d/%H)/batch.jsonl`

## Notes
- Hadoop 2.x (UI 50070), Kafka avec ZooKeeper, Python 3.5 (pas d’emoji/f-strings).
