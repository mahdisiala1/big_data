# usda_collector.py  (Python 3.5, ASCII only)
from __future__ import print_function
from kafka import KafkaProducer
import requests
import json
import time

KAFKA_BROKER = 'localhost:9092'
TOPIC = 'agriculture.crop.data'
USDA_URL = 'https://quickstats.nass.usda.gov/api/api_GET/'
USDA_KEY = 'EB184EA0-521F-3B0A-ABF2-1E568A06C96A'

class USDADataCollector(object):
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=KAFKA_BROKER,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            retries=5
        )

    def fetch_crop_data(self, crop='CORN', year='2023'):
        """
        Récupère les données USDA pour une culture/année (niveau STATE)
        et les publie dans Kafka.
        """
        params = {
            'key': USDA_KEY,
            'commodity_desc': crop,
            'year': year,
            'agg_level_desc': 'STATE',
            'statisticcat_desc': 'YIELD',   # utile pour restreindre
            'format': 'JSON'
            # NOTE: QuickStats ignore souvent 'limit', on filtre côté client si besoin
        }
        try:
            r = requests.get(USDA_URL, params=params, timeout=30)
            if r.status_code != 200:
                print("API ERROR: HTTP %d" % r.status_code)
                return None

            js = r.json()
            data = js.get('data', [])
            # nettoyage minimal: uniformiser Value (supprimer les virgules)
            for rec in data:
                v = rec.get('Value', '0')
                if isinstance(v, str):
                    rec['Value'] = v.replace(',', '')

            print("USDA %s %s: %d rows" % (crop, year, len(data)))

            # Envoi Kafka
            for record in data:
                self.producer.send(TOPIC, record)
            self.producer.flush()
            return data

        except Exception as e:
            print("ERROR:", e)
            return None

    def collect_multiple_crops(self):
        crops = ['CORN', 'SOYBEANS', 'WHEAT']
        for c in crops:
            self.fetch_crop_data(crop=c, year='2023')
            time.sleep(1)  # petite pause pour l'API

if __name__ == "__main__":
    collector = USDADataCollector()
    collector.collect_multiple_crops()
    print("USDA collection finished")

