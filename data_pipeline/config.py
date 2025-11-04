# Configuration pour Étudiant 1
KAFKA_BROKER = 'localhost:9092'
HDFS_NAMENODE = 'localhost:9870'

USDA_API_CONFIG = {
    'base_url': 'https://quickstats.nass.usda.gov/api/api_GET/',
    'key': 'EB184EA0-521F-3B0A-ABF2-1E568A06C96A' 
}

TOPICS = {
    'crop_data': 'agriculture.crop.data',
    'sensor_data': 'agriculture.sensor.data'
}