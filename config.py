# config.py

ELASTICSEARCH_HOST = "localhost"
ELASTICSEARCH_PORT = 9200
ELASTICSEARCH_USERNAME = "your_username"
ELASTICSEARCH_PASSWORD = "your_password"
INDEX_PREFIX = "ayahs_in_"
NUMBER_OF_SECTIONS = {
    "juz": 30,
    "surah": 114,
    "manzil": 7,
    "rukus": 556,
    "page": 604,
    "hizbQuarter": 240
}
EDITIONS_URL = "https://api.alquran.cloud/v1/edition?format=text&language=ar&type=quran"
