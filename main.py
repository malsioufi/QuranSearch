# This code was generated using ChatGPT3.5

import requests
from elasticsearch import Elasticsearch

# Fetch data from the APIs
quran_simple_url = "http://api.alquran.cloud/v1/quran/quran-simple"
quran_uthmani_url = "http://api.alquran.cloud/v1/quran/quran-uthmani"

response_simple = requests.get(quran_simple_url)
response_uthmani = requests.get(quran_uthmani_url)

data_simple = response_simple.json()["data"]["surahs"]
data_uthmani = response_uthmani.json()["data"]["surahs"]

# Combine surahs and ayas
combined_data = []
for i in range(len(data_simple)):
    surah = {
        "number": data_simple[i]["number"],
        "name": data_simple[i]["name"],
        "revelation_type": data_simple[i]["revelation_type"],
        "ayahs_simple": data_simple[i]["ayahs"],
        "ayahs_uthmani": data_uthmani[i]["ayahs"]
    }
    combined_data.append(surah)

# Connect to Elasticsearch
es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

# Index the combined data
index_name = "quran_combined"
index_settings = {
    "settings": {
        "number_of_shards": 1,
        "number_of_replicas": 0
    },
    "mappings": {
        "properties": {
            "number": {"type": "integer"},
            "name": {"type": "text"},
            "revelation_type": {"type": "text"},
            "ayahs_simple": {"type": "nested"},
            "ayahs_uthmani": {"type": "nested"}
        }
    }
}

# Create the index with settings and mappings
es.indices.create(index=index_name, body=index_settings)

# Index each surah
for surah in combined_data:
    es.index(index=index_name, body=surah)

print("Data indexed successfully.")
