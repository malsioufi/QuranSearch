import requests
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Document, Integer, Text

base_url_uthmani = "http://api.alquran.cloud/v1/page/{}/quran-uthmani"
base_url_simple = "http://api.alquran.cloud/v1/page/{}/quran-simple"

# Elasticsearch authentication credentials
es_username = "your_username"
es_password = "your_password"

# Define Elasticsearch connection with authentication
es = Elasticsearch(
    [{"host": "localhost", "port": 9200}], http_auth=(es_username, es_password)
)


# Define Elasticsearch index and mapping
class Ayah(Document):
    page_number = Integer()
    ayah_number = Integer()
    ayah_text_uthmani = Text()
    ayah_text_simple = Text()
    surah_number = Integer()
    surah_name = Text()

    class Index:
        name = "quran_ayahs"


# Create the index if it doesn't exist
if not es.indices.exists(index=Ayah.Index.name):
    Ayah.init(using=es)

# Loop through all pages (604 pages in total)
for page_number in range(1, 605):
    url_uthmani = base_url_uthmani.format(page_number)
    url_simple = base_url_simple.format(page_number)

    response_uthmani = requests.get(url_uthmani)
    response_simple = requests.get(url_simple)

    if response_uthmani.status_code == 200 and response_simple.status_code == 200:
        page_data_uthmani = response_uthmani.json()["data"]
        page_data_simple = response_simple.json()["data"]

        if "ayahs" in page_data_uthmani and "ayahs" in page_data_simple:
            ayahs_uthmani = page_data_uthmani["ayahs"]
            ayahs_simple = page_data_simple["ayahs"]

            for ayah_uthmani, ayah_simple in zip(ayahs_uthmani, ayahs_simple):
                ayah_number = int(ayah_uthmani["number"])
                surah_number = int(ayah_uthmani["surah"]["number"])
                surah_name = ayah_uthmani["surah"]["name"]
                ayah_text_uthmani = ayah_uthmani["text"]
                ayah_text_simple = ayah_simple["text"]

                ayah_doc = Ayah(
                    page_number=page_number,
                    ayah_number=ayah_number,
                    ayah_text_uthmani=ayah_text_uthmani,
                    ayah_text_simple=ayah_text_simple,
                    surah_number=surah_number,
                    surah_name=surah_name,
                )

                # Insert the ayah document into Elasticsearch
                ayah_doc.save(using=es)

            print(f"Inserted ayahs for page {page_number} into Elasticsearch.")

        else:
            print(f"Ayahs not found in page {page_number} data.")

    else:
        print(
            f"Failed to fetch page {page_number}. Uthmani Status code: {response_uthmani.status_code}, Simple Status code: {response_simple.status_code}"
        )
