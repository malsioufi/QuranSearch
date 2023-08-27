import requests
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Document, Integer, Text

base_url = "http://api.alquran.cloud/v1/page/{}/quran-uthmani"

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
    ayah_text = Text()
    surah_number = Integer()
    surah_name = Text()

    class Index:
        name = "quran_ayahs"


# Create the index if it doesn't exist
if not es.indices.exists(index=Ayah.Index.name):
    Ayah.init(using=es)

# Loop through all pages (604 pages in total)
for page_number in range(1, 605):
    url = base_url.format(page_number)

    response = requests.get(url)

    if response.status_code == 200:
        page_data = response.json()["data"]

        if "ayahs" in page_data:
            ayahs = page_data["ayahs"]

            for ayah in ayahs:
                ayah_text = ayah["text"]
                ayah_number = int(ayah["number"])
                surah_number = int(ayah["surah"]["number"])
                surah_name = ayah["surah"]["name"]

                ayah_doc = Ayah(
                    page_number=page_number,
                    ayah_number=ayah_number,
                    ayah_text=ayah_text,
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
            f"Failed to fetch page {page_number}. Status code: {response.status_code}"
        )
