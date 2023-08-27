# This code was generated using ChatGPT3.5
import requests
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Document, Text

base_url = "http://api.alquran.cloud/v1/page/{}/quran-uthmani"

# Define Elasticsearch connection
es = Elasticsearch([{"host": "localhost", "port": 9200}])


# Define Elasticsearch index and mapping
class Ayah(Document):
    page_number = Text()
    ayah_number = Text()
    ayah_text = Text()

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
                ayah_number = ayah["number"]

                ayah_doc = Ayah(
                    page_number=page_number,
                    ayah_number=ayah_number,
                    ayah_text=ayah_text,
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
