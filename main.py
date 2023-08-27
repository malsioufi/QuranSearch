import requests
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from elasticsearch_dsl import Document, Integer, Text
from concurrent.futures import ThreadPoolExecutor
import os
import time

# API URLs for Uthmani and Simple text versions
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
    ayah_number_in_quran = Integer()
    ayah_text_uthmani = Text(analyzer="arabic")
    ayah_text_simple = Text(analyzer="arabic")
    surah_number = Integer()
    ayah_number_in_surah = Integer()
    surah_name = Text(analyzer="keyword")

    class Index:
        name = "quran_ayahs"


# Function to fetch and process ayahs for a specific page
def process_page(page_number):
    max_retries = 3  # Maximum number of retries
    retry_delay = 2  # Delay in seconds before retrying

    for retry_attempt in range(max_retries):
        url_uthmani = base_url_uthmani.format(page_number)
        url_simple = base_url_simple.format(page_number)

        response_uthmani = requests.get(url_uthmani)
        response_simple = requests.get(url_simple)

        response_uthmani_ok = response_uthmani.status_code == 200
        response_simple_ok = response_simple.status_code == 200

        if response_uthmani_ok and response_simple_ok:
            page_data_uthmani = response_uthmani.json()["data"]
            page_data_simple = response_simple.json()["data"]

            if "ayahs" in page_data_uthmani and "ayahs" in page_data_simple:
                ayahs_uthmani = page_data_uthmani["ayahs"]
                ayahs_simple = page_data_simple["ayahs"]

                bulk_data = []

                for ayah_uthmani, ayah_simple in zip(ayahs_uthmani, ayahs_simple):
                    ayah_number_in_quran = int(ayah_uthmani["number"])
                    surah_number = int(ayah_uthmani["surah"]["number"])
                    ayah_number_in_surah = int(ayah_uthmani["numberInSurah"])
                    surah_name = ayah_uthmani["surah"]["name"]
                    ayah_text_uthmani = ayah_uthmani["text"]
                    ayah_text_simple = ayah_simple["text"]

                    ayah_doc = Ayah(
                        page_number=page_number,
                        ayah_number_in_quran=ayah_number_in_quran,
                        ayah_text_uthmani=ayah_text_uthmani,
                        ayah_text_simple=ayah_text_simple,
                        surah_number=surah_number,
                        ayah_number_in_surah=ayah_number_in_surah,
                        surah_name=surah_name,
                    )

                    bulk_data.append(ayah_doc.to_dict(include_meta=True))

                # Bulk insert the documents for this page
                if bulk_data:
                    bulk(es, bulk_data, index=Ayah.Index.name)
                    print(f"Bulk inserted ayahs for page {page_number}.")
                break  # Break the retry loop if successful
            else:
                print(f"Ayahs not found in page {page_number} data.")
                break  # Break the retry loop if unsuccessful
        else:
            print(
                f"Failed to fetch page {page_number} on attempt {retry_attempt+1}. "
                f"Uthmani Status code: {response_uthmani.status_code}, "
                f"Simple Status code: {response_simple.status_code}"
            )
            if retry_attempt < max_retries - 1:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)


# Create the index if it doesn't exist
if not es.indices.exists(index=Ayah.Index.name):
    Ayah.init(using=es)

# Get the number of CPU cores on the system
num_cores = os.cpu_count()

# Use ThreadPoolExecutor to fetch and process pages in parallel
with ThreadPoolExecutor(max_workers=num_cores) as executor:
    executor.map(process_page, range(1, 605))

print("Bulk indexing completed.")

# Credits to the API provider
print("Data provided by AlQuran Cloud API (http://api.alquran.cloud)")

# Credits to OpenAI assistant for code assistance and User for collaboration
print(
    "Code developed with assistance from OpenAI's GPT-3 assistant and in collaboration with Mohamad Alsioufi"
)
