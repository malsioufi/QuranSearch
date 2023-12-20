import requests
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Document, Integer, Text, Boolean

# Define the API URL to get the list of Arabic versions
editions_url = "https://api.alquran.cloud/v1/edition?format=text&language=ar&type=quran"

# Fetch the list of Arabic versions
response = requests.get(editions_url)

if response.status_code == 200:
    arabic_versions = response.json()["data"]
else:
    print("Failed to fetch the list of Arabic versions.")
    arabic_versions = []

# Elasticsearch authentication credentials
es_username = "your_username"
es_password = "your_password"

# Define Elasticsearch connection with authentication
es = Elasticsearch(
    [{"host": "localhost", "port": 9200}], http_auth=(es_username, es_password)
)


# Define Elasticsearch index and mapping
class Ayah(Document):
    version = Text(analyzer="keyword")
    ayah_text = Text(required=True)
    ayah_number_in_surah = Integer()
    ayah_number_in_quran = Integer()
    ayah_surah_name = Text(analyzer="keyword")
    ayah_surah_number = Integer()
    ayah_page_number = Integer()
    ayah_ruku_number = Integer()
    ayah_hizbQuarter_number = Integer()
    ayah_is_sajda = Boolean()

    @classmethod
    def init(cls, index=None, using=None):
        index_name = index or "quran_ayahs"
        return super().init(index=index_name, using=using)


# Function to fetch and process ayahs for a specific Arabic version
def process_arabic_version(version):
    # Delete the index if it exists
    index_name = f"ayahs_in_{version['identifier']}"
    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)
        print(f"Deleted existing index: {index_name}")
    # Create the index
    Ayah.init(using=es, index=index_name)
    print(f"Created index: {index_name}")
    print(f"\t\t{version['identifier']}")
    base_url = f"http://api.alquran.cloud/v1/page/{{}}/{version['identifier']}"
    for page_number in range(1, 605):
        print(f"\t\t\tPage: {page_number}")
        url = base_url.format(page_number)
        response = requests.get(url)
        if response.status_code == 200:
            page_data = response.json()["data"]
            ayahs = page_data.get("ayahs")
            if ayahs:
                for ayah in ayahs:
                    ayah_number_in_quran = int(ayah["number"])
                    ayah_doc = Ayah(
                        version=version,
                        ayah_text=ayah["text"],
                        ayah_number_in_surah=int(ayah["numberInSurah"]),
                        ayah_number_in_quran=ayah_number_in_quran,
                        ayah_surah_name=ayah["surah"]["name"],
                        ayah_surah_number=int(ayah["surah"]["number"]),
                        ayah_page_number=int(ayah["page"]),
                        ayah_ruku_number=int(ayah["ruku"]),
                        ayah_hizbQuarter_number=int(ayah["hizbQuarter"]),
                        ayah_is_sajda=ayah["sajda"],
                    )
                    ayah_doc.meta.id = f"{ayah_number_in_quran}"
                    ayah_doc.save()
            else:
                print(
                    f"Ayahs not found in {version['identifier']}, page {page_number} data."
                )
                print(f"URL: {url}")
        else:
            print(
                f"Failed to fetch page {page_number} for {version['identifier']}. Status code: {response.status_code}"
            )
            print(f"URL: {url}")


# Fetch and process ayahs for each Arabic version
for version in arabic_versions:
    process_arabic_version(version)

print("Data retrieval and indexing completed.")

# Credits to the API provider
print("Data provided by AlQuran Cloud API (http://api.alquran.cloud)")

# Credits to OpenAI assistant for code assistance and User for collaboration
print(
    "Code developed with assistance from OpenAI's GPT-3 assistant and in collaboration "
    "with Mohamad Alsioufi"
)
