import requests
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Document, Integer, Text, Boolean
import logging
from config import ELASTICSEARCH_HOST, ELASTICSEARCH_PORT, ELASTICSEARCH_USERNAME, ELASTICSEARCH_PASSWORD, INDEX_PREFIX, MAX_PAGES, EDITIONS_URL
from logger import configure_logger


logger = configure_logger()

# Define Elasticsearch connection with authentication
ES = Elasticsearch(
    [{"host": ELASTICSEARCH_HOST, "port": ELASTICSEARCH_PORT}],
    http_auth=(ELASTICSEARCH_USERNAME, ELASTICSEARCH_PASSWORD),
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


def fetch_arabic_versions():
    response = requests.get(EDITIONS_URL)
    if response.status_code == 200:
        return response.json().get("data", [])
    else:
        logger.error("Failed to fetch the list of Arabic versions.")
        return []


def configure_logger():
    # Set up logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

    # Configure a file handler for the logger
    file_handler = logging.FileHandler('app_log.log')
    file_handler.setLevel(logging.ERROR)  # Log only errors to the file
    file_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    file_handler.setFormatter(file_formatter)
    logger.addHandler(file_handler)
    return logger


def configure_elasticsearch():
    return Elasticsearch(
        [{"host": ELASTICSEARCH_HOST, "port": ELASTICSEARCH_PORT}],
        http_auth=(ELASTICSEARCH_USERNAME, ELASTICSEARCH_PASSWORD),
    )


def process_arabic_version(version, es):
    try:
        index_name = INDEX_PREFIX + version["identifier"]
        if es.indices.exists(index=index_name):
            es.indices.delete(index=index_name)
            logger.info(f"Deleted existing index: {index_name}")
        Ayah.init(using=es, index=index_name)
        logger.info(f"Created index: {index_name}")

        base_url = f"http://api.alquran.cloud/v1/page/{{}}/{version['identifier']}"
        for page_number in range(1, MAX_PAGES + 1):
            url = base_url.format(page_number)
            response = requests.get(url)

            if response.status_code == 200:
                page_data = response.json().get("data", {})
                ayahs = page_data.get("ayahs", [])

                if ayahs:
                    bulk_data = [
                        {
                            "_op_type": "index",  # Action: Index a document
                            "_index": index_name,  # Index name
                            "_type": "_doc",
                            "_id": str(ayah["number"]),  # Document ID
                            "_source": {
                                "version": version,
                                "ayah_text": ayah["text"],
                                "ayah_number_in_surah": int(ayah["numberInSurah"]),
                                "ayah_number_in_quran": int(ayah["number"]),
                                "ayah_surah_name": ayah["surah"]["name"],
                                "ayah_surah_number": int(ayah["surah"]["number"]),
                                "ayah_page_number": int(ayah["page"]),
                                "ayah_ruku_number": int(ayah["ruku"]),
                                "ayah_hizbQuarter_number": int(ayah["hizbQuarter"]),
                                "ayah_is_sajda": ayah["sajda"],
                            },
                        }
                        for ayah in ayahs
                    ]
                    # Log the content of bulk_data
                    print(f"Bulk Data: {bulk_data[0]}")
                    es.bulk(body=bulk_data)  # The bulk API request
                else:
                    logger.warning(f"Ayahs not found in {version['identifier']}, page {page_number} data.")
                    logger.info(f"URL: {url}")
            else:
                logger.error(
                    f"Failed to fetch page {page_number} for {version['identifier']}. Status code: {response.status_code}"
                )
                logger.info(f"URL: {url}")
                # Log the problematic URL to the file for later retry
                file_handler.error(f"Failed to fetch page {page_number} for {version['identifier']}. Status code: {response.status_code}. URL: {url}")
    except Exception as e:
        logger.exception(f"An error occurred: {str(e)}")


def main():
    logger = configure_logger()
    es = configure_elasticsearch()

    arabic_versions = fetch_arabic_versions()
    if not arabic_versions:
        logger.error("No Arabic versions found. Exiting.")
        return

    for version in arabic_versions:
        process_arabic_version(version, es)

    logger.info("Data retrieval and indexing completed.")
    logger.info("Data provided by AlQuran Cloud API (http://api.alquran.cloud)")
    logger.info("Code developed with assistance from OpenAI's GPT-3 assistant and in collaboration with Mohamad Alsioufi")


if __name__ == "__main__":
    main()
