import requests
from config import INDEX_PREFIX, MAX_PAGES
from elasticsearch_service import Ayah
from elasticsearch.helpers import bulk, BulkIndexError

from logger import logger


def create_index(index_name, es):
    """
    Create a new index with the specified name.
    """
    Ayah.init(using=es, index=index_name)
    logger.info(f"Created index: {index_name}")


def delete_existing_index(index_name, es):
    """
    Delete an existing index with the specified name.
    """
    if es.indices.exists(index=index_name):
        es.indices.delete(index=index_name)
        logger.info(f"Deleted existing index: {index_name}")


def fetch_and_process_data(edition, es, index_name):
    """
    Fetch data from the Quran API and process it.
    """
    base_url = f"http://api.alquran.cloud/v1/page/{{}}/{edition['identifier']}"

    for page_number in range(1, MAX_PAGES + 1):
        url = base_url.format(page_number)
        response = requests.get(url)

        if response.status_code == 200:
            process_page_data(
                response.json().get("data", {}),
                edition,
                es,
                index_name,
                page_number,
                url,
            )
        else:
            handle_error(
                "Failed to fetch page", response.status_code, edition, page_number, url
            )


def process_page_data(page_data, edition, es, index_name, page_number, url):
    """
    Process data from a page and index it.
    """
    ayahs = page_data.get("ayahs", [])
    if ayahs:
        bulk_data = []
        for ayah in ayahs:
            bulk_data.append(
                {
                    "_op_type": "index",
                    "_index": index_name,
                    "_id": str(ayah["number"]),
                    "_source": {
                        "edition_identifier": edition["identifier"],
                        "edition_name_in_arabic": edition["name"],
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
            )

        try:
            # Use the bulk helper to perform bulk insertion
            _, failed = bulk(es, bulk_data, raise_on_error=False)
            if failed:
                # Log details of failed documents
                for doc in failed:
                    handle_error("Failed to index", 500, edition, page_number, url)
        except BulkIndexError:
            # Log the overall bulk indexing error
            handle_error("Error during bulk indexing", 500, edition, page_number, url)
    else:
        handle_error("Ayahs not found", 404, edition, page_number, url)


def handle_error(message, status_code, edition, page_number, url):
    """
    Handle errors during data fetching.
    """
    logger.error(
        f"{message} {page_number} for {edition['identifier']}. Status code: {status_code}"
    )
    logger.info(url)


def process_arabic_edition(edition, es):
    """
    Process Arabic edition including index management, data retrieval, and processing.
    """
    try:
        index_name = INDEX_PREFIX + edition["identifier"]

        delete_existing_index(index_name, es)
        create_index(index_name, es)

        fetch_and_process_data(edition, es, index_name)

    except Exception as e:
        logger.exception(f"An error occurred: {str(e)}")
        handle_error("An error occurred in process_arabic_edition", 500, edition, 0, f"index_name:{index_name}")
