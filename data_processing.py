import requests
from config import INDEX_PREFIX, MAX_PAGES
from elasticsearch_service import Ayah

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
            handle_error(response.status_code, edition, page_number, url)


def process_page_data(page_data, edition, es, index_name, page_number, url):
    """
    Process data from a page and index it.
    """
    ayahs = page_data.get("ayahs", [])

    if ayahs:
        bulk_data = [
            {
                "_op_type": "index",  # Action: Index a document
                "_index": index_name,  # Index name
                "_type": "_doc",
                "_id": str(ayah["number"]),  # Document ID
                "_source": {
                    "version": edition,
                    "ayah_text": ayah["text"],
                    # ... (other fields)
                },
            }
            for ayah in ayahs
        ]
        es.bulk(body=bulk_data)
    else:
        logger.warning(
            f"Ayahs not found in {edition['identifier']}, page {page_number} data."
        )
        logger.info(f"URL: {url}")


def handle_error(status_code, edition, page_number, url):
    """
    Handle errors during data fetching.
    """
    logger.error(
        f"Failed to fetch page {page_number} for {edition['identifier']}. Status code: {status_code}"
    )
    logger.info(f"URL: {url}")
    # Log the problematic URL to the file for later retry
    logger.error(
        f"Failed to fetch page {page_number} for {edition['identifier']}. Status code: {status_code}. URL: {url}"
    )


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
