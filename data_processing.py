import re
import requests
from config import INDEX_PREFIX, NUMBER_OF_SECTIONS
from elasticsearch_service import Ayah, configure_elasticsearch
from elasticsearch.helpers import bulk, BulkIndexError

from logger import logger
from quran_api import fetch_arabic_editions


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


def get_and_index_edition_data(edition, es, index_name, section="juz"):
    number_of_sections = NUMBER_OF_SECTIONS[section]
    base_url = f"http://api.alquran.cloud/v1/{section}/{{}}/{edition['identifier']}"

    for section_number in range(1, number_of_sections + 1):
        url = base_url.format(section_number)
        try:
            get_and_index_section_data_from_url(
                edition=edition, es=es, index_name=index_name, url=url
            )
        except BulkIndexError as err:
            handle_error(message="Error during bulk indexing", url=url, error=err)
        except Exception as err:
            handle_error(message="Failed to fetch page", url=url, error=err)


def get_and_index_section_data_from_url(edition, es, index_name, url):
    print(f"Getting data from {url}")
    response = requests.get(url)
    if response.status_code == 200:
        ayahs = response.json().get("data", {}).get("ayahs", [])
        if ayahs:
            try:
                failed = index_ayahs(
                    ayahs=ayahs, edition=edition, es=es, index_name=index_name
                )
                if failed:
                    # Log details of failed documents
                    for _ in failed:
                        handle_error(message="Failed to index", url=url)
            except BulkIndexError as err:
                raise err
        else:
            handle_error(message="Ayahs not found", url=url)
    else:
        raise Exception


def index_ayahs(ayahs, edition, es, index_name):
    bulk_data = []
    for ayah in ayahs:
        bulk_data.append(
            prepare_ayah_document(edition=edition, index_name=index_name, ayah=ayah)
        )
    try:
        # Use the bulk helper to perform bulk insertion
        _, failed = bulk(es, bulk_data, raise_on_error=False)
        return failed
    except BulkIndexError as err:
        raise err


def prepare_ayah_document(edition, index_name, ayah):
    return {
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
            "ayah_juz_number": int(ayah["juz"]),
            "ayah_manzil_number": int(ayah["manzil"]),
            "ayah_surah_number": int(ayah["surah"]["number"]),
            "ayah_page_number": int(ayah["page"]),
            "ayah_ruku_number": int(ayah["ruku"]),
            "ayah_hizbQuarter_number": int(ayah["hizbQuarter"]),
            "ayah_is_sajda": ayah["sajda"],
        },
    }


def handle_error(message, url, error: None):
    """
    Handle errors during data fetching.
    """
    logger.error(f"{message} for {url}")
    logger.error(error)
    logger.info(url)


def process_edition(edition):
    """
    Process Arabic edition including index management, data retrieval, and processing.
    """
    try:
        es = configure_elasticsearch()
        index_name = INDEX_PREFIX + edition["identifier"]

        delete_existing_index(index_name=index_name, es=es)
        create_index(index_name=index_name, es=es)

        get_and_index_edition_data(edition=edition, es=es, index_name=index_name)

    except Exception as err:
        handle_error(
            message="An error occurred in process_arabic_edition",
            url=f"index_name:{index_name}",
            error=err,
        )


def rerun_failed_links(links_list_path):
    with open(links_list_path, "r") as links_file:
        links_content = links_file.readlines()

    # Extract URLs using a regular expression
    url_pattern = re.compile(r"http://api.alquran.cloud/v1/page/\d+/([a-zA-Z0-9_-]+)")
    urls_and_editions_to_reprocess = [
        {
            "url": url_pattern.search(line).group(),
            "edition": url_pattern.search(line).group(1),
        }
        for line in links_content
        if url_pattern.search(line)
    ]

    arabic_editions = fetch_arabic_editions()
    editions = [
        {
            "identifier": edition["identifier"],
            "name": edition["name"],
        }
        for edition in arabic_editions
    ]

    es = configure_elasticsearch()

    for url_and_edition in urls_and_editions_to_reprocess:
        edition_identifier = url_and_edition["edition"]
        url = url_and_edition["url"]
        edition = next(
            (
                edition
                for edition in editions
                if edition["identifier"] == edition_identifier
            ),
            None,
        )
        index_name = INDEX_PREFIX + edition_identifier
        get_and_index_section_data_from_url(edition, es, index_name, url)
