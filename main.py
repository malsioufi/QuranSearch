import requests
from config import INDEX_PREFIX, MAX_PAGES
from logger import logger
from api import fetch_arabic_editions
from elasticsearch_service import configure_elasticsearch, Ayah


def process_arabic_edition(edition, es):
    try:
        index_name = INDEX_PREFIX + edition["identifier"]
        if es.indices.exists(index=index_name):
            es.indices.delete(index=index_name)
            logger.info(f"Deleted existing index: {index_name}")
        Ayah.init(using=es, index=index_name)
        logger.info(f"Created index: {index_name}")

        base_url = f"http://api.alquran.cloud/v1/page/{{}}/{edition['identifier']}"
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
                                "version": edition,
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
                    logger.warning(
                        f"Ayahs not found in {edition['identifier']}, page {page_number} data."
                    )
                    logger.info(f"URL: {url}")
            else:
                logger.error(
                    f"Failed to fetch page {page_number} for {edition['identifier']}. Status code: {response.status_code}"
                )
                logger.info(f"URL: {url}")
                # Log the problematic URL to the file for later retry
                logger.error(
                    f"Failed to fetch page {page_number} for {edition['identifier']}. Status code: {response.status_code}. URL: {url}"
                )
    except Exception as e:
        logger.exception(f"An error occurred: {str(e)}")


def main():
    es = configure_elasticsearch()

    arabic_editions = fetch_arabic_editions()

    if not arabic_editions:
        logger.error("No Arabic versions found. Exiting.")
        return

    for edition in arabic_editions:
        process_arabic_edition(edition, es)

    logger.info("Data retrieval and indexing completed.")
    logger.info("Data provided by AlQuran Cloud API (http://api.alquran.cloud)")
    logger.info(
        "Code developed with assistance from OpenAI's GPT-3 assistant and in collaboration with Mohamad Alsioufi"
    )


if __name__ == "__main__":
    main()
