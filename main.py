from logger import logger
from quran_api import fetch_arabic_editions
from elasticsearch_service import configure_elasticsearch
from data_processing import process_arabic_edition


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
