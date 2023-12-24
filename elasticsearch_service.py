# elasticsearch_service.py
from elasticsearch import Elasticsearch
from elasticsearch_dsl import Document, Integer, Object, Text, Boolean
from config import (
    ELASTICSEARCH_HOST,
    ELASTICSEARCH_PORT,
    ELASTICSEARCH_USERNAME,
    ELASTICSEARCH_PASSWORD,
)
from logger import logger


# Define Elasticsearch index and mapping
class Ayah(Document):
    edition_identifier = Text(analyzer="keyword")
    edition_name_in_arabic = Text(analyzer="keyword")
    ayah_text = Text(analyzer="arabic", required=True)
    ayah_surah_name = Text(analyzer="keyword")

    ayah_number_in_surah = Integer()
    ayah_number_in_quran = Integer()
    ayah_surah_number = Integer()
    ayah_page_number = Integer()
    ayah_ruku_number = Integer()
    ayah_hizbQuarter_number = Integer()

    ayah_is_sajda = Object(
        properties={"id": Integer(), "recommended": Boolean(), "obligatory": Boolean()},
        enabled=False,
    )

    @classmethod
    def init(cls, index=None, using=None):
        logger.info(f"Creating index: {index or 'quran_ayahs'}")
        index_name = index or "quran_ayahs"
        return super().init(index=index_name, using=using)


def configure_elasticsearch():
    return Elasticsearch(
        [{"host": ELASTICSEARCH_HOST, "port": ELASTICSEARCH_PORT, "scheme": "http"}],
        http_auth=(ELASTICSEARCH_USERNAME, ELASTICSEARCH_PASSWORD),
    )
