# api.py
import requests
from logger import logger
from config import EDITIONS_URL


def fetch_arabic_editions():
    response = requests.get(EDITIONS_URL)
    if response.status_code == 200:
        return response.json().get("data", [])
    else:
        logger.error("Failed to fetch the list of Arabic editions.")
        return []
