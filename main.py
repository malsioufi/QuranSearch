import argparse
import sys
from data_processing import process_edition, rerun_failed_links
from logger import logger
from quran_api import fetch_arabic_editions


def main():
    parser = argparse.ArgumentParser(description="Description of your script.")
    parser.add_argument(
        "--rerun",
        action="store_true",
        help="Rerun mode to process failed links from the log file.",
    )
    parser.add_argument(
        "--links_list",
        type=str,
        required="--rerun" in sys.argv,
        help="Path to the log file.",
    )
    args = parser.parse_args()

    if args.rerun:
        # Rerun mode
        rerun_failed_links(args.links_list)
        pass

    else:
        arabic_editions = fetch_arabic_editions()

        if not arabic_editions:
            logger.error("No Arabic versions found. Exiting.")
            return
        # Normal mode
        for arabic_edition in arabic_editions:
            process_edition(edition=arabic_edition)

    logger.info("Data retrieval and indexing completed.")
    logger.info("Data provided by AlQuran Cloud API (http://api.alquran.cloud)")
    logger.info(
        "Code developed with assistance from OpenAI's GPT-3 assistant and in collaboration with Mohamad Alsioufi"
    )


if __name__ == "__main__":
    main()
