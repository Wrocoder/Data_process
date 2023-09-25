from extraction.DMProcess import DMProcessor

from helper.job_helper.decors import timing_and_size
from helper.job_helper.job_helpers import get_config
from logger import LOGGER


@timing_and_size
def university_main_dm():
    CONFIG = get_config()
    process = DMProcessor()
    process.extract_data(CONFIG['universityData']['stg']['destination'])
    LOGGER.info(f"Transforming data {process}")
    process.transform_data()
    process.load_data(schema=CONFIG['universityData']['dm']['schema'],
                      table=CONFIG['universityData']['dm']['table'])

    return process.__repr__()


if __name__ == "__main__":
    LOGGER.info("Starting process")
    university_main_dm()