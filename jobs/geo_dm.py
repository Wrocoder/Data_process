from extraction.DMProcess import DMProcessor

from helper.job_helper.decors import timing_and_size
from helper.job_helper.job_helpers import get_config
from logger import LOGGER


@timing_and_size
def geo_main_dm():
    CONFIG = get_config()
    process = DMProcessor()
    process.extract_data(CONFIG['geoData']['stg']['destination'])
    LOGGER.info(f"Transforming data {process}")
    process.transform_data()
    process.load_data(schema=CONFIG['geoData']['dm']['schema'],
                      table=CONFIG['geoData']['dm']['table'])

    return process.__repr__()


if __name__ == "__main__":
    LOGGER.info("Starting process")
    geo_main_dm()
