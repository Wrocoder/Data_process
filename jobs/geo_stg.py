from extraction.DataProcess import DataProcessor
from helper.job_helper.decors import Timing
from helper.job_helper.job_helpers import get_config
from logger import LOGGER


@Timing
def geo_main_stg():
    CONFIG = get_config()
    process = DataProcessor()
    process.extract_data(source=CONFIG['geoData']['stg']['format'],
                         path=CONFIG['geoData']['stg']['source'])
    LOGGER.info(f"Transforming data {process}")
    process.transform_data()
    process.load_data(destination=CONFIG['geoData']['stg']['destination'])

    return process.__repr__()


if __name__ == "__main__":
    LOGGER.info("Starting process")
    geo_main_stg()
