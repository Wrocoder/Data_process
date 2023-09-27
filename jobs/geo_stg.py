from extraction.DataProcess import DataProcessor
from helper.job_helper.decors import timing_and_size
from helper.job_helper.job_helpers import get_config
from logger import LOGGER


@timing_and_size
def geo_main_stg():
    CONFIG = get_config()
    process = DataProcessor()
    process.extract_data(source=CONFIG['geoData']['stg']['format'],
                         path=CONFIG['geoData']['stg']['source'])
    LOGGER.info(f"Transforming data {process}")
    process.transform_data()
    process.load_data(destination=CONFIG['geoData']['stg']['destination'])

    return repr(process)


if __name__ == "__main__":
    LOGGER.info("Starting process")
    geo_main_stg()
