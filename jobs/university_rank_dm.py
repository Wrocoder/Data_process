from extraction.DMProcess import DMProcessor

from helper.job_helper.decors import Timing
from helper.job_helper.job_helpers import get_config
from logger import LOGGER


@Timing
def geo_main_stg():
    CONFIG = get_config()
    process = DMProcessor()
    process.extract_data(CONFIG['universityRank']['stg']['path'])
    LOGGER.info(f"Transforming data {process}")
    process.transform_data()
    process.load_data(schema='test', table='test')

    return process.__repr__()


if __name__ == "__main__":
    LOGGER.info("Starting process")
    geo_main_stg()