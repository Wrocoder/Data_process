from extraction.DataProcess import DataProcessor
from helper.job_helper.decors import timing_and_size, measure_memory_usage
from helper.job_helper.job_helpers import get_config
from logger import LOGGER


@timing_and_size
@measure_memory_usage
def uni_main_stg():
    CONFIG = get_config()
    process = DataProcessor()
    process.extract_data(source=CONFIG['universityData']['stg']['format'],
                         path=CONFIG['universityData']['stg']['source'])
    LOGGER.info(f"Transforming data {process}")
    process.transform_data()
    process.load_data(destination=CONFIG['universityData']['stg']['destination'], part_cols=['country', ])


if __name__ == "__main__":
    LOGGER.info("Starting process")
    uni_main_stg()
