from extraction.DataProcess import DataProcessor
from helper.job_helper.decors import timing_and_size, measure_memory_usage
from helper.job_helper.job_helpers import get_config
from helper.spec.univers_rank_spec import spec_logic
from logger import LOGGER


@timing_and_size
@measure_memory_usage
def uni_rank_stg():
    CONFIG = get_config()
    process = DataProcessor()
    process.extract_data(source=CONFIG['universityRank']['stg']['format'],
                         path=CONFIG['universityRank']['stg']['path'])
    LOGGER.info(f"Transforming data {process}")
    process.spec_transform(spec_logic)
    process.transform_data()
    process.load_data(destination=CONFIG['universityRank']['dm']['path'],
                      part_cols=['Location', ])
    return process.__repr__()


if __name__ == "__main__":
    LOGGER.info("Starting process")
    uni_rank_stg()
