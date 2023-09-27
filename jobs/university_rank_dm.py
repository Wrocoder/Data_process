from extraction.DMProcess import DMProcessor

from helper.job_helper.decors import timing_and_size
from helper.job_helper.job_helpers import get_config
from logger import LOGGER


@timing_and_size
def univ_rank_main_dm():
    CONFIG = get_config()
    process = DMProcessor()
    process.extract_data(CONFIG['universityRank']['stg']['path'])
    LOGGER.info(f"Transforming data {process}")
    process.transform_data()
    process.load_data(schema=CONFIG['universityRank']['dm']['schema'],
                      table=CONFIG['universityRank']['dm']['table'])

    return repr(process)


if __name__ == "__main__":
    LOGGER.info("Starting process")
    univ_rank_main_dm()
