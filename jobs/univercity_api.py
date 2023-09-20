import os

import pandas as pd

from extraction.ApiScraper import APIScraper
from helper.job_helper.decors import timing_and_size
from helper.job_helper.job_helpers import get_config, to_csv_and_load, add_ts_col_to_df
from logger import LOGGER

pd.options.display.max_colwidth = 500
pd.options.display.max_columns = 10
pd.options.display.max_rows = 200
pd.options.display.width = 2000


@timing_and_size
def uni_main():
    countries = APIScraper.get_countries()
    CONFIG = get_config()
    LOGGER.info(CONFIG)
    df = pd.DataFrame()

    for code, country in countries.items():
        url = f"{CONFIG['universityData']['initial']['url']}{country}"
        pipeline = APIScraper(url, CONFIG['universityData']['name'])
        LOGGER.info(f'Processing {pipeline.__repr__()} data for {country}')
        result_df = pipeline.run()
        df = pd.concat([df, result_df], ignore_index=True)

    name = CONFIG['universityData']['name']
    home_path = f"{os.environ['HOME']}{CONFIG['universityData']['initial']['loadPath']}{name}/{name}.csv"
    LOGGER.info(f'Loading data to: {home_path}')
    LOGGER.info(f'Count rows in dataframe: {df.count()[0]}')
    to_csv_and_load(add_ts_col_to_df(df), home_path)


if __name__ == "__main__":
    LOGGER.info("Starting process")
    uni_main()
