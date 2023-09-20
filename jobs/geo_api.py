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
def geo_main():
    CONFIG = get_config()
    url = CONFIG['geoData']['initial']['url']
    params = {'access_key': CONFIG['geoData']['initial']['access_key']}
    pipeline = APIScraper(url, CONFIG['geoData']['name'], params)
    LOGGER.info(f'Processing {pipeline.__repr__()}')
    df = pipeline.run()
    name = CONFIG['geoData']['name']
    home_path = f"{os.environ['HOME']}{CONFIG['geoData']['initial']['loadPath']}{name}/{name}.csv"
    LOGGER.info(f'Loading data to: {home_path}')
    LOGGER.info(f'Count rows in dataframe: {df.count()[0]}')
    to_csv_and_load(add_ts_col_to_df(df), home_path)


if __name__ == "__main__":
    LOGGER.info("Starting process")
    geo_main()
