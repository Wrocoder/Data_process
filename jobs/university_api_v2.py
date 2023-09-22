import asyncio
import os

import httpx
import pandas as pd

from helper.job_helper.decors import timing_and_size
from helper.job_helper.job_helpers import get_config, to_csv_and_load, add_ts_col_to_df, get_countries
from logger import LOGGER

pd.options.display.max_colwidth = 500
pd.options.display.max_columns = 10
pd.options.display.max_rows = 200
pd.options.display.width = 2000


async def fetch_data_from_api(url):
    async with httpx.AsyncClient() as client:
        try:
            response = await client.get(url)
            return pd.DataFrame.from_dict(response.json())
        except httpx.RequestError as e:
            print(f"Request error occurred: {e}")
        except httpx.HTTPError as e:
            print(f"HTTP error occurred: {e}")
        except Exception as e:
            print(f"An error occurred: {e}")


@timing_and_size
async def uni_main():
    countries = get_countries()
    CONFIG = get_config()
    main_url = CONFIG['universityData']['initial']['url']
    as_resp = [fetch_data_from_api(f"{main_url}{country}") for code, country in countries.items()]
    results = await asyncio.gather(*as_resp)
    df = pd.concat([item for item in results], ignore_index=True)
    name = CONFIG['universityData']['name']
    home_path = f"{os.environ['HOME']}{CONFIG['universityData']['initial']['loadPath']}{name}/{name}.csv"
    LOGGER.info(f'Row count: {df.count()[0]}. \n Loading data to: {home_path}')
    to_csv_and_load(add_ts_col_to_df(df), home_path)


if __name__ == "__main__":
    LOGGER.info("Starting process")
    asyncio.run(uni_main())
