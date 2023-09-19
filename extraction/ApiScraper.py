import pandas as pd
import requests
from country_list import countries_for_language

from helper.job_helper.decors import count_calls, timing_and_size
from logger import LOGGER


class APIScraper:
    def __init__(self, url, name):
        self.url = url
        self.df = None
        self.name = name

    def __repr__(self):
        return f'(\'{self.name}\')'

    def extract_via_api(self):
        try:
            return requests.get(self.url)
        except requests.exceptions.Timeout:
            # Maybe set up for a retry, or continue in a retry loop
            LOGGER.error('Timeout Exception')
        except requests.exceptions.TooManyRedirects:
            # Tell the user their URL was bad and try a different one
            LOGGER.error('Too many redirects')
        except requests.exceptions.RequestException as e:
            LOGGER.fatal(f'Exception: {e}')
            # catastrophic error. bail.
            raise SystemExit(e)

    @staticmethod
    def get_countries():
        return dict(countries_for_language('en'))

    @count_calls
    @timing_and_size
    def run(self):
        article_text = self.extract_via_api()
        self.df = pd.DataFrame.from_dict(article_text.json())
        return self.df
