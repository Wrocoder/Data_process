import pandas as pd
import requests
from country_list import countries_for_language

from logger import LOGGER


class APIScraper:
    """
    A class for scraping data from an API.

    Attributes:
    - url (str): The URL of the API.
    - df (pd.DataFrame): The resulting DataFrame.
    - name (str): The name of the scraper.
    """

    def __init__(self, url, name, params=None):
        """
        Initializes an APIScraper instance.

        Args:
        - url (str): The URL of the API.
        - name (str): The name of the scraper.
        """
        self.url = url
        self.params = params
        self.df = None
        self.name = name

    def __repr__(self):
        """
        Returns a string representation of the APIScraper instance.

        Returns:
        str: A string representation of the instance.
        """
        return f'(\'{self.name}\')'

    def extract_via_api(self, **kwargs):
        """
        Extracts data from the API.

        Returns:
        requests.Response: The API response object.
        """
        try:
            return requests.get(self.url, params=kwargs['params'])
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
        """
        Retrieves a dictionary of countries.

        Returns:
        dict: A dictionary of countries.
        """
        return dict(countries_for_language('en'))

    def run(self):
        """
        Runs the scraper to extract data from the API.

        Returns:
        pd.DataFrame: The resulting DataFrame.
        """
        article_text = self.extract_via_api(params=self.params)
        self.df = pd.DataFrame.from_dict(article_text.json())
        return self.df
