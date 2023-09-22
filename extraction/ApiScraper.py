import pandas as pd
import requests
from pandas import DataFrame

from logger import LOGGER


class APIScraper:
    """A class for scraping data from an API.

    Attributes
    ----------
    - url (str): The URL of the API.
    - df (pd.DataFrame): The resulting DataFrame.
    - name (str): The name of the scraper.
    """

    def __init__(self, url: str, name: str, params=None) -> None:
        """Initializes an APIScraper instance.

        Args:
        ----
        - url (str): The URL of the API.
        - name (str): The name of the scraper.
        """
        self.url = url
        self.params = params
        self.df = None
        self.name = name

    def __repr__(self) -> str:
        """Returns a string representation of the APIScraper instance.

        Returns
        -------
        str: A string representation of the instance.
        """
        return f'(\'{self.name}\')'

    def extract_via_api(self, **kwargs):
        """Extracts data from the API.

        Returns
        -------
        requests.Response: The API response object.
        """
        try:
            return requests.get(self.url, params=kwargs['params'])
        except requests.exceptions.Timeout as e:
            # Maybe set up for a retry, or continue in a retry loop
            LOGGER.error('Timeout Exception')
            raise SystemExit(e)
        except requests.exceptions.TooManyRedirects as e:
            # Tell the user their URL was bad and try a different one
            LOGGER.error('Too many redirects')
            raise SystemExit(e)
        except requests.exceptions.RequestException as e:
            LOGGER.fatal(f'Exception: {e}')
            # catastrophic error.
            raise SystemExit(e)

    def run(self) -> DataFrame:
        """Runs the scraper to extract data from the API.

        Returns
        -------
        pd.DataFrame: The resulting DataFrame.
        """
        article_text = self.extract_via_api(params=self.params)
        self.df = pd.DataFrame.from_dict(article_text.json())
        return self.df
