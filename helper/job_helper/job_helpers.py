import pandas as pd
import yaml
from country_list import countries_for_language


def get_config(path='config/config.yaml', location='local') -> dict:
    """Reads yaml config.
    :param path: path(key)
    :type path: str
    :param location: where your config.yaml is located
    :type location: str.

    :return: yaml config.
    :rtype: dict
    """
    if location == 'local':
        with open(path) as yaml_file:
            return yaml.safe_load(yaml_file)
    else:
        raise RuntimeError


def to_csv_and_load(df: pd.DataFrame, file_name: str) -> None:
    """Loading data to csv format.
    :param df: dataframe
    :type df: pandas dataframe
    :param file_name: name of file
    :type file_name: str.

    :return: None
    """
    df.to_csv(file_name, index=False)


def add_ts_col_to_df(df: pd.DataFrame) -> pd.DataFrame:
    """Add timestamp columns to a DataFrame.

    This function adds two new columns to the input DataFrame:
    - 'today_ts': Contains the current timestamp.
    - 'today_date': Contains the current date extracted from the timestamp.

    Args:
    ----
    - df (pd.DataFrame): The input DataFrame to which the columns will be added.

    Returns:
    -------
    pd.DataFrame: The modified DataFrame with the added columns.
    """
    df['today_ts'] = pd.Timestamp.today()
    df['today_date'] = pd.to_datetime(df['today_ts'].dt.date)
    return df


def get_countries() -> dict:
    """Retrieves a dictionary of countries.

    Returns
    -------
    dict: A dictionary of countries.
    """
    return dict(countries_for_language('en'))


def get_db_creds():
    """
    Get database credentials from the configuration.

    Returns:
    tuple: A tuple containing the database URL and properties (user, password, driver).
    """
    CONFIG = get_config()
    url = CONFIG['db']['url']

    properties = {
        "user": CONFIG['db']['user'],
        "password": CONFIG['db']['pass'],
        "driver": CONFIG['db']['driver']
    }

    return url, properties

