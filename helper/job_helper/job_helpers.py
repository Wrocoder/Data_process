import pandas as pd
import yaml


def get_config(path='config/config.yaml', location='local'):
    """
    Reads yaml config.
    :param path: path(key)
    :type path: str
    :param location: where your config.yaml is located
    :type location: str

    :return: yaml config.
    :rtype: dict
    """
    if location == 'local':
        with open(path, 'r') as yaml_file:
            return yaml.safe_load(yaml_file)
    else:
        raise RuntimeError


def to_csv_and_load(df, file_name):
    """
    Loading data to csv format
    :param df: dataframe
    :type df: pandas dataframe
    :param file_name: name of file
    :type file_name: str

    :return: None
    """
    df.to_csv(file_name, index=True)


def add_ts_col_to_df(df):
    df['today_ts'] = pd.Timestamp.today()
    df['today_date'] = pd.to_datetime(df['today_ts'].dt.date)
    return df
