import pandas as pd
from pandas.testing import assert_frame_equal
import yaml
import pytest
from unittest.mock import mock_open, patch
from helper.job_helper.job_helpers import get_config, to_csv_and_load, \
    add_ts_col_to_df, get_countries

# Example test data
example_config = {
    'key1': 'value1',
    'key2': 'value2',
}

example_df = pd.DataFrame({
    'column1': [1, 2, 3],
    'column2': ['A', 'B', 'C'],
})


def test_get_config_local():
    # Mock open() to simulate reading the YAML file
    with patch("builtins.open", mock_open(read_data=yaml.dump(example_config))) as m:
        config = get_config()
    m.assert_called_once_with('config/config.yaml')
    assert config == example_config


def test_get_config_remote():
    with pytest.raises(RuntimeError):
        get_config(location='remote')


def test_to_csv_and_load():
    file_name = 'data/test_output.csv'
    to_csv_and_load(example_df, file_name)
    loaded_df = pd.read_csv(file_name)

    assert_frame_equal(loaded_df, example_df)


def test_add_ts_col_to_df():
    df_with_ts = add_ts_col_to_df(example_df)
    assert 'today_ts' in df_with_ts.columns
    assert 'today_date' in df_with_ts.columns


def test_get_countries():
    countries = get_countries()
    assert isinstance(countries, dict)
    assert 'US' in countries
    assert 'CA' in countries
    assert 'GB' in countries
    assert 'IN' in countries
