import unittest
from unittest.mock import Mock, patch

import pandas as pd
import requests

from extraction import ApiProcess


class TestAPIScraper(unittest.TestCase):
    def setUp(self):
        self.url = 'https://example.com/api'
        self.name = 'Test Scraper'
        self.params = {'param1': 'value1', 'param2': 'value2'}
        self.scraper = ApiProcess.APIProcessor(self.url, self.name, self.params)

    def test_init(self):
        self.assertEqual(self.scraper.url, self.url)
        self.assertEqual(self.scraper.name, self.name)
        self.assertEqual(self.scraper.params, self.params)
        self.assertIsNone(self.scraper.df)

    @patch('requests.get')
    def test_extract_via_api_successful(self, mock_requests_get):
        mock_response = Mock()
        mock_response.json.return_value = {'data': [1, 2, 3]}
        mock_requests_get.return_value = mock_response

        response = self.scraper.extract_via_api(params=self.params)

        mock_requests_get.assert_called_once_with(self.url, params=self.params)
        self.assertEqual(response.json(), {'data': [1, 2, 3]})

    @patch('requests.get', side_effect=requests.exceptions.Timeout)
    def test_extract_via_api_timeout_exception(self, mock_requests_get):
        with self.assertRaises(SystemExit):
            self.scraper.extract_via_api(params=self.params)

    @patch('requests.get', side_effect=requests.exceptions.TooManyRedirects)
    def test_extract_via_api_redirects_exception(self, mock_requests_get):
        with self.assertRaises(SystemExit):
            self.scraper.extract_via_api(params=self.params)

    @patch('requests.get', side_effect=requests.exceptions.RequestException)
    def test_extract_via_api_request_exception(self, mock_requests_get):
        with self.assertRaises(SystemExit):
            self.scraper.extract_via_api(params=self.params)

    @patch('requests.get')
    def test_run(self, mock_requests_get):
        mock_response = Mock()
        mock_response.json.return_value = {'data': [1, 2, 3]}
        mock_requests_get.return_value = mock_response

        result_df = self.scraper.run()

        mock_requests_get.assert_called_once_with(self.url, params=self.params)
        expected_df = pd.DataFrame({'data': [1, 2, 3]})
        pd.testing.assert_frame_equal(result_df, expected_df)


if __name__ == '__main__':
    unittest.main()
