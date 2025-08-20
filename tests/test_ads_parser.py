"""
Tests for ADS parser functionality.
"""

import unittest
import os
import sys
from unittest.mock import patch, MagicMock
import requests

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from ads_parser import (
    test_ads_connection, 
    get_ads_headers, 
    _make_ads_request_with_retry,
    get_paper_info
)


class TestADSConnection(unittest.TestCase):
    """Test ADS API connection functionality."""
    
    @patch('ads_parser.ADS_API_TOKEN', None)
    @patch('ads_parser.requests.get')
    def test_connection_without_token(self, mock_get):
        """Test connection fails gracefully without token."""
        # Should not make any API calls when token is missing
        result = test_ads_connection()
        self.assertFalse(result)
        mock_get.assert_not_called()
    
    @patch('ads_parser.ADS_API_TOKEN', 'test_token')
    @patch('ads_parser.requests.get')
    def test_connection_successful(self, mock_get):
        """Test successful API connection."""
        # Mock successful response
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            'response': {
                'numFound': 100,
                'docs': [{'bibcode': 'test', 'title': ['Test Paper']}]
            }
        }
        mock_get.return_value = mock_response
        
        result = test_ads_connection()
        self.assertTrue(result)
    
    @patch('ads_parser.ADS_API_TOKEN', 'test_token')
    @patch('ads_parser.requests.get')
    def test_connection_api_error(self, mock_get):
        """Test handling of API errors."""
        # Mock failed response
        mock_response = MagicMock()
        mock_response.status_code = 401
        mock_response.text = "Unauthorized"
        mock_get.return_value = mock_response
        
        result = test_ads_connection()
        self.assertFalse(result)


class TestRetryLogic(unittest.TestCase):
    """Test retry functionality."""
    
    @patch('ads_parser.requests.get')
    @patch('ads_parser.time.sleep')  # Mock sleep to speed up tests
    def test_retry_on_rate_limit(self, mock_sleep, mock_get):
        """Test retry logic on rate limit errors."""
        # First call returns 429, second call succeeds
        mock_responses = [
            MagicMock(status_code=429),
            MagicMock(status_code=200)
        ]
        mock_get.side_effect = mock_responses
        
        result = _make_ads_request_with_retry(
            "http://test.com",
            {"Authorization": "Bearer test"},
            {"q": "test"},
            max_retries=1
        )
        
        self.assertEqual(result.status_code, 200)
        self.assertEqual(mock_get.call_count, 2)
        mock_sleep.assert_called_once()
    
    @patch('ads_parser.requests.get')
    def test_max_retries_exceeded(self, mock_get):
        """Test behavior when max retries are exceeded."""
        # All calls return 429
        mock_get.return_value = MagicMock(status_code=429)
        
        result = _make_ads_request_with_retry(
            "http://test.com",
            {"Authorization": "Bearer test"},
            {"q": "test"},
            max_retries=2
        )
        
        self.assertEqual(result.status_code, 429)
        self.assertEqual(mock_get.call_count, 3)  # Initial + 2 retries


if __name__ == '__main__':
    unittest.main()
