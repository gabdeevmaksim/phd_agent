import unittest
import os
import sys
from unittest.mock import patch, MagicMock

# Add src to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'src'))

from ads_parser import get_ads_headers
from wordcloud_utils import clean_text, get_default_stopwords as wc_get_default_stopwords


class TestADSParser(unittest.TestCase):
    """Test ADS parser functionality."""
    
    @patch('ads_parser.ADS_API_TOKEN', 'test_token')
    def test_get_ads_headers_with_token(self):
        """Test that headers are properly formatted when token is available."""
        headers = get_ads_headers()
        self.assertEqual(headers, {"Authorization": "Bearer test_token"})
    
    @patch('ads_parser.ADS_API_TOKEN', None)
    def test_get_ads_headers_without_token(self):
        """Test that ValueError is raised when token is missing."""
        with self.assertRaises(ValueError):
            get_ads_headers()


class TestWordcloudUtils(unittest.TestCase):
    """Test wordcloud utility functions."""
    
    def test_clean_text_basic(self):
        """Test basic text cleaning functionality."""
        text = "This is a test with <html>tags</html> and {latex} markup."
        cleaned = clean_text(text)
        # Should remove HTML and LaTeX, filter stopwords, keep meaningful words
        self.assertNotIn('html', cleaned)
        self.assertNotIn('latex', cleaned)
        self.assertIn('test', cleaned)
    
    def test_clean_text_custom_stopwords(self):
        """Test text cleaning with custom stopwords."""
        text = "astronomy stellar binary test"
        custom_stopwords = {'astronomy', 'stellar'}
        cleaned = clean_text(text, custom_stopwords=custom_stopwords)
        self.assertNotIn('astronomy', cleaned)
        self.assertNotIn('stellar', cleaned)
        self.assertIn('binary', cleaned)
        self.assertIn('test', cleaned)
    
    def test_clean_text_min_length(self):
        """Test minimum word length filtering."""
        text = "a bb ccc dddd"
        cleaned = clean_text(text, min_word_length=4)
        self.assertNotIn('ccc', cleaned)
        self.assertIn('dddd', cleaned)
    
    def test_get_default_stopwords(self):
        """Test that default stopwords are returned as a set."""
        stopwords = wc_get_default_stopwords()
        self.assertIsInstance(stopwords, set)
        self.assertIn('the', stopwords)
        self.assertIn('analysis', stopwords)
        self.assertTrue(len(stopwords) > 50)  # Should have substantial list


class TestIntegration(unittest.TestCase):
    """Integration tests for the package."""
    
    def test_modules_import_successfully(self):
        """Test that all main modules can be imported."""
        try:
            import ads_parser
            import wordcloud_utils
        except ImportError as e:
            self.fail(f"Failed to import modules: {e}")


if __name__ == '__main__':
    unittest.main()