# ADS NASA Parser

A Python script to query the ADS API and download PDFs of research papers.

## Requirements

- Python 3.x
- `requests` library
- `python-dotenv` library
- `pandas` library
- `wordcloud` library (for word cloud generation)
- `matplotlib` library (for visualizations)

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/ads_nasa_parser.git
   cd ads_nasa_parser
   ```

2. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Create a `.env` file in the root directory with your ADS API token:
   ```
   ADS_API_TOKEN=your_ads_api_token_here
   ```

## Usage

### Test Connection
To test the ADS API connection, run:
```bash
python -c "from src.ads_parser import test_ads_connection; test_ads_connection()"
```

### Test Paper Information Retrieval
To test retrieving paper information and abstracts using a bibcode:
```bash
python -c "from src.ads_parser import get_abstract; get_abstract('2020AJ....159..189L')"
```

### Test Abstract Retrieval
To test retrieving only the abstract:
```bash
python -c "from src.ads_parser import get_abstract; abstract = get_abstract('2020AJ....159..189L')"
```

### Bulk Operations
To download abstracts for multiple bibcodes:
```bash
python -c "
from src.ads_parser import get_bulk_paper_info
bibcodes = ['2020AJ....159..189L', '2018NewA...59....8S', '2015AJ....150..117Q']
results = get_bulk_paper_info(bibcodes, batch_size=50)
"
```

### Download Full Catalogue
To download abstracts for an entire catalogue:
```bash
python -c "
from src.ads_parser import download_catalogue_abstracts
download_catalogue_abstracts('data/WUMaCat.csv', 'abstracts.json', batch_size=50)
"
```

### Word Cloud Generation
To generate word clouds and frequency analysis from downloaded abstracts:

#### Generate Word Cloud for Abstracts Only
```bash
python src/abstracts_wordcloud.py
```

#### Generate Word Cloud for Titles Only
```bash
python src/titles_wordcloud.py
```

Both scripts will create:
- Word cloud visualization (PNG image)
- Word frequency data (JSON file with top 100 most frequent words)

Output files are saved in the `wordclouds/` directory.

### Main Application
To run the main application (when implemented), execute:
```bash
python -m src.ads_parser
```

## Features

### Connection Testing
- `test_ads_connection()` - Tests the ADS API connection and token validity

### Paper Information Retrieval
- `get_paper_info(bibcode, show_abstract=True)` - Downloads all information about a paper using its bibcode
- `get_abstract(bibcode)` - Retrieves only the abstract of a paper using its bibcode
- `get_bulk_paper_info(bibcodes, show_abstracts=False, batch_size=50)` - Bulk retrieval for multiple bibcodes
- `download_catalogue_abstracts(csv_file, output_json, batch_size=50)` - Download abstracts for entire catalogues
- Returns comprehensive paper data including title, authors, abstract, journal, etc.

### Bulk Processing Features
- **Batch processing** - Process multiple bibcodes efficiently (50-100 per request)
- **Rate limit monitoring** - Automatic tracking of API usage
- **Duplicate handling** - Automatic removal of duplicate bibcodes
- **Progress tracking** - Real-time progress and sample results
- **JSON export** - Save results to structured JSON files
- **Error resilience** - Continues processing on individual failures

### Word Cloud Analysis Features
- **Text preprocessing** - Automatic cleaning of titles and abstracts (removes HTML, LaTeX, stop words)
- **Word frequency analysis** - Generates top 100 most frequent words with occurrence counts
- **Visual word clouds** - High-quality PNG images with customizable color schemes
- **JSON export** - Machine-readable word frequency data with metadata
- **Statistical insights** - Total words, unique words, and frequency distributions
- **Separate analysis** - Independent processing for titles vs abstracts

### Available Functions

#### ADS API Functions
- `get_ads_headers()` - Helper function to get authorization headers
- `get_paper_info(bibcode, show_abstract=True)` - Main function for paper information retrieval
- `get_abstract(bibcode)` - Dedicated function for abstract retrieval
- `get_bulk_paper_info(bibcodes, show_abstracts=False, batch_size=50)` - Bulk paper information retrieval
- `download_catalogue_abstracts(csv_file_path, output_json_path, batch_size=50)` - Full catalogue processing

#### Word Cloud Analysis Scripts
- `src/abstracts_wordcloud.py` - Generates word clouds and frequency analysis for paper abstracts
- `src/titles_wordcloud.py` - Generates word clouds and frequency analysis for paper titles

Both scripts include functions for:
- Text cleaning and preprocessing
- Word frequency analysis
- Word cloud visualization
- JSON export of frequency data

## Running Tests
To run the unit tests, you can use the following command:
```bash
python -m unittest discover -s tests
```

## Project Structure
- `src/ads_parser.py` - Main application with connection testing, paper retrieval, and bulk processing
- `src/abstracts_wordcloud.py` - Word cloud generator for abstracts with frequency analysis
- `src/titles_wordcloud.py` - Word cloud generator for titles with frequency analysis
- `src/main_old.py` - Original implementation (backup)
- `data/WUMaCat.csv` - Catalogue of 688 binary star systems (424 unique bibcodes)
- `data/wumacat_abstracts.json` - Downloaded abstracts and titles for the entire WUMaCat catalogue
- `data/classification_gaia_deb_new.csv` - Classification data for contact binaries
- `wordclouds/` - Directory containing generated word clouds and frequency data
  - `titles_wordcloud.png` - Word cloud visualization for paper titles
  - `abstracts_wordcloud.png` - Word cloud visualization for paper abstracts
  - `titles_word_frequencies.json` - Top 100 frequent words from titles
  - `abstracts_word_frequencies.json` - Top 100 frequent words from abstracts
- `notebooks/test_ads_parser.ipynb` - Interactive testing notebook for all functions

## API Rate Limits
- **Search endpoint**: 5,000 requests per day (used by our functions)
- **Export endpoint**: 100 requests per day (for citation formatting)
- **Bulk processing**: Efficiently uses ~9 requests for 424 unique bibcodes
- **Rate monitoring**: Automatic tracking via response headers

## Contributing
Contributions are welcome! Please open an issue or submit a pull request for any improvements or bug fixes.

## License
This project is licensed under the MIT License. See the LICENSE file for more details.