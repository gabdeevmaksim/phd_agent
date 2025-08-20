# PhD Research Agent - ADS Analysis Toolkit

A comprehensive Python toolkit for PhD research focusing on astronomical literature analysis using NASA's Astrophysics Data System (ADS) API. This project provides automated tools for paper discovery, keyword analysis, and bibliometric research with specialized focus on binary star systems and astronomical surveys.

## Requirements

- Python 3.x
- `requests>=2.25.1` - HTTP library for ADS API communication
- `python-dotenv>=1.0.0` - Environment variable management
- `numpy>=2.3.0` - Numerical computing foundation
- `pandas>=2.3.0` - Data manipulation and analysis
- `pytest>=6.2.4` - Testing framework
- `wordcloud>=1.9.0` - Word cloud generation
- `matplotlib>=3.7.0` - Plotting and visualization
- `jupyter` - Interactive notebook environment (for research analysis)

## Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/phd_agent.git
   cd phd_agent
   ```

2. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Install Jupyter for interactive analysis:
   ```bash
   pip install jupyter
   ```

4. Create a `.env` file in the root directory with your ADS API token:
   ```
   ADS_API_TOKEN=your_ads_api_token_here
   ```

   You can obtain an ADS API token by registering at: https://ui.adsabs.harvard.edu/user/settings/token

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
To generate word clouds and frequency analysis from downloaded abstracts using the utility functions:

```python
from src.wordcloud_utils import generate_wordcloud_from_json, load_data

# Generate word cloud from processed data
generate_wordcloud_from_json('data/wumacat_abstracts.json', 'wordclouds/')
```

The word cloud utilities provide:
- Text preprocessing and cleaning
- Word frequency analysis
- Word cloud visualization
- JSON export of frequency data with metadata

Output files are saved in the `wordclouds/` directory.

## Interactive Analysis Notebooks

The project includes several Jupyter notebooks for interactive research and analysis:

### 1. ADS Query Analysis (`notebooks/ads_query_analysis.ipynb`)
- **Purpose**: Advanced search strategy optimization and query performance analysis
- **Features**:
  - Comparative analysis of different search approaches
  - Query efficiency and coverage metrics
  - Systematic evaluation of keyword combinations
  - Search strategy recommendations

### 2. Keyword Experiment (`notebooks/keyword_experiment.ipynb`)
- **Purpose**: Systematic discovery and evaluation of effective research keywords
- **Features**:
  - Automated keyword extraction from literature
  - Coverage analysis across different keyword sets
  - Overlap detection between search strategies
  - Statistical evaluation of keyword effectiveness
  - Generates comprehensive experiment results and visualizations

### 3. Test ADS Parser (`notebooks/test_ads_parser.ipynb`)
- **Purpose**: Interactive testing and development environment for ADS API functions
- **Features**:
  - Live testing of API connections and queries
  - Development and debugging of new features
  - Performance benchmarking
  - API response analysis

### 4. Wordcloud Analysis (`notebooks/wordcloud_analysis.ipynb`)
- **Purpose**: Text mining and visualization of research literature
- **Features**:
  - Automated text preprocessing and cleaning
  - Word frequency analysis across different datasets
  - Comparative word cloud generation
  - Trend analysis in research terminology

### 5. Exact Keyword Experiment (`notebooks/exact_keyword_experiment.ipynb`)
- **Purpose**: Precise keyword matching and exact phrase analysis
- **Features**:
  - Exact keyword matching without stemming or variations
  - Phrase-based search optimization
  - Precision-focused literature discovery
  - Comparison with fuzzy matching approaches

### 6. Similarity Experiments (`notebooks/similarity_experiments.ipynb`)
- **Purpose**: Advanced similarity analysis and clustering of research papers
- **Features**:
  - Text similarity calculations between papers
  - Clustering analysis of research themes
  - Semantic similarity metrics
  - Research trend identification through similarity networks

### Running Notebooks
To start the Jupyter environment:
```bash
pip install jupyter
jupyter notebook notebooks/
```

### Main Application
To run the main application (when implemented), execute:
```bash
python -m src.ads_parser
```

## Features

### Core ADS API Interface
- **Connection Testing**: `test_ads_connection()` - Validates ADS API connectivity and token authentication
- **Paper Retrieval**: Comprehensive paper information extraction using bibcodes
- **Bulk Processing**: Efficient batch processing of large datasets (50-100 bibcodes per request)
- **Rate Limit Management**: Automatic API usage tracking and optimization
- **Error Handling**: Robust error recovery and duplicate detection

### Advanced Research Analysis
- **Keyword Discovery**: Systematic identification and evaluation of effective research keywords
- **Search Strategy Optimization**: Comparative analysis of different search approaches with performance metrics
- **Literature Coverage Analysis**: Statistical evaluation of keyword effectiveness and overlap detection
- **Bibliometric Research**: Comprehensive analysis of research trends and patterns
- **Similarity Analysis**: Advanced clustering and similarity metrics for research paper analysis
- **Exact Matching**: Precision-focused keyword matching for specialized research queries

### Text Mining & Visualization
- **Word Cloud Generation**: Advanced text preprocessing and visualization with customizable styling
- **Frequency Analysis**: Statistical analysis of word patterns across titles and abstracts
- **Text Preprocessing**: Automatic cleaning (HTML, LaTeX, stop words removal)
- **Comparative Analysis**: Side-by-side analysis of different datasets and time periods
- **Export Capabilities**: Machine-readable JSON output with comprehensive metadata

### Interactive Research Environment
- **Jupyter Integration**: Full notebook-based research workflow
- **Real-time Analysis**: Live testing and development environment
- **Performance Benchmarking**: API response time and efficiency analysis
- **Visualization Tools**: Comprehensive plotting and chart generation
- **Experiment Tracking**: Systematic recording and analysis of research experiments

### Data Management
- **Structured Storage**: Organized JSON and CSV output formats
- **Metadata Preservation**: Complete paper metadata including authors, journals, dates
- **Duplicate Handling**: Automatic detection and removal of duplicate entries
- **Progress Tracking**: Real-time progress indicators and sample results
- **Backup & Recovery**: Robust data persistence and error recovery

### Available Functions

#### ADS API Functions (`src/ads_parser.py`)
- `test_ads_connection()` - Tests ADS API connectivity and token validation
- `get_ads_headers()` - Helper function to get authorization headers
- `get_paper_info(bibcode, show_abstract=True)` - Main function for paper information retrieval
- `get_abstract(bibcode)` - Dedicated function for abstract retrieval
- `get_bulk_paper_info(bibcodes, show_abstracts=False, batch_size=50)` - Bulk paper information retrieval
- `download_catalogue_abstracts(csv_file_path, output_json_path, batch_size=50)` - Full catalogue processing

#### Word Cloud Utilities (`src/wordcloud_utils.py`)
- `extract_top_words_from_json_files()` - Extract top N words from multiple JSON frequency files
- `load_data()` - Load and parse JSON data files with error handling
- `preprocess_text()` - Clean and preprocess text data for analysis
- `generate_word_frequencies()` - Calculate word frequency distributions
- `create_wordcloud()` - Generate customized word cloud visualizations
- `save_frequency_data()` - Export frequency analysis to JSON format

#### Research Analysis Functions
The notebooks contain specialized functions for:
- Keyword extraction and evaluation
- Search strategy optimization
- Coverage analysis and overlap detection
- Performance benchmarking and visualization
- Statistical analysis of research trends

## Research Data & Outputs

### Core Datasets
- **WUMaCat.csv**: W UMa contact binary star catalogue containing 688 binary star systems with 424 unique ADS bibcodes
- **classification_gaia_deb_new.csv**: Gaia-based classification data for detached eclipsing binary stars
- **wumacat_abstracts.json**: Complete abstracts and metadata for the entire WUMaCat catalogue (downloaded via ADS API)

### Extended Research Data
- **comprehensive_keyword_experiment_results.json**: Extended keyword discovery experiments with detailed analysis results

### Experiment Results
- **keyword_experiment_results.json**: Comprehensive results from systematic keyword discovery experiments
- **keyword_experiment_results.png**: Visualization showing keyword effectiveness and coverage analysis
- **keyword_experiment_summary.csv**: Statistical summary of keyword coverage experiments with metrics like overlap percentage and new discoveries
- **search_strategy_summary.json**: Optimized search strategies and their performance benchmarks
- **bibcode_comparison.json**: Comparative analysis of bibcode overlaps across different search methodologies

### Analysis Outputs
- **Word Cloud Visualizations** (`wordclouds/` directory):
  - High-resolution PNG images for titles and abstracts
  - JSON frequency data with statistical metadata
  - Top 100 most frequent terms with occurrence counts
- **Research Insights**: Quantitative analysis of literature trends, keyword effectiveness, and search optimization

### Key Research Findings
From `keyword_experiment_summary.csv`:
- **2-keyword searches**: 67,312 total papers, 87.7% coverage of known catalogue
- **3-keyword searches**: 12,660 papers, 87.5% coverage with improved precision  
- **4-keyword searches**: 9,425 papers, 86.3% coverage with highest precision
- **Optimal strategy**: 3-4 keyword combinations provide best balance of coverage and precision

## Running Tests
To run the unit tests, you can use the following command:
```bash
python -m unittest discover -s tests
```

## Project Structure

### Core Source Code
- `src/ads_parser.py` - Main ADS API interface with connection testing, paper retrieval, and bulk processing capabilities
- `src/wordcloud_utils.py` - Comprehensive word cloud generation and text analysis utilities
- `src/__init__.py` - Package initialization

### Interactive Notebooks
- `notebooks/ads_query_analysis.ipynb` - Advanced ADS query analysis and search strategy optimization
- `notebooks/keyword_experiment.ipynb` - Systematic keyword discovery and literature coverage experiments
- `notebooks/test_ads_parser.ipynb` - Interactive testing and development notebook for API functions
- `notebooks/wordcloud_analysis.ipynb` - Word cloud generation and text mining analysis
- `notebooks/exact_keyword_experiment.ipynb` - Precise keyword matching and exact phrase analysis
- `notebooks/similarity_experiments.ipynb` - Advanced similarity analysis and clustering of research papers

### Data Directory
- `data/WUMaCat.csv` - W UMa contact binary star catalogue (688 systems, 424 unique bibcodes)
- `data/wumacat_abstracts.json` - Complete abstracts and metadata for the WUMaCat catalogue
- `data/classification_gaia_deb_new.csv` - Gaia-based classification data for detached eclipsing binaries
- `data/keyword_experiment_results.json` - Results from systematic keyword discovery experiments
- `data/keyword_experiment_results.png` - Visualization of keyword experiment outcomes
- `data/keyword_experiment_summary.csv` - Summary statistics of keyword coverage analysis
- `data/search_strategy_summary.json` - Optimized search strategies and performance metrics
- `data/bibcode_comparison.json` - Comparative analysis of bibcode overlaps across search strategies
- `data/comprehensive_keyword_experiment_results.json` - Comprehensive results from extended keyword experiments

### Generated Outputs
- `wordclouds/` - Directory containing word cloud visualizations and frequency analysis
  - `titles_wordcloud.png` - Word cloud visualization for paper titles
  - `abstracts_wordcloud.png` - Word cloud visualization for paper abstracts  
  - `titles_word_frequencies.json` - Top 100 frequent words from titles with metadata
  - `abstracts_word_frequencies.json` - Top 100 frequent words from abstracts with metadata

### Configuration & Testing
- `tests/` - Unit tests and validation scripts
- `requirements.txt` - Python dependencies with version specifications
- `setup.py` - Package installation configuration
- `.env` - Environment variables (ADS API token) - *create this file*

## Research Applications

This toolkit is specifically designed for PhD-level astronomical research with focus on:

### Binary Star Systems Research
- Systematic literature surveys of W UMa contact binary systems
- Cross-correlation with Gaia classification data
- Bibliometric analysis of research trends in binary star studies

### Search Strategy Optimization
- Automated keyword discovery from existing literature
- Statistical evaluation of search effectiveness
- Optimization of literature review coverage vs precision

### Text Mining Applications
- Analysis of research terminology evolution
- Identification of emerging research areas
- Comparative studies across different astronomical domains

## API Rate Limits & Best Practices
- **Search endpoint**: 5,000 requests per day (used by our functions)
- **Export endpoint**: 100 requests per day (for citation formatting)
- **Bulk processing**: Efficiently uses ~9 requests for 424 unique bibcodes
- **Rate monitoring**: Automatic tracking via response headers
- **Best practice**: Use batch processing for large datasets to minimize API calls

## Academic Use & Citation

If you use this toolkit in your research, please cite appropriately. This project demonstrates automated literature analysis techniques applicable to astronomical research and bibliometric studies.

## Contributing

This is a research project developed for PhD studies. Contributions that enhance the research capabilities, add new analysis methods, or improve the astronomical focus are welcome. Please open an issue to discuss potential contributions.

## License

This project is licensed under the MIT License. See the LICENSE file for more details.