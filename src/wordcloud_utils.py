"""
Wordcloud utility functions for processing word frequency data
"""

import json
import re
import matplotlib.pyplot as plt
from wordcloud import WordCloud
from collections import Counter
import os
from typing import Dict, List, Optional, Set, Tuple, Union
from datetime import datetime


def extract_top_words_from_json_files(titles_file: str, abstracts_file: str, 
                                     n_words: int) -> List[str]:
    """
    Extract top N words from both JSON files and return a comprehensive list without duplicates.
    
    Args:
        titles_file: Path to titles word frequencies JSON file
        abstracts_file: Path to abstracts word frequencies JSON file
        n_words: Number of top words to extract from each file
        
    Returns:
        Comprehensive list of unique words
    """
    # Load titles data
    with open(titles_file, 'r') as f:
        titles_data = json.load(f)
    
    # Load abstracts data
    with open(abstracts_file, 'r') as f:
        abstracts_data = json.load(f)
    
    # Get top N words from each file
    titles_freq = titles_data["word_frequencies"]
    abstracts_freq = abstracts_data["word_frequencies"]
    
    titles_top = sorted(titles_freq.items(), key=lambda x: x[1], reverse=True)[:n_words]
    abstracts_top = sorted(abstracts_freq.items(), key=lambda x: x[1], reverse=True)[:n_words]
    
    # Extract just the words
    titles_words = [word for word, freq in titles_top]
    abstracts_words = [word for word, freq in abstracts_top]
    
    # Combine and remove duplicates
    all_words = list(set(titles_words + abstracts_words))
    
    return all_words


def load_wumacat_bibcodes(csv_path: str = "data/WUMaCat.csv") -> Set[str]:
    """
    Load WUMaCat bibcodes from CSV file.
    
    Args:
        csv_path: Path to WUMaCat CSV file
        
    Returns:
        Set of unique bibcodes
    """
    import csv
    
    bibcodes = set()
    try:
        with open(csv_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if 'Bibcode' in row and row['Bibcode'].strip():
                    bibcodes.add(row['Bibcode'].strip())
        
        print(f"✅ Loaded {len(bibcodes)} unique bibcodes from WUMaCat")
        return bibcodes
        
    except FileNotFoundError:
        print(f"❌ WUMaCat file not found: {csv_path}")
        return set()
    except Exception as e:
        print(f"❌ Error loading WUMaCat bibcodes: {e}")
        return set()


def get_top_keywords_from_wordclouds(titles_freq_file: str, abstracts_freq_file: str, 
                                   top_n: int = 20, exclude_generic: bool = True) -> List[str]:
    """
    Extract top keywords from wordcloud frequency files, excluding generic terms.
    
    Args:
        titles_freq_file: Path to titles frequency JSON file
        abstracts_freq_file: Path to abstracts frequency JSON file  
        top_n: Number of top keywords to extract
        exclude_generic: Whether to exclude generic research terms
        
    Returns:
        List of top keywords suitable for exact searching
    """
    try:
        # Load frequency data
        with open(titles_freq_file, 'r') as f:
            titles_data = json.load(f)
        with open(abstracts_freq_file, 'r') as f:
            abstracts_data = json.load(f)
        
        # Combine frequencies from both sources
        combined_freq = {}
        
        for word, freq in titles_data.get("word_frequencies", {}).items():
            combined_freq[word] = combined_freq.get(word, 0) + freq * 2  # Weight titles higher
            
        for word, freq in abstracts_data.get("word_frequencies", {}).items():
            combined_freq[word] = combined_freq.get(word, 0) + freq
        
        # Generic terms to exclude from exact searching
        generic_terms = {
            'system', 'systems', 'object', 'objects', 'source', 'sources', 'method', 'methods',
            'observation', 'observations', 'measurement', 'measurements', 'detection', 'detections',
            'model', 'models', 'simulation', 'simulations', 'technique', 'techniques', 'approach',
            'approaches', 'investigation', 'investigations', 'research', 'work', 'survey', 'surveys',
            'catalog', 'catalogue', 'database', 'sample', 'samples', 'population', 'populations',
            'distribution', 'distributions', 'properties', 'characteristics', 'parameters',
            'values', 'data', 'dataset', 'datasets', 'analysis', 'analyses', 'statistics'
        } if exclude_generic else set()
        
        # Sort by frequency and filter
        sorted_words = sorted(combined_freq.items(), key=lambda x: x[1], reverse=True)
        
        # Extract top keywords, excluding generic terms
        keywords = []
        for word, freq in sorted_words:
            if len(keywords) >= top_n:
                break
            if word not in generic_terms and len(word) > 3:  # Exclude very short words
                keywords.append(word)
        
        print(f"✅ Extracted top {len(keywords)} keywords for exact searching")
        return keywords
        
    except Exception as e:
        print(f"❌ Error extracting keywords: {e}")
        return []


def save_experiment_results(results: Dict, output_file: str) -> None:
    """
    Save experiment results to JSON file with metadata.
    
    Args:
        results: Experiment results dictionary
        output_file: Path to save results
    """
    try:
        # Add metadata
        results_with_metadata = {
            "experiment_type": "exact_keyword_search",
            "timestamp": datetime.now().isoformat(),
            "metadata": {
                "description": "Exact keyword search experiment comparing different ADS fields",
                "search_method": "exact field matching using =field:\"keyword\" syntax"
            },
            "results": results
        }
        
        with open(output_file, 'w', encoding='utf-8') as f:
            json.dump(results_with_metadata, f, indent=2, ensure_ascii=False)
        
        print(f"✅ Experiment results saved to: {output_file}")
        
    except Exception as e:
        print(f"❌ Error saving results: {e}")


def load_data(json_file_path: str) -> Dict:
    """Load the JSON data from the file."""
    with open(json_file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
        return data.get('papers', {})


def get_default_stopwords() -> Set[str]:
    """Get default stopwords for astronomical literature."""
    return {
        'the', 'and', 'of', 'in', 'to', 'a', 'is', 'are', 'for', 'with', 'by', 'on', 'as', 
        'at', 'be', 'or', 'an', 'we', 'it', 'that', 'from', 'this', 'these', 'have', 'has',
        'was', 'were', 'been', 'their', 'they', 'them', 'than', 'more', 'can', 'will', 'would',
        'could', 'should', 'may', 'might', 'must', 'shall', 'do', 'does', 'did', 'had', 'which',
        'who', 'what', 'where', 'when', 'why', 'how', 'all', 'any', 'each', 'every', 'some',
        'many', 'much', 'most', 'other', 'such', 'only', 'own', 'same', 'so', 'also', 'just',
        'now', 'here', 'there', 'then', 'very', 'well', 'still', 'even', 'back', 'through',
        'about', 'into', 'over', 'after', 'up', 'out', 'if', 'no', 'not', 'new', 'our', 'but',
        'first', 'last', 'two', 'three', 'one', 'year', 'years', 'time', 'during', 'within',
        'between', 'under', 'above', 'below', 'found', 'using', 'used', 'based', 'obtained',
        'observed', 'presented', 'show', 'shows', 'shown', 'present', 'presents', 'analysis',
        'study', 'studies', 'paper', 'data', 'results', 'result', 'suggest', 'suggests',
        'indicate', 'indicates', 'determine', 'determined', 'calculate', 'calculated',
        'measure', 'measured', 'estimate', 'estimated', 'derive', 'derived', 'find', 'finds'
    }

def clean_text(text: str, custom_stopwords: Optional[Set[str]] = None, 
               min_word_length: int = 3, remove_numbers: bool = True,
               remove_latex: bool = True) -> str:
    """
    Clean and preprocess text for word cloud generation.
    
    Args:
        text: Input text to clean
        custom_stopwords: Custom set of stopwords (uses default if None)
        min_word_length: Minimum word length to keep
        remove_numbers: Whether to remove numerical content
        remove_latex: Whether to remove LaTeX markup
        
    Returns:
        Cleaned text string
    """
    if not text:
        return ""
    
    stopwords = custom_stopwords if custom_stopwords is not None else get_default_stopwords()
    
    # Remove HTML tags
    text = re.sub(r'<[^>]+>', '', text)
    
    if remove_latex:
        text = re.sub(r'{[^}]*}', '', text)  # Remove LaTeX-style markup
        text = re.sub(r'\\[a-zA-Z]+', '', text)  # Remove LaTeX commands
    
    # Convert to lowercase and remove punctuation
    if remove_numbers:
        text = re.sub(r'[^a-zA-Z\s]', ' ', text.lower())
    else:
        text = re.sub(r'[^\w\s]', ' ', text.lower())
    
    # Split into words and filter
    words = [word.strip() for word in text.split() 
            if len(word.strip()) >= min_word_length and word.strip() not in stopwords]
    
    return ' '.join(words)


def extract_abstracts(papers_data):
    """Extract and clean all abstracts from the papers."""
    abstracts = []
    for paper_id, paper_data in papers_data.items():
        abstract = paper_data.get('abstract', '')
        if abstract:
            abstracts.append(abstract)
    
    combined_abstracts = ' '.join(abstracts)
    cleaned_abstracts = clean_text(combined_abstracts)
    return cleaned_abstracts


def extract_titles(papers_data):
    """Extract and clean all titles from the papers."""
    titles = []
    for paper_id, paper_data in papers_data.items():
        title = paper_data.get('title', '')
        if title:
            titles.append(title)
    
    combined_titles = ' '.join(titles)
    cleaned_titles = clean_text(combined_titles)
    return cleaned_titles


def get_word_frequencies(text, top_n=50):
    """Get the most frequent words from text."""
    words = text.split()
    word_freq = Counter(words)
    return dict(word_freq.most_common(top_n))


def print_top_words(text, top_n=20, title="Words"):
    """Print the most frequent words from the text."""
    word_freq = get_word_frequencies(text, top_n)
    
    print(f"\nTop {top_n} most frequent words in {title}:")
    print("=" * 50)
    for i, (word, freq) in enumerate(word_freq.items(), 1):
        print(f"{i:2d}. {word:<20} ({freq} occurrences)")


def create_wordcloud(text, output_file=None, title="Word Cloud"):
    """Create and display a word cloud from the given text."""
    if not text.strip():
        print("No text available for word cloud")
        return None
    
    # Configure word cloud parameters
    wordcloud = WordCloud(
        width=1200,
        height=600,
        background_color='white',
        max_words=100,
        colormap='plasma',
        relative_scaling=0.5,
        random_state=42
    ).generate(text)
    
    # Create the plot
    plt.figure(figsize=(15, 8))
    plt.imshow(wordcloud, interpolation='bilinear')
    plt.axis('off')
    plt.title(title, fontsize=16, fontweight='bold', pad=20)
    plt.tight_layout()
    
    # Save if output file is specified
    if output_file:
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        plt.savefig(output_file, dpi=300, bbox_inches='tight')
        print(f"Word cloud saved to: {output_file}")
    
    plt.show()
    return wordcloud


def save_word_frequencies(text, output_file, top_n=100):
    """Save word frequencies to a JSON file."""
    word_freq = get_word_frequencies(text, top_n)
    
    # Create output directory if it doesn't exist
    os.makedirs(os.path.dirname(output_file), exist_ok=True)
    
    # Prepare data for JSON
    frequency_data = {
        "metadata": {
            "total_words": len(text.split()),
            "unique_words": len(set(text.split())),
            "top_n_words": top_n,
            "generated_at": "2025-01-20"
        },
        "word_frequencies": word_freq
    }
    
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(frequency_data, f, indent=2, ensure_ascii=False)
    
    print(f"Word frequencies saved to: {output_file}")


def generate_abstracts_wordcloud(json_file='data/wumacat_abstracts.json', 
                               output_file='wordclouds/abstracts_wordcloud.png',
                               frequencies_file='wordclouds/abstracts_word_frequencies.json'):
    """Generate word cloud for abstracts."""
    print("Loading papers data...")
    papers_data = load_data(json_file)
    print(f"Loaded {len(papers_data)} papers")
    
    print("Processing abstracts...")
    abstracts_text = extract_abstracts(papers_data)
    print_top_words(abstracts_text, title="Abstracts")
    
    print("Saving word frequencies...")
    save_word_frequencies(abstracts_text, frequencies_file)
    
    print("Generating word cloud...")
    create_wordcloud(abstracts_text, output_file, "Word Cloud: Paper Abstracts")
    
    print("Abstracts word cloud generation completed!")


def generate_titles_wordcloud(json_file='data/wumacat_abstracts.json',
                            output_file='wordclouds/titles_wordcloud.png',
                            frequencies_file='wordclouds/titles_word_frequencies.json'):
    """Generate word cloud for titles."""
    print("Loading papers data...")
    papers_data = load_data(json_file)
    print(f"Loaded {len(papers_data)} papers")
    
    print("Processing titles...")
    titles_text = extract_titles(papers_data)
    print_top_words(titles_text, title="Titles")
    
    print("Saving word frequencies...")
    save_word_frequencies(titles_text, frequencies_file)
    
    print("Generating word cloud...")
    create_wordcloud(titles_text, output_file, "Word Cloud: Paper Titles")
    
    print("Titles word cloud generation completed!")
