#!/usr/bin/env python3
"""
Generate word cloud for abstracts only
"""

import json
import re
import matplotlib.pyplot as plt
from wordcloud import WordCloud
from collections import Counter
import os


def load_data(json_file_path):
    """Load the JSON data from the file."""
    with open(json_file_path, 'r', encoding='utf-8') as file:
        data = json.load(file)
        return data.get('papers', {})


def clean_text(text):
    """Clean and preprocess text for word cloud generation."""
    if not text:
        return ""
    
    # Common stop words for astronomical literature
    stop_words = {
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
    
    # Remove HTML tags and special characters
    text = re.sub(r'<[^>]+>', '', text)
    text = re.sub(r'{[^}]*}', '', text)  # Remove LaTeX-style markup
    text = re.sub(r'\\[a-zA-Z]+', '', text)  # Remove LaTeX commands
    
    # Convert to lowercase and remove punctuation, keeping only letters and spaces
    text = re.sub(r'[^a-zA-Z\s]', ' ', text.lower())
    
    # Split into words and filter out stop words and short words
    words = [word.strip() for word in text.split() 
            if len(word.strip()) > 2 and word.strip() not in stop_words]
    
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


def get_word_frequencies(text, top_n=50):
    """Get the most frequent words from text."""
    words = text.split()
    word_freq = Counter(words)
    return dict(word_freq.most_common(top_n))


def print_top_words(text, top_n=20):
    """Print the most frequent words from the text."""
    word_freq = get_word_frequencies(text, top_n)
    
    print(f"\nTop {top_n} most frequent words in Abstracts:")
    print("=" * 50)
    for i, (word, freq) in enumerate(word_freq.items(), 1):
        print(f"{i:2d}. {word:<20} ({freq} occurrences)")


def create_wordcloud(text, output_file=None):
    """Create and display a word cloud from the given text."""
    if not text.strip():
        print("No text available for abstracts")
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
    plt.title('Word Cloud: Paper Abstracts', fontsize=16, fontweight='bold', pad=20)
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


def main():
    """Main function to run the abstracts word cloud generator."""
    json_file = 'data/wumacat_abstracts.json'
    output_file = 'wordclouds/abstracts_wordcloud.png'
    frequencies_file = 'wordclouds/abstracts_word_frequencies.json'
    
    print("Loading papers data...")
    papers_data = load_data(json_file)
    print(f"Loaded {len(papers_data)} papers")
    
    print("Processing abstracts...")
    abstracts_text = extract_abstracts(papers_data)
    print_top_words(abstracts_text)
    
    print("Saving word frequencies...")
    save_word_frequencies(abstracts_text, frequencies_file)
    
    print("Generating word cloud...")
    create_wordcloud(abstracts_text, output_file)
    
    print("Abstracts word cloud generation completed!")


if __name__ == "__main__":
    main()
