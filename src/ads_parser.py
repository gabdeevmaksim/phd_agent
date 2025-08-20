"""
ADS NASA Parser - Clean implementation with connection testing
"""

import requests
import os
from dotenv import load_dotenv
import time
import json
import csv
from datetime import datetime
from typing import Dict, List, Optional, Union, Set
import random

# Load environment variables
load_dotenv()

# Configuration
ADS_API_TOKEN = os.getenv("ADS_API_TOKEN")
ADS_API_BASE_URL = "https://api.adsabs.harvard.edu/v1"

def test_ads_connection() -> bool:
    """
    Test the connection to ADS API using the provided token.
    
    Returns:
        bool: True if connection is successful, False otherwise
    """
    if not ADS_API_TOKEN:
        print("❌ Error: ADS_API_TOKEN not found in environment variables")
        print("Please set your ADS API token in the .env file")
        return False
    
    headers = {"Authorization": f"Bearer {ADS_API_TOKEN}"}
    
    # Test with a simple query to verify the token works
    test_params = {
        "q": "author:Einstein",
        "fl": "bibcode,title",
        "rows": 1
    }
    
    try:
        print("🔍 Testing ADS API connection...")
        response = requests.get(
            f"{ADS_API_BASE_URL}/search/query",
            headers=headers,
            params=test_params,
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            if "response" in data and "docs" in data["response"]:
                print("✅ ADS API connection successful!")
                print(f"   Found {data['response']['numFound']} total results")
                print(f"   Retrieved {len(data['response']['docs'])} documents")
                return True
            else:
                print("❌ Unexpected response format from ADS API")
                return False
        else:
            print(f"❌ ADS API request failed with status code: {response.status_code}")
            print(f"   Response: {response.text}")
            return False
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to connect to ADS API: {e}")
        return False
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return False

def get_ads_headers() -> Dict[str, str]:
    """
    Get the headers needed for ADS API requests.
    
    Returns:
        dict: Headers with authorization token
    """
    if not ADS_API_TOKEN:
        raise ValueError("ADS_API_TOKEN not found in environment variables")
    
    return {"Authorization": f"Bearer {ADS_API_TOKEN}"}

def _make_ads_request_with_retry(url: str, headers: Dict[str, str], params: Dict[str, Union[str, int]], 
                                max_retries: int = 3, timeout: int = 30) -> Optional[requests.Response]:
    """
    Make an ADS API request with exponential backoff retry logic.
    
    Args:
        url: API endpoint URL
        headers: Request headers
        params: Query parameters
        max_retries: Maximum number of retry attempts
        timeout: Request timeout in seconds
        
    Returns:
        Response object or None if all retries failed
    """
    retry_codes = {429, 502, 503, 504}
    
    for attempt in range(max_retries + 1):
        try:
            response = requests.get(url, headers=headers, params=params, timeout=timeout)
            
            # Log rate limit info
            if 'X-RateLimit-Remaining' in response.headers:
                remaining = response.headers['X-RateLimit-Remaining']
                print(f"   Rate limit remaining: {remaining}")
            
            if response.status_code not in retry_codes:
                return response
                
            if attempt < max_retries:
                # Exponential backoff with jitter
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                print(f"   ⚠️  API returned {response.status_code}, retrying in {wait_time:.1f}s (attempt {attempt + 1}/{max_retries})")
                time.sleep(wait_time)
            else:
                print(f"   ❌ Max retries exceeded. Last status: {response.status_code}")
                return response
                
        except requests.exceptions.RequestException as e:
            if attempt < max_retries:
                wait_time = (2 ** attempt) + random.uniform(0, 1)
                print(f"   ⚠️  Request failed: {e}, retrying in {wait_time:.1f}s (attempt {attempt + 1}/{max_retries})")
                time.sleep(wait_time)
            else:
                print(f"   ❌ Request failed after {max_retries} retries: {e}")
                return None
    
    return None

def get_paper_info(bibcode: str, show_abstract: bool = True) -> Optional[Dict]:
    """
    Download all information about a paper using its bibcode.
    
    Args:
        bibcode (str): The bibcode of the paper to retrieve
        show_abstract (bool): Whether to display the abstract in the output
        
    Returns:
        dict: Paper information or None if failed
    """
    if not ADS_API_TOKEN:
        print("❌ Error: ADS_API_TOKEN not found in environment variables")
        return None
    
    headers = {"Authorization": f"Bearer {ADS_API_TOKEN}"}
    
    try:
        print(f"🔍 Retrieving paper information for bibcode: {bibcode}")
        
        # Query the ADS API for the specific bibcode
        params = {
            "q": f"bibcode:{bibcode}",
            "fl": "*"  # Get all available fields
        }
        
        response = requests.get(
            f"{ADS_API_BASE_URL}/search/query",
            headers=headers,
            params=params,
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            if "response" in data and "docs" in data["response"] and data["response"]["docs"]:
                paper_info = data["response"]["docs"][0]  # Get the first (and should be only) result
                print(f"✅ Successfully retrieved paper information")
                print(f"   Title: {paper_info.get('title', ['N/A'])[0] if paper_info.get('title') else 'N/A'}")
                print(f"   Authors: {', '.join(paper_info.get('author', ['N/A'])) if paper_info.get('author') else 'N/A'}")
                print(f"   Year: {paper_info.get('year', 'N/A')}")
                print(f"   Journal: {paper_info.get('pub', 'N/A')}")
                
                # Display abstract if requested and available
                if show_abstract and paper_info.get('abstract'):
                    abstract = paper_info.get('abstract')
                    print(f"   Abstract: {abstract}")
                elif show_abstract:
                    print("   Abstract: Not available")
                
                return paper_info
            else:
                print(f"❌ No paper found with bibcode: {bibcode}")
                return None
        else:
            print(f"❌ ADS API request failed with status code: {response.status_code}")
            print(f"   Response: {response.text}")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to retrieve paper information: {e}")
        return None
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return None

def get_abstract(bibcode: str) -> Optional[str]:
    """
    Retrieve only the abstract of a paper using its bibcode.
    
    Args:
        bibcode: The bibcode of the paper to retrieve
        
    Returns:
        Abstract text or None if not found
    """
    if not ADS_API_TOKEN:
        print("❌ Error: ADS_API_TOKEN not found in environment variables")
        return None
    
    headers = {"Authorization": f"Bearer {ADS_API_TOKEN}"}
    
    try:
        print(f"📄 Retrieving abstract for bibcode: {bibcode}")
        
        # Query the ADS API for the specific bibcode, requesting only abstract
        params = {
            "q": f"bibcode:{bibcode}",
            "fl": "bibcode,title,abstract"  # Only get bibcode, title, and abstract
        }
        
        response = requests.get(
            f"{ADS_API_BASE_URL}/search/query",
            headers=headers,
            params=params,
            timeout=10
        )
        
        if response.status_code == 200:
            data = response.json()
            if "response" in data and "docs" in data["response"] and data["response"]["docs"]:
                paper_info = data["response"]["docs"][0]
                abstract = paper_info.get('abstract')
                title = paper_info.get('title', ['N/A'])[0] if paper_info.get('title') else 'N/A'
                
                if abstract:
                    print(f"✅ Abstract retrieved successfully")
                    print(f"   Title: {title}")
                    print(f"   Abstract length: {len(abstract)} characters")
                    print(f"\n📄 Abstract:")
                    print("-" * 60)
                    print(abstract)
                    print("-" * 60)
                    return abstract
                else:
                    print(f"❌ No abstract available for this paper")
                    print(f"   Title: {title}")
                    return None
            else:
                print(f"❌ No paper found with bibcode: {bibcode}")
                return None
        else:
            print(f"❌ ADS API request failed with status code: {response.status_code}")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Failed to retrieve abstract: {e}")
        return None
    except Exception as e:
        print(f"❌ Unexpected error: {e}")
        return None

def get_bulk_paper_info(bibcodes: List[str], show_abstracts: bool = False, 
                       batch_size: int = 50) -> Optional[Dict[str, Dict]]:
    """
    Retrieve information for multiple papers using their bibcodes in batches.
    
    Args:
        bibcodes: List of bibcodes to retrieve
        show_abstracts: Whether to display abstracts in output
        batch_size: Number of bibcodes per request (recommended: 50-100)
        
    Returns:
        Dictionary with bibcode as key and paper info as value
    """
    if not ADS_API_TOKEN:
        print("❌ Error: ADS_API_TOKEN not found in environment variables")
        return None
    
    headers = {"Authorization": f"Bearer {ADS_API_TOKEN}"}
    results = {}
    total_bibcodes = len(bibcodes)
    
    print(f"🔍 Retrieving information for {total_bibcodes} papers in batches of {batch_size}")
    
    # Process bibcodes in batches
    for i in range(0, total_bibcodes, batch_size):
        batch = bibcodes[i:i + batch_size]
        batch_num = (i // batch_size) + 1
        total_batches = (total_bibcodes + batch_size - 1) // batch_size
        
        print(f"\n📦 Processing batch {batch_num}/{total_batches} ({len(batch)} bibcodes)")
        
        try:
            # Create query for multiple bibcodes
            bibcode_query = " OR ".join([f"bibcode:{bibcode}" for bibcode in batch])
            
            params = {
                "q": bibcode_query,
                "fl": "*",  # Get all available fields
                "rows": len(batch)  # Ensure we get all results
            }
            
            response = requests.get(
                f"{ADS_API_BASE_URL}/search/query",
                headers=headers,
                params=params,
                timeout=30  # Longer timeout for bulk requests
            )
            
            # Check rate limit headers
            if 'X-RateLimit-Remaining' in response.headers:
                remaining = response.headers['X-RateLimit-Remaining']
                print(f"   Rate limit remaining: {remaining}")
            
            if response.status_code == 200:
                data = response.json()
                if "response" in data and "docs" in data["response"]:
                    batch_results = data["response"]["docs"]
                    
                    for paper in batch_results:
                        bibcode = paper.get('bibcode')
                        if bibcode:
                            results[bibcode] = paper
                            
                            if show_abstracts:
                                title = paper.get('title', ['N/A'])[0] if paper.get('title') else 'N/A'
                                abstract = paper.get('abstract', 'No abstract available')
                                print(f"   ✅ {bibcode}: {title}")
                                if abstract != 'No abstract available':
                                    print(f"      Abstract: {abstract[:100]}...")
                    
                    print(f"   ✅ Retrieved {len(batch_results)} papers from batch")
                else:
                    print(f"   ❌ No results in batch {batch_num}")
            else:
                print(f"   ❌ Batch {batch_num} failed with status {response.status_code}")
                if response.status_code == 429:  # Rate limit exceeded
                    print("   ⚠️  Rate limit exceeded. Consider reducing batch size or adding delays.")
                    break
                
        except requests.exceptions.RequestException as e:
            print(f"   ❌ Error in batch {batch_num}: {e}")
            continue
        except Exception as e:
            print(f"   ❌ Unexpected error in batch {batch_num}: {e}")
            continue
        
        # Add small delay between batches to be respectful to the API
        if i + batch_size < total_bibcodes:
            time.sleep(1)
    
    print(f"\n🎉 Bulk retrieval completed! Retrieved {len(results)}/{total_bibcodes} papers")
    return results

def download_catalogue_abstracts(csv_file_path: str, output_json_path: str, 
                               batch_size: int = 50) -> Optional[Dict[str, Dict]]:
    """
    Download abstracts for all papers in a catalogue and save to JSON.
    
    Args:
        csv_file_path: Path to the CSV file containing bibcodes
        output_json_path: Path where to save the JSON output
        batch_size: Number of bibcodes per batch (default: 50)
        
    Returns:
        Results with title and abstract for each bibcode
    """
    import csv
    import json
    from datetime import datetime
    
    if not ADS_API_TOKEN:
        print("❌ Error: ADS_API_TOKEN not found in environment variables")
        return None
    
    # Read bibcodes from CSV
    print(f"📂 Reading bibcodes from {csv_file_path}")
    bibcodes = []
    
    try:
        with open(csv_file_path, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                if 'Bibcode' in row and row['Bibcode'].strip():
                    bibcodes.append(row['Bibcode'].strip())
        
        print(f"✅ Found {len(bibcodes)} total bibcodes in catalogue")
        
        # Remove duplicates while preserving order
        unique_bibcodes = []
        seen = set()
        for bibcode in bibcodes:
            if bibcode not in seen:
                unique_bibcodes.append(bibcode)
                seen.add(bibcode)
        
        duplicates_removed = len(bibcodes) - len(unique_bibcodes)
        bibcodes = unique_bibcodes
        
        print(f"✅ Removed {duplicates_removed} duplicates")
        print(f"✅ Processing {len(bibcodes)} unique bibcodes")
        
    except FileNotFoundError:
        print(f"❌ Error: File {csv_file_path} not found")
        return None
    except Exception as e:
        print(f"❌ Error reading CSV: {e}")
        return None
    
    if not bibcodes:
        print("❌ No bibcodes found in the file")
        return None
    
    # Initialize results structure
    results = {
        "metadata": {
            "source_file": csv_file_path,
            "download_date": datetime.now().isoformat(),
            "total_bibcodes": len(bibcodes),
            "batch_size": batch_size
        },
        "papers": {}
    }
    
    headers = {"Authorization": f"Bearer {ADS_API_TOKEN}"}
    total_bibcodes = len(bibcodes)
    
    print(f"🚀 Starting download of abstracts for {total_bibcodes} papers")
    print(f"📦 Using batch size: {batch_size}")
    print(f"💾 Output file: {output_json_path}")
    print("=" * 60)
    
    # Process bibcodes in batches
    for i in range(0, total_bibcodes, batch_size):
        batch = bibcodes[i:i + batch_size]
        batch_num = (i // batch_size) + 1
        total_batches = (total_bibcodes + batch_size - 1) // batch_size
        
        print(f"\n📦 Processing batch {batch_num}/{total_batches} ({len(batch)} bibcodes)")
        
        try:
            # Create query for multiple bibcodes
            bibcode_query = " OR ".join([f"bibcode:{bibcode}" for bibcode in batch])
            
            params = {
                "q": bibcode_query,
                "fl": "bibcode,title,abstract",  # Only get bibcode, title, and abstract
                "rows": len(batch)
            }
            
            response = requests.get(
                f"{ADS_API_BASE_URL}/search/query",
                headers=headers,
                params=params,
                timeout=30
            )
            
            # Check rate limit headers
            if 'X-RateLimit-Remaining' in response.headers:
                remaining = response.headers['X-RateLimit-Remaining']
                print(f"   Rate limit remaining: {remaining}")
            
            if response.status_code == 200:
                data = response.json()
                if "response" in data and "docs" in data["response"]:
                    batch_results = data["response"]["docs"]
                    
                    batch_count = 0
                    for paper in batch_results:
                        bibcode = paper.get('bibcode')
                        if bibcode:
                            # Store only title and abstract
                            results["papers"][bibcode] = {
                                "title": paper.get('title', [''])[0] if paper.get('title') else '',
                                "abstract": paper.get('abstract', '')
                            }
                            batch_count += 1
                    
                    print(f"   ✅ Retrieved {batch_count} papers from batch")
                    
                    # Show sample result
                    if batch_results:
                        sample = batch_results[0]
                        sample_bibcode = sample.get('bibcode', 'N/A')
                        sample_title = sample.get('title', ['N/A'])[0] if sample.get('title') else 'N/A'
                        has_abstract = bool(sample.get('abstract'))
                        print(f"   📄 Sample: {sample_bibcode}")
                        print(f"      Title: {sample_title[:60]}...")
                        print(f"      Abstract: {'✅ Available' if has_abstract else '❌ Not available'}")
                        
                else:
                    print(f"   ❌ No results in batch {batch_num}")
                    
            elif response.status_code == 429:  # Rate limit exceeded
                print(f"   ⚠️  Rate limit exceeded in batch {batch_num}")
                print("   Stopping to prevent API abuse. Try again later or reduce batch size.")
                break
            else:
                print(f"   ❌ Batch {batch_num} failed with status {response.status_code}")
                
        except requests.exceptions.RequestException as e:
            print(f"   ❌ Network error in batch {batch_num}: {e}")
            continue
        except Exception as e:
            print(f"   ❌ Unexpected error in batch {batch_num}: {e}")
            continue
        
        # Add delay between batches to be respectful to the API
        if i + batch_size < total_bibcodes:
            print("   ⏱️  Waiting 2 seconds before next batch...")
            time.sleep(2)
        
        # Save intermediate results every 5 batches
        if batch_num % 5 == 0:
            try:
                with open(output_json_path, 'w', encoding='utf-8') as f:
                    json.dump(results, f, indent=2, ensure_ascii=False)
                print(f"   💾 Intermediate save completed (batch {batch_num})")
            except Exception as e:
                print(f"   ⚠️  Warning: Could not save intermediate results: {e}")
    
    # Final save
    try:
        results["metadata"]["completed_date"] = datetime.now().isoformat()
        results["metadata"]["papers_retrieved"] = len(results["papers"])
        
        with open(output_json_path, 'w', encoding='utf-8') as f:
            json.dump(results, f, indent=2, ensure_ascii=False)
        
        print(f"\n🎉 Download completed!")
        print(f"📊 Final Results:")
        print(f"   Total papers processed: {len(results['papers'])}/{total_bibcodes}")
        print(f"   Success rate: {len(results['papers'])/total_bibcodes*100:.1f}%")
        print(f"   Output saved to: {output_json_path}")
        
        # Count papers with abstracts
        papers_with_abstracts = sum(1 for paper in results["papers"].values() if paper["abstract"])
        print(f"   Papers with abstracts: {papers_with_abstracts}/{len(results['papers'])} ({papers_with_abstracts/len(results['papers'])*100:.1f}%)")
        
        return results
        
    except Exception as e:
        print(f"❌ Error saving final results: {e}")
        return None

def search_papers_by_keywords(keywords, search_fields="full", max_results=50):
    """
    Search for papers containing keywords in specified fields.
    
    Args:
        keywords (list): List of keywords to search for
        search_fields (str): "title", "abs", "full", or "title,abs"
        max_results (int): Maximum number of results to return
        
    Returns:
        dict: API response with matching papers
    """
    if not ADS_API_TOKEN:
        print("❌ Error: ADS_API_TOKEN not found in environment variables")
        return None
    
    headers = {"Authorization": f"Bearer {ADS_API_TOKEN}"}
    
    # Build query based on search fields
    if search_fields == "title":
        query_parts = [f"title:{keyword}" for keyword in keywords]
    elif search_fields == "abs":
        query_parts = [f"abs:{keyword}" for keyword in keywords]
    elif search_fields == "full":
        query_parts = [f"full:{keyword}" for keyword in keywords]
    elif search_fields == "title,abs":
        # Search in both title and abstract
        title_parts = [f"title:{keyword}" for keyword in keywords]
        abs_parts = [f"abs:{keyword}" for keyword in keywords]
        query_parts = title_parts + abs_parts
    else:
        print(f"❌ Invalid search_fields: {search_fields}")
        return None
    
    # Join keywords with OR operator
    query = " AND ".join(query_parts)
    
    params = {
        "q": query,
        "fl": "bibcode,title,abstract,author,year,pub",
        "rows": max_results,
        "sort": "date desc"  # Sort by date, newest first
    }
    
    try:
        print(f"🔍 Searching for: {query}")
        response = requests.get(
            f"{ADS_API_BASE_URL}/search/query",
            headers=headers,
            params=params,
            timeout=30
        )
        
        if response.status_code == 200:
            data = response.json()
            num_found = data.get("response", {}).get("numFound", 0)
            docs = data.get("response", {}).get("docs", [])
            
            print(f"✅ Found {num_found} papers, retrieved {len(docs)}")
            return data
        else:
            print(f"❌ API request failed with status {response.status_code}")
            return None
            
    except requests.exceptions.RequestException as e:
        print(f"❌ Request failed: {e}")
        return None


def process_search_results(results):
    """
    Process and display search results.
    
    Args:
        results (dict): API response from search_papers_by_keywords
        
    Returns:
        list: List of processed paper information
    """
    if not results or "response" not in results:
        print("❌ No results to process")
        return []
    
    docs = results["response"]["docs"]
    processed_papers = []
    
    print(f"\n📊 Processing {len(docs)} papers:")
    print("=" * 80)
    
    for i, paper in enumerate(docs, 1):
        title = paper.get("title", ["N/A"])[0] if paper.get("title") else "N/A"
        authors = ", ".join(paper.get("author", ["N/A"])) if paper.get("author") else "N/A"
        year = paper.get("year", "N/A")
        abstract = paper.get("abstract", "No abstract available")
        bibcode = paper.get("bibcode", "N/A")
        journal = paper.get("pub", "N/A")
        
        # Store processed paper info
        processed_paper = {
            "bibcode": bibcode,
            "title": title,
            "authors": authors,
            "year": year,
            "journal": journal,
            "abstract": abstract
        }
        processed_papers.append(processed_paper)
        
        # Display paper info
        print(f"\n{i}. {title}")
        print(f"   Authors: {authors}")
        print(f"   Year: {year} | Journal: {journal}")
        print(f"   Bibcode: {bibcode}")
        print(f"   Abstract: {abstract[:200]}{'...' if len(abstract) > 200 else ''}")
        print("-" * 80)
    
    return processed_papers


def search_and_process_papers(keywords, search_fields="full", max_results=50):
    """
    Combined function to search for papers and process the results.
    
    Args:
        keywords (list): List of keywords to search for
        search_fields (str): "title", "abs", "full", or "title,abs"
        max_results (int): Maximum number of results to return
        
    Returns:
        list: List of processed paper information
    """
    print(f"🚀 Starting search for keywords: {keywords}")
    print(f"📂 Search fields: {search_fields}")
    print(f"📊 Max results: {max_results}")
    
    # Search for papers
    results = search_papers_by_keywords(keywords, search_fields, max_results)
    
    if results:
        # Process and display results
        processed_papers = process_search_results(results)
        return processed_papers
    else:
        print("❌ No results found or search failed")
        return []


def count_publications_for_keywords(keywords, search_fields="full", description="", silent=False):
    """
    Count how many publications match the given keywords without retrieving the full data.
    
    Args:
        keywords (list): List of keywords to search for
        search_fields (str): "title", "abs", "full", or "title,abs"
        description (str): Description for logging
        silent (bool): If True, suppress output messages
    
    Returns:
        int: Total number of publications found
    """
    if not silent:
        print(f"\n🔍 Counting publications for {description}...")
        print(f"Keywords: {keywords}")
        print(f"Search fields: {search_fields}")
    
    # Use max_results=1 to minimize data transfer while getting the count
    results = search_papers_by_keywords(keywords, search_fields=search_fields, max_results=1)
    
    if results and "response" in results:
        total_count = results["response"].get("numFound", 0)
        if not silent:
            print(f"📊 Total publications found: {total_count:,}")
        return total_count
    else:
        if not silent:
            print("❌ Failed to get publication count")
        return 0


def search_all_bibcodes(keywords, search_fields="full", silent=False):
    """
    Search for papers and return ALL bibcodes using pagination.
    Handles the 2000 per request limit automatically.
    
    Args:
        keywords (list): List of keywords to search for
        search_fields (str): "title", "abs", "full", or "title,abs"
        silent (bool): If True, suppress output messages
    
    Returns:
        list: List of all bibcodes found
    """
    if not ADS_API_TOKEN:
        if not silent:
            print("❌ Error: ADS_API_TOKEN not found in environment variables")
        return []
    
    headers = {"Authorization": f"Bearer {ADS_API_TOKEN}"}
    
    # Build query based on search fields
    if search_fields == "title":
        query_parts = [f"title:{keyword}" for keyword in keywords]
    elif search_fields == "abs":
        query_parts = [f"abs:{keyword}" for keyword in keywords]
    elif search_fields == "full":
        query_parts = [f"full:{keyword}" for keyword in keywords]
    elif search_fields == "title,abs":
        title_parts = [f"title:{keyword}" for keyword in keywords]
        abs_parts = [f"abs:{keyword}" for keyword in keywords]
        query_parts = title_parts + abs_parts
    else:
        if not silent:
            print(f"❌ Invalid search_fields: {search_fields}")
        return []
    
    # Join keywords with AND operator
    query = " AND ".join(query_parts)
    
    # First request to get total count
    initial_params = {
        "q": query,
        "fl": "bibcode",
        "rows": 1,  # Just get count first
        "sort": "date desc"
    }
    
    try:
        if not silent:
            print(f"🔍 Searching for bibcodes with query: {query}")
            print(f"📊 Getting total count first...")
        
        response = requests.get(
            f"{ADS_API_BASE_URL}/search/query",
            headers=headers,
            params=initial_params,
            timeout=30
        )
        
        if response.status_code != 200:
            if not silent:
                print(f"❌ Initial request failed with status {response.status_code}")
            return []
        
        data = response.json()
        total_found = data.get("response", {}).get("numFound", 0)
        
        if total_found == 0:
            if not silent:
                print("❌ No papers found for this query")
            return []
        
        if not silent:
            print(f"✅ Found {total_found:,} total papers")
        
        # Calculate pagination
        max_per_request = 2000
        requests_needed = (total_found + max_per_request - 1) // max_per_request
        
        if not silent:
            print(f"📄 Will need {requests_needed} requests to get all bibcodes")
        
        all_bibcodes = []
        
        # Get all bibcodes with pagination
        for i in range(requests_needed):
            start = i * max_per_request
            remaining = total_found - start
            rows = min(max_per_request, remaining)
            
            if not silent:
                print(f"📥 Request {i+1}/{requests_needed}: Getting bibcodes {start+1:,}-{start+rows:,}")
            
            params = {
                "q": query,
                "fl": "bibcode",
                "rows": rows,
                "start": start,
                "sort": "date desc"
            }
            
            response = requests.get(
                f"{ADS_API_BASE_URL}/search/query",
                headers=headers,
                params=params,
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                docs = data.get("response", {}).get("docs", [])
                batch_bibcodes = [doc.get("bibcode") for doc in docs if doc.get("bibcode")]
                all_bibcodes.extend(batch_bibcodes)
                
                if not silent:
                    print(f"   ✅ Retrieved {len(batch_bibcodes)} bibcodes")
                    
                    # Check rate limit
                    if 'X-RateLimit-Remaining' in response.headers:
                        remaining_requests = response.headers['X-RateLimit-Remaining']
                        print(f"   🔄 API requests remaining: {remaining_requests}")
                
                # Small delay between requests to be nice to the API
                if i < requests_needed - 1:  # Don't delay after last request
                    time.sleep(0.5)
                    
            else:
                if not silent:
                    print(f"   ❌ Request {i+1} failed with status {response.status_code}")
                break
        
        if not silent:
            print(f"\n🎯 FINAL RESULTS:")
            print(f"   Total papers found: {total_found:,}")
            print(f"   Total bibcodes retrieved: {len(all_bibcodes):,}")
            print(f"   API requests used: {min(i+1, requests_needed)}")
        
        return all_bibcodes
        
    except requests.exceptions.RequestException as e:
        if not silent:
            print(f"❌ Request failed: {e}")
        return []


def search_exact_keywords(keywords: List[str], source_field: str = "full", 
                         max_results: int = 2000, count_only: bool = False) -> Optional[Dict]:
    """
    Search ADS for exact keyword matches in specified fields.
    
    Args:
        keywords: List of keywords to search for exactly
        source_field: Field to search in ("title", "abs", "full", "author", etc.)
        max_results: Maximum number of results to return
        count_only: If True, only return count information
        
    Returns:
        Dictionary with search results or count information
    """
    if not ADS_API_TOKEN:
        print("❌ Error: ADS_API_TOKEN not found in environment variables")
        return None
    
    if not keywords:
        print("❌ Error: No keywords provided")
        return None
    
    headers = get_ads_headers()
    
    # Build exact search query using =source:"keyword" format
    exact_queries = [f'={source_field}:"{keyword}"' for keyword in keywords]
    query = " AND ".join(exact_queries)
    
    print(f"🔍 Searching for exact keywords in {source_field} field:")
    print(f"   Keywords: {keywords}")
    print(f"   Query: {query}")
    
    try:
        if count_only:
            # Just get the count
            params = {
                "q": query,
                "fl": "bibcode",
                "rows": 0  # We only want the count
            }
        else:
            # Get full results
            params = {
                "q": query,
                "fl": "bibcode,title,abstract,author,pub,year,property",
                "rows": min(max_results, 2000),  # ADS single request limit
                "sort": "citation_count desc"  # Sort by most cited
            }
        
        response = _make_ads_request_with_retry(
            f"{ADS_API_BASE_URL}/search/query",
            headers,
            params,
            max_retries=3
        )
        
        if response and response.status_code == 200:
            data = response.json()
            if "response" in data:
                total_found = data["response"]["numFound"]
                docs = data["response"].get("docs", [])
                
                print(f"✅ Found {total_found} papers with exact keyword matches")
                
                if count_only:
                    return {
                        "total_found": total_found,
                        "keywords": keywords,
                        "source_field": source_field,
                        "query": query
                    }
                else:
                    # For larger result sets, implement pagination
                    all_docs = docs.copy()
                    
                    if max_results > 2000 and total_found > 2000 and len(docs) == 2000:
                        remaining_needed = min(max_results - 2000, total_found - 2000)
                        print(f"   Getting additional {remaining_needed} papers with pagination...")
                        
                        start = 2000
                        while start < total_found and len(all_docs) < max_results:
                            batch_size = min(2000, max_results - len(all_docs), total_found - start)
                            
                            page_params = {
                                "q": query,
                                "fl": "bibcode,title,abstract,author,pub,year,property",
                                "rows": batch_size,
                                "start": start,
                                "sort": "citation_count desc"
                            }
                            
                            page_response = _make_ads_request_with_retry(
                                f"{ADS_API_BASE_URL}/search/query",
                                headers,
                                page_params,
                                max_retries=3
                            )
                            
                            if page_response and page_response.status_code == 200:
                                page_data = page_response.json()
                                page_docs = page_data.get("response", {}).get("docs", [])
                                all_docs.extend(page_docs)
                                print(f"   Retrieved additional {len(page_docs)} papers (total: {len(all_docs)})")
                                start += batch_size
                                time.sleep(0.5)  # Brief delay between requests
                            else:
                                print(f"   ⚠️  Failed to get page starting at {start}")
                                break
                    
                    print(f"   📄 Total retrieved: {len(all_docs)} papers")
                    return {
                        "total_found": total_found,
                        "retrieved": len(all_docs),
                        "papers": all_docs,
                        "keywords": keywords,
                        "source_field": source_field,
                        "query": query
                    }
            else:
                print("❌ Unexpected response format")
                return None
        else:
            print(f"❌ Search failed with status {response.status_code if response else 'No response'}")
            return None
            
    except Exception as e:
        print(f"❌ Error during exact keyword search: {e}")
        return None


def compare_search_strategies(keywords: List[str], wumacat_bibcodes: Set[str], 
                            source_fields: List[str] = ["title", "abs", "full"]) -> Dict:
    """
    Compare exact keyword search strategies across different fields.
    
    Args:
        keywords: List of keywords to test
        wumacat_bibcodes: Set of known WUMaCat bibcodes for overlap analysis
        source_fields: List of fields to search in
        
    Returns:
        Dictionary with comparison results
    """
    if not ADS_API_TOKEN:
        print("❌ Error: ADS_API_TOKEN not found in environment variables")
        return {}
    
    print(f"🧪 Comparing exact search strategies for {len(keywords)} keywords")
    print(f"   Fields to test: {source_fields}")
    print(f"   WUMaCat bibcodes for overlap: {len(wumacat_bibcodes)}")
    
    results = {
        "keywords": keywords,
        "wumacat_size": len(wumacat_bibcodes),
        "field_results": {},
        "summary": {}
    }
    
    for field in source_fields:
        print(f"\n📊 Testing exact search in '{field}' field...")
        
        # Get count first
        count_result = search_exact_keywords(keywords, field, count_only=True)
        if not count_result:
            continue
            
        # Get actual results if count is reasonable
        total_found = count_result["total_found"]
        if total_found > 5000:
            print(f"   ⚠️  Too many results ({total_found}), skipping detailed retrieval")
            field_data = {
                "total_found": total_found,
                "retrieved": 0,
                "overlap_count": 0,
                "overlap_percentage": 0,
                "skipped": True
            }
        else:
            # Get detailed results
            search_result = search_exact_keywords(keywords, field, max_results=2000)
            if search_result:
                bibcodes = {paper["bibcode"] for paper in search_result["papers"]}
                overlap = bibcodes.intersection(wumacat_bibcodes)
                
                field_data = {
                    "total_found": total_found,
                    "retrieved": len(bibcodes),
                    "bibcodes": list(bibcodes),
                    "overlap_bibcodes": list(overlap),
                    "overlap_count": len(overlap),
                    "overlap_percentage": (len(overlap) / len(wumacat_bibcodes)) * 100 if wumacat_bibcodes else 0,
                    "skipped": False
                }
                
                print(f"   ✅ Overlap: {len(overlap)}/{len(wumacat_bibcodes)} WUMaCat papers ({field_data['overlap_percentage']:.1f}%)")
            else:
                field_data = {"error": "Search failed", "skipped": True}
        
        results["field_results"][field] = field_data
        
        # Add delay between searches
        time.sleep(1)
    
    # Generate summary
    successful_fields = [f for f, r in results["field_results"].items() if not r.get("skipped", True) and not r.get("error")]
    if successful_fields:
        best_field = max(successful_fields, key=lambda f: results["field_results"][f]["overlap_percentage"])
        results["summary"] = {
            "best_field": best_field,
            "best_overlap_percentage": results["field_results"][best_field]["overlap_percentage"],
            "best_overlap_count": results["field_results"][best_field]["overlap_count"],
            "successful_fields": successful_fields
        }
        
        print(f"\n🎯 Best field: '{best_field}' with {results['summary']['best_overlap_percentage']:.1f}% overlap")
    
    return results


def test_keyword_combination_sizes(all_keywords: List[str], wumacat_bibcodes: Set[str], 
                                 combination_sizes: List[int] = [20, 15, 10, 7, 5],
                                 source_field: str = "full") -> Dict:
    """
    Test different numbers of keyword combinations in full text search.
    
    Args:
        all_keywords: Full list of keywords to test
        wumacat_bibcodes: Set of known WUMaCat bibcodes for overlap analysis
        combination_sizes: List of keyword counts to test
        source_field: Field to search in (default: "full")
        
    Returns:
        Dictionary with results for each combination size
    """
    if not ADS_API_TOKEN:
        print("❌ Error: ADS_API_TOKEN not found in environment variables")
        return {}
    
    print(f"🧪 Testing keyword combination sizes in '{source_field}' field")
    print(f"   Available keywords: {len(all_keywords)}")
    print(f"   Combination sizes to test: {combination_sizes}")
    print(f"   WUMaCat reference size: {len(wumacat_bibcodes)}")
    
    results = {
        "source_field": source_field,
        "total_keywords_available": len(all_keywords),
        "wumacat_size": len(wumacat_bibcodes),
        "combination_results": {},
        "summary": {}
    }
    
    for size in combination_sizes:
        if size > len(all_keywords):
            print(f"\n⚠️  Skipping size {size} - only {len(all_keywords)} keywords available")
            continue
            
        print(f"\n📊 Testing combination size: {size} keywords")
        
        # Select top N keywords
        test_keywords = all_keywords[:size]
        print(f"   Keywords: {test_keywords}")
        
        # Get count first
        count_result = search_exact_keywords(test_keywords, source_field, count_only=True)
        if not count_result:
            print(f"   ❌ Count query failed for size {size}")
            continue
            
        total_found = count_result["total_found"]
        print(f"   📈 Found {total_found} papers with ALL {size} keywords")
        
        # Determine if we should get detailed results
        if total_found == 0:
            combination_data = {
                "keyword_count": size,
                "keywords": test_keywords,
                "total_found": 0,
                "retrieved": 0,
                "overlap_count": 0,
                "overlap_percentage": 0,
                "precision": 0,
                "recall": 0,
                "f1_score": 0,
                "note": "No papers found"
            }
        elif total_found > 20000:
            print(f"   ⚠️  Too many results ({total_found}), getting sample only")
            search_result = search_exact_keywords(test_keywords, source_field, max_results=20000)
            if search_result:
                bibcodes = {paper["bibcode"] for paper in search_result["papers"]}
                overlap = bibcodes.intersection(wumacat_bibcodes)
                
                combination_data = {
                    "keyword_count": size,
                    "keywords": test_keywords,
                    "total_found": total_found,
                    "retrieved": len(bibcodes),
                    "overlap_count": len(overlap),
                    "overlap_percentage": (len(overlap) / len(wumacat_bibcodes)) * 100,
                    "precision": (len(overlap) / len(bibcodes)) * 100 if bibcodes else 0,
                    "recall": (len(overlap) / len(wumacat_bibcodes)) * 100,
                    "note": f"Sample of {len(bibcodes)} from {total_found} total"
                }
            else:
                combination_data = {"error": "Failed to retrieve sample", "keyword_count": size}
        else:
            # Get all results
            search_result = search_exact_keywords(test_keywords, source_field, max_results=total_found)
            if search_result:
                bibcodes = {paper["bibcode"] for paper in search_result["papers"]}
                overlap = bibcodes.intersection(wumacat_bibcodes)
                
                precision = (len(overlap) / len(bibcodes)) * 100 if bibcodes else 0
                recall = (len(overlap) / len(wumacat_bibcodes)) * 100
                f1_score = (2 * precision * recall) / (precision + recall) if (precision + recall) > 0 else 0
                
                combination_data = {
                    "keyword_count": size,
                    "keywords": test_keywords,
                    "total_found": total_found,
                    "retrieved": len(bibcodes),
                    "bibcodes": list(bibcodes),
                    "overlap_bibcodes": list(overlap),
                    "overlap_count": len(overlap),
                    "overlap_percentage": (len(overlap) / len(wumacat_bibcodes)) * 100,
                    "precision": precision,
                    "recall": recall,
                    "f1_score": f1_score
                }
            else:
                combination_data = {"error": "Failed to retrieve results", "keyword_count": size}
        
        # Calculate F1 score if not already done
        if "f1_score" not in combination_data and "precision" in combination_data and "recall" in combination_data:
            p, r = combination_data["precision"], combination_data["recall"]
            combination_data["f1_score"] = (2 * p * r) / (p + r) if (p + r) > 0 else 0
        
        results["combination_results"][size] = combination_data
        
        # Display results
        if "error" not in combination_data:
            overlap_pct = combination_data["overlap_percentage"]
            precision = combination_data.get("precision", 0)
            print(f"   ✅ WUMaCat overlap: {combination_data['overlap_count']}/{len(wumacat_bibcodes)} ({overlap_pct:.1f}%)")
            print(f"   📊 Precision: {precision:.1f}% | Recall: {overlap_pct:.1f}% | F1: {combination_data.get('f1_score', 0):.1f}%")
        
        # Add delay between searches
        time.sleep(1)
    
    # Generate summary analysis
    successful_combinations = {k: v for k, v in results["combination_results"].items() 
                             if "error" not in v and v.get("total_found", 0) > 0}
    
    if successful_combinations:
        # Find best by different metrics
        best_by_overlap = max(successful_combinations.items(), key=lambda x: x[1]["overlap_percentage"])
        best_by_precision = max(successful_combinations.items(), key=lambda x: x[1].get("precision", 0))
        best_by_f1 = max(successful_combinations.items(), key=lambda x: x[1].get("f1_score", 0))
        
        results["summary"] = {
            "successful_combinations": list(successful_combinations.keys()),
            "best_overlap": {
                "size": best_by_overlap[0],
                "percentage": best_by_overlap[1]["overlap_percentage"],
                "count": best_by_overlap[1]["overlap_count"]
            },
            "best_precision": {
                "size": best_by_precision[0],
                "percentage": best_by_precision[1].get("precision", 0),
                "total_found": best_by_precision[1]["total_found"]
            },
            "best_f1": {
                "size": best_by_f1[0],
                "score": best_by_f1[1].get("f1_score", 0)
            }
        }
        
        print(f"\n🎯 SUMMARY:")
        print(f"   Best overlap: {best_by_overlap[1]['overlap_percentage']:.1f}% with {best_by_overlap[0]} keywords")
        print(f"   Best precision: {best_by_precision[1].get('precision', 0):.1f}% with {best_by_precision[0]} keywords")
        print(f"   Best F1-score: {best_by_f1[1].get('f1_score', 0):.1f}% with {best_by_f1[0]} keywords")
    
    return results


def find_similar_papers(bibcode: str, max_results: int = 50, fields: List[str] = None, min_score: float = None) -> Optional[Dict]:
    """
    Find papers similar to a given paper using ADS similarity search based on abstract content.
    
    Args:
        bibcode: The bibcode of the reference paper to find similar papers for
        max_results: Maximum number of similar papers to return (default: 50)
        fields: List of fields to retrieve (default: bibcode, title, abstract, author, year, pub)
        min_score: Minimum similarity score to include papers (default: None, no filtering)
        
    Returns:
        Dictionary with similar papers information or None if failed
    """
    if not ADS_API_TOKEN:
        print("❌ Error: ADS_API_TOKEN not found in environment variables")
        return None
    
    if not bibcode or not bibcode.strip():
        print("❌ Error: Invalid bibcode provided")
        return None
    
    # Default fields if none specified
    if fields is None:
        fields = ["bibcode", "title", "abstract", "author", "year", "pub", "citation_count"]
    
    # Ensure 'score' field is included if min_score filtering is requested
    if min_score is not None and "score" not in fields:
        fields = fields + ["score"]
    
    headers = get_ads_headers()
    
    # Build similarity query using ADS similar() function
    query = f"similar({bibcode.strip()})"
    
    print(f"🔍 Searching for papers similar to: {bibcode}")
    print(f"   Query: {query}")
    print(f"   Max results: {max_results}")
    print(f"   Min score filter: {min_score if min_score is not None else 'None'}")
    print(f"   Fields: {fields}")
    
    try:
        params = {
            "q": query,
            "fl": ",".join(fields),
            "rows": min(max_results, 2000),  # ADS single request limit
            "sort": "score desc"  # Sort by similarity score (highest first)
        }
        
        response = _make_ads_request_with_retry(
            f"{ADS_API_BASE_URL}/search/query",
            headers,
            params,
            max_retries=3
        )
        
        if response and response.status_code == 200:
            data = response.json()
            if "response" in data:
                total_found = data["response"]["numFound"]
                docs = data["response"].get("docs", [])
                
                print(f"✅ Found {total_found} similar papers")
                print(f"   Retrieved: {len(docs)} papers")
                
                # Filter out the original paper if it appears in results
                filtered_docs = [doc for doc in docs if doc.get("bibcode") != bibcode]
                
                if len(filtered_docs) < len(docs):
                    print(f"   Filtered out original paper from results")
                
                # Apply score filtering if min_score is specified
                score_filtered_docs = filtered_docs
                if min_score is not None:
                    score_filtered_docs = [doc for doc in filtered_docs if doc.get("score", 0) >= min_score]
                    print(f"   Score filtered: {len(score_filtered_docs)}/{len(filtered_docs)} papers (score >= {min_score})")
                
                return {
                    "reference_bibcode": bibcode,
                    "total_found": total_found,
                    "retrieved": len(filtered_docs),
                    "score_filtered": len(score_filtered_docs),
                    "papers": score_filtered_docs,
                    "query": query,
                    "fields": fields,
                    "min_score": min_score
                }
            else:
                print("❌ Unexpected response format")
                return None
        else:
            print(f"❌ Similarity search failed with status {response.status_code if response else 'No response'}")
            return None
            
    except Exception as e:
        print(f"❌ Error during similarity search: {e}")
        return None


def find_similar_papers_bulk(bibcodes: List[str], max_results_per_paper: int = 20, 
                            delay_between_requests: float = 1.0) -> Dict[str, Dict]:
    """
    Find similar papers for multiple bibcodes in bulk.
    
    Args:
        bibcodes: List of bibcodes to find similar papers for
        max_results_per_paper: Maximum similar papers per input bibcode
        delay_between_requests: Delay in seconds between API requests
        
    Returns:
        Dictionary with bibcode as key and similarity results as value
    """
    if not ADS_API_TOKEN:
        print("❌ Error: ADS_API_TOKEN not found in environment variables")
        return {}
    
    if not bibcodes:
        print("❌ Error: No bibcodes provided")
        return {}
    
    print(f"🔍 Finding similar papers for {len(bibcodes)} bibcodes")
    print(f"   Max results per paper: {max_results_per_paper}")
    print(f"   Delay between requests: {delay_between_requests}s")
    
    results = {}
    total_bibcodes = len(bibcodes)
    
    for i, bibcode in enumerate(bibcodes, 1):
        print(f"\n📄 Processing {i}/{total_bibcodes}: {bibcode}")
        
        try:
            similar_papers = find_similar_papers(
                bibcode, 
                max_results=max_results_per_paper,
                fields=["bibcode", "title", "author", "year", "pub", "citation_count"]
            )
            
            if similar_papers:
                results[bibcode] = similar_papers
                print(f"   ✅ Found {similar_papers['retrieved']} similar papers")
            else:
                results[bibcode] = {"error": "Failed to find similar papers"}
                print(f"   ❌ Failed to find similar papers")
                
        except Exception as e:
            results[bibcode] = {"error": str(e)}
            print(f"   ❌ Error: {e}")
        
        # Add delay between requests to be respectful to the API
        if i < total_bibcodes:
            time.sleep(delay_between_requests)
    
    successful_searches = sum(1 for result in results.values() if "error" not in result)
    print(f"\n🎉 Bulk similarity search completed!")
    print(f"   Successful searches: {successful_searches}/{total_bibcodes}")
    
    return results


def analyze_similarity_overlap(reference_bibcode: str, comparison_bibcodes: Set[str], 
                              max_similar_papers: int = 100) -> Dict:
    """
    Analyze how many papers from a comparison set appear as similar to a reference paper.
    
    Args:
        reference_bibcode: The bibcode to find similar papers for
        comparison_bibcodes: Set of bibcodes to check for overlap
        max_similar_papers: Maximum number of similar papers to retrieve
        
    Returns:
        Dictionary with overlap analysis results
    """
    if not ADS_API_TOKEN:
        print("❌ Error: ADS_API_TOKEN not found in environment variables")
        return {}
    
    print(f"🔍 Analyzing similarity overlap for reference: {reference_bibcode}")
    print(f"   Comparison set size: {len(comparison_bibcodes)}")
    print(f"   Max similar papers to check: {max_similar_papers}")
    
    # Find similar papers
    similar_result = find_similar_papers(reference_bibcode, max_results=max_similar_papers)
    
    if not similar_result:
        return {"error": "Failed to find similar papers"}
    
    # Extract bibcodes from similar papers
    similar_bibcodes = {paper["bibcode"] for paper in similar_result["papers"]}
    
    # Calculate overlap
    overlap_bibcodes = similar_bibcodes.intersection(comparison_bibcodes)
    
    overlap_percentage = (len(overlap_bibcodes) / len(comparison_bibcodes)) * 100 if comparison_bibcodes else 0
    similarity_coverage = (len(overlap_bibcodes) / len(similar_bibcodes)) * 100 if similar_bibcodes else 0
    
    results = {
        "reference_bibcode": reference_bibcode,
        "comparison_set_size": len(comparison_bibcodes),
        "similar_papers_found": len(similar_bibcodes),
        "overlap_count": len(overlap_bibcodes),
        "overlap_bibcodes": list(overlap_bibcodes),
        "overlap_percentage": overlap_percentage,
        "similarity_coverage": similarity_coverage,
        "similar_bibcodes": list(similar_bibcodes),
        "all_similar_papers": similar_result["papers"]
    }
    
    print(f"✅ Analysis completed:")
    print(f"   Similar papers found: {len(similar_bibcodes)}")
    print(f"   Overlap with comparison set: {len(overlap_bibcodes)}/{len(comparison_bibcodes)} ({overlap_percentage:.1f}%)")
    print(f"   Coverage of similar papers: {len(overlap_bibcodes)}/{len(similar_bibcodes)} ({similarity_coverage:.1f}%)")
    
    return results
