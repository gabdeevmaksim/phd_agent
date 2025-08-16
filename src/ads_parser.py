"""
ADS NASA Parser - Clean implementation with connection testing
"""

import requests
import os
from dotenv import load_dotenv
import time

# Load environment variables
load_dotenv()

# Configuration
ADS_API_TOKEN = os.getenv("ADS_API_TOKEN")
ADS_API_BASE_URL = "https://api.adsabs.harvard.edu/v1"

def test_ads_connection():
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

def get_ads_headers():
    """
    Get the headers needed for ADS API requests.
    
    Returns:
        dict: Headers with authorization token
    """
    if not ADS_API_TOKEN:
        raise ValueError("ADS_API_TOKEN not found in environment variables")
    
    return {"Authorization": f"Bearer {ADS_API_TOKEN}"}

def get_paper_info(bibcode, show_abstract=True):
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

def get_abstract(bibcode):
    """
    Retrieve only the abstract of a paper using its bibcode.
    
    Args:
        bibcode (str): The bibcode of the paper to retrieve
        
    Returns:
        str: Abstract text or None if not found
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

def get_bulk_paper_info(bibcodes, show_abstracts=False, batch_size=50):
    """
    Retrieve information for multiple papers using their bibcodes in batches.
    
    Args:
        bibcodes (list): List of bibcodes to retrieve
        show_abstracts (bool): Whether to display abstracts in output
        batch_size (int): Number of bibcodes per request (recommended: 50-100)
        
    Returns:
        dict: Dictionary with bibcode as key and paper info as value
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

def download_catalogue_abstracts(csv_file_path, output_json_path, batch_size=50):
    """
    Download abstracts for all papers in a catalogue and save to JSON.
    
    Args:
        csv_file_path (str): Path to the CSV file containing bibcodes
        output_json_path (str): Path where to save the JSON output
        batch_size (int): Number of bibcodes per batch (default: 50)
        
    Returns:
        dict: Results with title and abstract for each bibcode
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
