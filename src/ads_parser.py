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
