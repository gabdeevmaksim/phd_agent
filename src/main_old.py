# ADS NASA Parser Notebook

# Import necessary libraries
import requests
import os
import csv
import time

# Configuration
ADS_API_TOKEN = os.getenv("ADS_API_TOKEN")  # Retrieve the API token from the environment variable
KEYWORD = "spot"  # Replace with your keyword
OBJECT_NAMES_FILE = "data/classification_gaia_deb_new.csv"  # Replace with your file name
PDF_DOWNLOAD_DIR = "pdfs"  # Directory to save PDFs

# Function to read object names from a file
def read_object_names(filename):
    try:
        with open(filename, "r") as f:
            reader = csv.reader(f)
            return [row[0].strip() for row in reader if row]  # Read the first column
    except FileNotFoundError:
        print(f"Error: File '{filename}' not found.")
        return []
    except IndexError:
        print("Error: CSV file format is incorrect.")
        return []

# Function to query the ADS API
def query_ads_api(keyword, object_name):
    headers = {"Authorization": f"Bearer {ADS_API_TOKEN}"}
    query = f"full:{keyword} AND abs:{object_name}"
    params = {"q": query, "fl": "bibcode, pdf"}  # Request bibcode and pdf fields

    try:
        response = requests.get("https://api.adsabs.harvard.edu/v1/search/query", headers=headers, params=params)
        time.sleep(1)  # Add a 1-second delay to avoid overloading the server

        if response.status_code == 200:
            return response.json()
        else:
            print(f"Error: ADS API request failed with status code {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error: Failed to query ADS API: {e}")
        return None

# Function to download PDFs
def download_pdf(pdf_url, bibcode, download_dir, object_name):
    # Create a subdirectory for the object name
    object_dir = os.path.join(download_dir, object_name)
    if not os.path.exists(object_dir):
        os.makedirs(object_dir)

    try:
        response = requests.get(pdf_url)
        response.raise_for_status()  # Raise an exception for bad status codes

        filename = os.path.join(object_dir, f"{bibcode}.pdf")
        with open(filename, "wb") as f:
            f.write(response.content)
        print(f"Downloaded: {filename}")

        # Check if the file exists, retry if not
        retries = 5
        while retries > 0:
            if os.path.exists(filename):
                print(f"File exists: {filename}")
                break
            else:
                print(f"File not found, retrying... ({retries} retries left)")
                time.sleep(1)
                retries -= 1

        if retries == 0:
            print(f"Failed to verify the existence of the file: {filename}")
    except requests.exceptions.RequestException as e:
        print(f"Error downloading PDF: {e}")

# Main execution
def main():
    object_names = read_object_names(OBJECT_NAMES_FILE)

    if object_names:
        for object_name in object_names:
            api_response = query_ads_api(KEYWORD, object_name)

            if api_response and "response" in api_response and "docs" in api_response["response"]:
                for paper in api_response["response"]["docs"]:
                    if "pdf" in paper and paper["pdf"]:
                        pdf_url = paper["pdf"][0]  # Assuming only one PDF link
                        bibcode = paper["bibcode"]
                        download_pdf(pdf_url, bibcode, PDF_DOWNLOAD_DIR, object_name)

# Run the main function
if __name__ == "__main__":
    main()