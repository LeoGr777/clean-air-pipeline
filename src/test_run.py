import requests
import logging
import time
import os
from dotenv import load_dotenv

load_dotenv()



def fetch_sequentially_test(urls: list[str], url_params: str, requests_per_minute: int = 60) -> list[dict]:
    """
    Fetches data from a list of URLs sequentially, one by one,
    respecting a defined rate limit.
    """
    API_KEY = os.getenv("API_KEY")
    if not API_KEY:
        logging.error("API_KEY not found in environment.")
        raise RuntimeError("API_KEY must be set.")

    headers = {"accept": "application/json", "X-API-Key": API_KEY}
    all_results = []
    
    # Calculate the delay needed between each request
    delay = 60.0 / requests_per_minute

    # Loop through each URL one by one
    for i, url in enumerate(urls, 1):
        logging.info(f"Requesting {i}/{len(urls)}: {url}")
        try:
            response = requests.get(url, headers=headers, timeout=30, params = url_params)
            response.raise_for_status()
            all_results.append(response.json())
        except requests.exceptions.RequestException as e:
            logging.error(f"Request failed for {url}: {e}")
        
        # Wait for the calculated delay before the next request
        logging.info(f"Waiting for {delay:.2f} seconds...")
        time.sleep(delay)

    logging.info(f"Successfully fetched data from {len(all_results)} URLs.")
    return all_results