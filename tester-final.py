import json
import requests
from bs4 import BeautifulSoup
import random
import csv
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from playwright.sync_api import sync_playwright
from playwright_stealth import stealth_sync
from dataclasses import dataclass, fields, asdict

@dataclass
class SiteResult:
    url: str = ""
    category: str = None
    pings_without_useragent: int = 0
    pings_with_useragent: int = 0
    pings_with_browseragent: int = 0
    pings_with_playwright: int = 0
    pings_with_stealth: int = 0
    status_naked: int = -1
    status_with_useragent: int = -1
    status_with_browseragent: int = -1
    status_with_playwright: int = -1
    status_with_stealth: int = -1
    time_taken_naked: float = 0
    time_taken_useragent: float = 0
    time_taken_browseragent: float = 0
    time_taken_playwright: float = 0
    time_taken_stealth: float = 0

class DataPipeline:
    def __init__(self, csv_filename="", storage_queue_limit=50):
        self.names_seen = []
        self.storage_queue = []
        self.storage_queue_limit = storage_queue_limit
        self.csv_filename = csv_filename
        self.csv_file_open = False

    def save_to_csv(self):
        self.csv_file_open = True
        data_to_save = list(self.storage_queue)
        self.storage_queue.clear()
        if not data_to_save:
            return

        keys = [field.name for field in fields(data_to_save[0])]
        mode = "a"
        file_exists = os.path.isfile(self.csv_filename)

        with open(self.csv_filename, mode=mode, newline="", encoding="utf-8") as output_file:
            writer = csv.DictWriter(output_file, fieldnames=keys)
            if not file_exists:
                writer.writeheader()
            for item in data_to_save:
                writer.writerow(asdict(item))

        self.csv_file_open = False

    def is_duplicate(self, input_data):
        if input_data.url in self.names_seen:
            return True
        self.names_seen.append(input_data.url)
        return False

    def add_data(self, scraped_data):
        if not self.is_duplicate(scraped_data):
            self.storage_queue.append(scraped_data)
            if len(self.storage_queue) >= self.storage_queue_limit and not self.csv_file_open:
                self.save_to_csv()

    def close_pipeline(self):
        if self.csv_file_open:
            time.sleep(3)
        if len(self.storage_queue) > 0:
            self.save_to_csv()

# Define a global variable for the number of requests per test
MAX_PINGS = 100
SOPS_API_KEY = "YOUR-SCRAPEOPS-API-KEY"

def get_proxy_with_session(session_range_start, session_range_end):
    session_id = random.randint(session_range_start, session_range_end)
    return {
        "http": f"http://scrapeops.country=us.sticky_session={session_id}:{SOPS_API_KEY}@residential-proxy.scrapeops.io:8181",
        "https": f"http://scrapeops.country=us.sticky_session={session_id}:{SOPS_API_KEY}@residential-proxy.scrapeops.io:8181",
    }

from openai import OpenAI

# Set your OpenAI API key

def get_failed_test_string_reason(html_content):

    if html_content == "":
        return {"valid_response": False, "failed_validation_reason": "no_html_content"}
    
    ## Get <body> tag
    soup = BeautifulSoup(html_content, 'html.parser')
    body_tag = soup.find('body')
    if body_tag is None:
        return {"valid_response": False, "failed_validation_reason": "no_body_tag"}
    
    ## Body Text
    body_text = body_tag.get_text()
    if body_text == "":
        return {"valid_response": False, "failed_validation_reason": "no_body_text"}
    

    # Construct a prompt for ChatGPT
    prompt = ("""
    You are an AI web scraper model that analyzes the HTML content and determine if the page is a valid page and if not return a reason for why the page failed to a validation check.     You will analyze the HTML content and return a reason for why the page failed to a validation check. From the following list of reasons:
    js_rendering_required, login_page, ban_page, captcha_page, automated_access_denied, rate_limiting, no_body, 404_page, other
              
    Return your data in the following JSON format:
    {
        "valid_response": "boolean" // true if the page is a valid response, false if it is not
        "failed_validation_reason": "string" // the reason the page is not a valid response from the list of reasons above. Do not make this up, only use the list of reasons above.
    }

    HTML CONTENT:

""" + body_text[:100000])

    try:

        

        client = OpenAI(
            api_key="YOUR-OPENAI-API-KEY",  # Replace with your actual API key
        )
        response = client.chat.completions.create(
            model="gpt-4o",
            messages=[
                {"role": "system", "content": "You are a web scraping model that validates the HTML content of a website and returns a reason for why the page failed a validation check."},
                {"role": "user", "content": str(prompt)},  # Ensure prompt is a string
            ]
        )


        # Extract response content safely
        response_text = response.choices[0].message.content

        cleaned_response_text = response_text.strip()
        cleaned_response_text = cleaned_response_text.replace("```json", "").replace("```", "")

        # Parse the response text as JSON
        validation_result = json.loads(cleaned_response_text)

        return validation_result

    except Exception as e:
        print(f"An error occurred: {e}")
        return None

def test_string_check(response_text, test_string_list_string):
    if test_string_list_string == "":
        return True
    test_string_list = test_string_list_string.split(" || ")
    for test_string in test_string_list:
        if test_string in response_text:
            return True
    return False

def test_naked(url, proxies, test_string):
    start_time = time.time()
    pings = 0
    error_count = 0
    status = -1
    chatgpt_time = 0  # Track ChatGPT validation tim
    while pings < MAX_PINGS:
        try:
            response = requests.get(url, proxies=proxies, verify=False)
            status = response.status_code
            if test_string_check(response.text, test_string) == False:
                validation_start = time.time()
                validation_result = get_failed_test_string_reason(response.text)
                chatgpt_time += time.time() - validation_start
                if validation_result["valid_response"] == False:
                    status = validation_result["failed_validation_reason"]
                    break
            if status != 200:
                break
            pings += 1
            # time.sleep(random.uniform(0.1, 1))
        except Exception as e:
            print(f"Error in test_naked: {e}")
            error_count += 1
            status = "error_no_response"
            break
    end_time = time.time()
    total_time = end_time - start_time - chatgpt_time  # Subtract ChatGPT time
    return pings, status, total_time

def test_with_useragent(url, proxies, headers, test_string):
    start_time = time.time()
    pings = 0
    error_count = 0
    status = -1
    chatgpt_time = 0  # Track ChatGPT validation time
    while pings < MAX_PINGS:
        try:
            response = requests.get(url, proxies=proxies, headers=headers, verify=False)
            status = response.status_code
            if test_string_check(response.text, test_string) == False:
                validation_start = time.time()
                validation_result = get_failed_test_string_reason(response.text)
                chatgpt_time += time.time() - validation_start
                if validation_result["valid_response"] == False:
                    status = validation_result["failed_validation_reason"]
                    break
            if status != 200:
                break
            pings += 1
            # time.sleep(random.uniform(0.1, 1))
        except Exception as e:
            print(f"Error in test_with_useragent: {e}")
            error_count += 1
            status = "error_no_response"
            break
    end_time = time.time()
    total_time = end_time - start_time - chatgpt_time  # Subtract ChatGPT time
    return pings, status, total_time

def test_with_browseragent(url, proxies, browser_agent, test_string):
    start_time = time.time()
    pings = 0
    error_count = 0
    status = -1
    chatgpt_time = 0  # Track ChatGPT validation time
    while pings < MAX_PINGS:
        try:
            response = requests.get(url, proxies=proxies, headers=browser_agent, verify=False)
            status = response.status_code
            if test_string_check(response.text, test_string) == False:
                validation_start = time.time()
                validation_result = get_failed_test_string_reason(response.text)
                chatgpt_time += time.time() - validation_start
                if validation_result["valid_response"] == False:
                    status = validation_result["failed_validation_reason"]
                    break
            if status != 200:
                break
            pings += 1
            # time.sleep(random.uniform(0.1, 1))
        except Exception as e:
            print(f"Error in test_with_browseragent: {e}")
            error_count += 1
            status = "error_no_response"
            break
    end_time = time.time()
    total_time = end_time - start_time - chatgpt_time  # Subtract ChatGPT time
    return pings, status, total_time

# Commented out Playwright functionality
def test_with_playwright(url, proxies, test_string):
    start_time = time.time()
    pings = 0
    error_count = 0
    status = -1
    chatgpt_time = 0  # Track ChatGPT validation time
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        page.set_extra_http_headers(proxies)
        while pings < MAX_PINGS:
            try:
                response = page.goto(url, wait_until="networkidle")
                status = response.status
                if test_string_check(response.text(), test_string) == False:
                    validation_start = time.time()
                    validation_result = get_failed_test_string_reason(response.text())
                    chatgpt_time += time.time() - validation_start
                    if validation_result["valid_response"] == False:
                        status = validation_result["failed_validation_reason"]
                        break
                if status != 200:
                    break
                pings += 1
                # time.sleep(random.uniform(0.1, 1))
            except Exception as e:
                print(f"Error in test_with_playwright: {e}")
                error_count += 1
                status = "error_no_response"
                break
        browser.close()
    end_time = time.time()
    total_time = end_time - start_time - chatgpt_time  # Subtract ChatGPT time
    return pings, status, total_time

def test_with_stealth(url, proxies, test_string):
    start_time = time.time()
    pings = 0
    error_count = 0
    status = -1
    chatgpt_time = 0  # Track ChatGPT validation time
    with sync_playwright() as p:
        browser = p.chromium.launch()
        page = browser.new_page()
        stealth_sync(page)
        page.set_extra_http_headers(proxies)
        while pings < MAX_PINGS:
            try:
                response = page.goto(url, wait_until="networkidle")
                status = response.status
                if test_string_check(response.text(), test_string) == False:
                    validation_start = time.time()
                    validation_result = get_failed_test_string_reason(response.text())
                    chatgpt_time += time.time() - validation_start
                    if validation_result["valid_response"] == False:
                        status = validation_result["failed_validation_reason"]
                        break
                if status != 200:
                    break
                pings += 1
                # time.sleep(random.uniform(0.1, 1))
            except Exception as e:
                print(f"Error in test_with_playwright: {e}")
                error_count += 1
                status = "error_no_response"
                break
        browser.close()
    end_time = time.time()
    total_time = end_time - start_time - chatgpt_time  # Subtract ChatGPT time
    return pings, status, total_time

def test_website(website, datapipeline):
    url = website["site_name"]
    category = website["category"]
    test_string = website["test_string"]

    # Generate proxies with unique session ranges for each test type
    proxies_naked = get_proxy_with_session(0, 2000)
    proxies_useragent = get_proxy_with_session(2000, 4000)
    proxies_browseragent = get_proxy_with_session(4000, 6000)
    proxies_playwright = get_proxy_with_session(6000, 8000)
    proxies_stealth = get_proxy_with_session(8000, 10000)

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36"
    }

    browser_agent = {
        "Connection": "keep-alive",
        "Cache-Control": "max-age=0",
        "User-Agent": headers["User-Agent"],
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Accept-Language": "en-US,en;q=0.9",
    }

    with ThreadPoolExecutor(max_workers=4) as executor:
        futures = {
            executor.submit(test_naked, url, proxies_naked, test_string ): "naked",
            executor.submit(test_with_useragent, url, proxies_useragent, headers, test_string): "useragent",
            executor.submit(test_with_browseragent, url, proxies_browseragent, browser_agent, test_string): "browseragent",
            executor.submit(test_with_playwright, url, proxies_playwright, test_string): "playwright",
            executor.submit(test_with_stealth, url, proxies_stealth, test_string): "stealth"
        }

        results = {key: None for key in futures.values()}
        for future in as_completed(futures):
            test_type = futures[future]
            try:
                pings, status, time_taken = future.result()
                results[test_type] = (pings, status, time_taken)
            except Exception as e:
                print(f"Error in {test_type} test: {e}")

    test_result = SiteResult(
        url=url,
        category=category,
        pings_without_useragent=results["naked"][0],
        pings_with_useragent=results["useragent"][0],
        pings_with_browseragent=results["browseragent"][0],
        pings_with_playwright= results["playwright"][0],
        pings_with_stealth=results["stealth"][0],
        status_naked=results["naked"][1],
        status_with_useragent=results["useragent"][1],
        status_with_browseragent=results["browseragent"][1],
        status_with_playwright=results["playwright"][1],
        status_with_stealth=results["stealth"][1],
        time_taken_naked=results["naked"][2],
        time_taken_useragent=results["useragent"][2],
        time_taken_browseragent=results["browseragent"][2],
        time_taken_playwright=results["playwright"][2],
        time_taken_stealth=results["stealth"][2]
    )

    datapipeline.add_data(test_result)

if __name__ == "__main__":
    THREADS = 4

    with open("sites.csv") as csvfile:
        reader = list(csv.DictReader(csvfile))

        datapipeline = DataPipeline(csv_filename="results-with-playwright.csv")

        with ThreadPoolExecutor(max_workers=THREADS) as executor:
            executor.map(lambda website: test_website(website, datapipeline), reader)

        datapipeline.close_pipeline()