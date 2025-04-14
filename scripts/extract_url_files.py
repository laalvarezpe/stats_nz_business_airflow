import json
import requests
from bs4 import BeautifulSoup
import os
import zipfile
import logging
import chardet
import pandas as pd
from io import BytesIO
import openpyxl

base_url = "https://www.stats.govt.nz"
url = "https://www.stats.govt.nz/large-datasets/csv-files-for-download/"
download_dir = "../data"
output_path = "../data/output"


logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")

def download_page(url):
    try:
        page = requests.get(url)
        page.raise_for_status()
        return page.content
    except requests.exceptions.RequestException as e:
        logging.error(f"error accessing page: {e}")
        return None

def extract_zip(zip_data, extract_to):
    try:
        with zipfile.ZipFile(BytesIO(zip_data)) as zip_file:
            zip_file.extractall(extract_to)
            logging.info(f"files extracted to: {extract_to}")
    except zipfile.BadZipFile:
        logging.error("file is not a valid zip.")
        return False
    return True

def convert_to_utf8(file_path):
    """convert a single csv file to utf-8"""
    if file_path.endswith(".csv"):
        #print(f"\n processing: {os.path.basename(file_path)}")
        try:
            with open(file_path, "rb") as f:
                raw_data = f.read()
                detected = chardet.detect(raw_data)
                encoding = detected["encoding"]
                #print(f"detected encoding: {encoding}")

            try:
                df = pd.read_csv(file_path, encoding=encoding)
                df.to_csv(file_path, index=False, encoding="utf-8")
                #print("rewritten with pandas in utf-8")
            except Exception as e:
                #print(f"⚠️ pandas error: {e}")
                #print("➡️ trying manual read with error replacement...")
                text = raw_data.decode(encoding or "utf-8", errors="replace")
                with open(file_path, "w", encoding="utf-8") as f:
                    f.write(text)
                #print("manually rewritten in utf-8")
        except Exception as final_error:
            print(f"failed to process {file_path}: {final_error}")

def convert_xlsx_to_csv(xlsx_path):
    # convert xlsx to csv and remove original
    try:
        df = pd.read_excel(xlsx_path, engine='openpyxl')
        csv_path = xlsx_path.replace(".xlsx", ".csv")
        df.to_csv(csv_path, index=False, encoding="utf-8")
        os.remove(xlsx_path)
        logging.info(f"xlsx file converted to csv: {csv_path}")
    except Exception as e:
        logging.error(f"error converting {xlsx_path} to csv: {e}")

def process_zip_file(zip_url, download_dir):
    response = requests.get(zip_url)
    if response.status_code == 200:
        zip_data = response.content
        if extract_zip(zip_data, download_dir):
            for file_name in os.listdir(download_dir):
                file_path = os.path.join(download_dir, file_name)
                if file_name.endswith(".csv"):
                    convert_to_utf8(file_path)
                elif file_name.endswith(".xlsx"):
                    convert_xlsx_to_csv(file_path)
    else:
        logging.error(f"error downloading zip file: {zip_url}")

def process_single_file(file_url, download_dir):
    response = requests.get(file_url)
    if response.status_code == 200:
        file_name = os.path.basename(file_url)
        file_path = os.path.join(download_dir, file_name)
        with open(file_path, "wb") as f:
            f.write(response.content)

        if file_name.endswith(".csv"):
            convert_to_utf8(file_path)
        elif file_name.endswith(".xlsx"):
            convert_xlsx_to_csv(file_path)
    else:
        logging.error(f"error downloading file: {file_url}")

def extract_url_files():
    os.makedirs(download_dir, exist_ok=True)

    page_content = download_page(url)
    if not page_content:
        return []

    soup = BeautifulSoup(page_content, "html.parser")
    data_value = soup.find("div", id="pageViewData")["data-value"]
    data = json.loads(data_value)

    list_urls = []
    for block in data['PageBlocks']:
        if block['Title'] == 'Business':
            for document in block['BlockDocuments']:
                file_url = base_url + document['DocumentLink']
                list_urls.append(file_url)
                if file_url.endswith(".zip"):
                    process_zip_file(file_url, download_dir)
                else:
                    process_single_file(file_url, download_dir)

    logging.info(f"processed links: {len(list_urls)}")
    print(list_urls)
    return list_urls

if __name__ == "__main__":
    extract_url_files()



# handle formatting of this
