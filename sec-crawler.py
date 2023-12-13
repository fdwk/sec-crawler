from pathlib import Path
import pandas as pd
import re 
from tqdm import tqdm

import requests
from urllib.request import urlopen
from zipfile import ZipFile, BadZipFile
from io import BytesIO

import logging 
import time

from collections.abc import Iterator
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, wait

import json
from pprint import pprint
import warnings
warnings.filterwarnings('ignore')


class RateLimiter(Iterator):
    """Iterator that yields a value at most once every 'interval' seconds."""
    def __init__(self, interval):
        self.lock = Lock()
        self.interval = interval
        self.next_yield = 0

    def __next__(self):
        with self.lock:
            t = time.monotonic()
            if t < self.next_yield:
                time.sleep(self.next_yield - t)
                t = time.monotonic()
            self.next_yield = t + self.interval

    next = __next__


class SECDataDownloader:
    """
    class FilingType(Enum):
        _10K = auto()
        _10Q = auto()
        _8K = auto()
    """

    def __init__(self, data_path='./sec-data/data', start_date='1993-01-01', end_date='2023-06-30', user_agent=""):
        self.validate_user_agent(user_agent)
        
        self.sec_path = Path(data_path)
        self.start_date = start_date
        self.end_date = end_date
        self.sec_url = 'https://www.sec.gov'
        self.headers = {'User-Agent': user_agent}
        self.filing_periods = self.generate_filing_periods()
        self.logger = self.setup_logging()
        self.cache_expiry = 600  # Cache expiry time in seconds (adjust as needed)
        self.rate_limit_sleep = 0.1  # Sleep duration for rate limit (adjust as needed)
        self.rate_limiter = RateLimiter(0.1)

    def validate_user_agent(self, user_agent):
        if not user_agent or not re.match(r"[^@]+@[^@]+\.[^@]+", user_agent):
            raise ValueError("User agent must be a valid email address. Please provide a valid user agent.")

    def generate_filing_periods(self):
        return [(d.year, d.quarter) for d in pd.date_range(self.start_date, self.end_date, freq='Q')]

    def setup_logging(self):
        logger = logging.getLogger(__name__)
        logger.setLevel(logging.DEBUG)
        formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
        stream_handler = logging.StreamHandler()
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)
        return logger

    def create_data_path(self):
        if not self.sec_path.exists():
            self.sec_path.mkdir(parents=True)

    def download_master_files(self):
        self.logger.info(f"Downloading master index files from SEC Edgar...")

        for (yr, qr) in tqdm(self.filing_periods, desc="Downloading Master Files"):
            path = self.sec_path / f'{yr}_{qr}' / 'source'
            if not path.exists():
                path.mkdir(parents=True)

            file = "master.idx"
            local_file = path / file
            src = f"{self.sec_url}/Archives/edgar/full-index/{yr}/QTR{qr}/{file}"

            try:
                self.logger.info(f"Downloading {yr} Q{qr} master index")
                response = requests.get(src, headers=self.headers)

                if response.status_code == 200:
                    with local_file.open('wb') as output_file:
                        output_file.write(response.content)
                else:
                    self.logger.error(f"Failed to download the file. Status code: {response.status_code}")

                # avoid rate limiting
                time.sleep(0.1)

            except Exception as e:
                self.logger.error(f"Failed {e}")

    def process_master_files(self): 
        for path in self.sec_path.glob('*/*/source'):
            if not path.is_dir():
                continue

            local_file = path / 'master.txt'
            output_file = path / 'master.csv'

            if output_file.exists():
                print(f"CSV file already exists in {path}")
                continue
            
            
            with open(local_file, 'r', encoding='UTF-8', errors='ignore') as file:
                content = file.read()

            lines = content.split('\n')[11:]

            pattern = re.compile(r'(\d+)\|([^|]+)\|([^|]+)\|(\d{4}-\d{2}-\d{2})\|([^|\n]+)')

            matches = []
            non_matches = []

            for line in lines:
                match = pattern.match(line)
                if match:
                    matches.append(match.groups())
                if not match:
                    non_matches.append(line)

            df = pd.DataFrame(matches, columns=['CIK', 'Company Name', 'Form Type', 'Date Filed', 'Filename'])
            df.to_csv(output_file, index=False)

            #yr, qr = path.parts[-2].split('_')
            #self.process_filings(yr, qr)


    def extract_and_save_csv(self):
        for (yr, qr) in tqdm(self.filing_periods, desc="Extracting and Saving CSV"):
            path = self.sec_path / f'{yr}_{qr}' / 'source'
            if not path.exists():
                continue

            file = "master.idx"
            local_file = path / 'master.idx'
            output_file = path / 'master.csv'

            with open(local_file, 'r', encoding='UTF-8', errors='ignore') as file:
                content = file.read()

            lines = content.split('\n')[11:]

            pattern = re.compile(r'(\d+)\|([^|]+)\|([^|]+)\|(\d{4}-\d{2}-\d{2})\|([^|\n]+)')

            matches = []
            non_matches = []

            for line in lines:
                match = pattern.match(line)
                if match:
                    matches.append(match.groups())
                if not match:
                    non_matches.append(line)

            df = pd.DataFrame(matches, columns=['CIK', 'Company Name', 'Form Type', 'Date Filed', 'Filename'])
            df.to_csv(output_file, index=False)

    def download_filings(self):
        """
        for (yr, qr) in tqdm(self.filing_periods[0:1], desc="Filtering and Downloading 10-K"):
            print(yr,qr)
        
        return 
        """
        
        for (yr, qr) in tqdm(self.filing_periods[-5:], desc="Filtering and Downloading 10-K"):
            path = self.sec_path / f'{yr}_{qr}' / 'source'
            if not path.exists():
                continue

            csv_file_path = path / 'master.csv'
            df = pd.read_csv(csv_file_path)
            ten_k = df[df['Form Type'] == '10-K']
    
            self.logger.info(f"downloading 10-K filings from {yr} Q{qr}")
            path = self.sec_path / f'{yr}_{qr}' / '10-K'
            if not path.exists():
                path.mkdir(parents=True)

            """
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = [
                    executor.submit(print, file)
                    for file in ten_k['Filename']
                ]
                wait(futures)"""
      
            
            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = [
                    executor.submit(self.download_10k_file, file, path)
                    for file in ten_k['Filename']
                ]

                # Wait for all tasks to complete
                wait(futures)

    def download_10k_file(self, file, path):
        next(self.rate_limiter)

        file_url = f"https://www.sec.gov/Archives/{file}"
        local_file = path / file.split('/')[-1]

        try:
            response = requests.get(file_url, headers=self.headers)

            if response.status_code == 200:
                with open(local_file, 'wb') as file:
                    file.write(response.content)
            else:
                print(f"Failed to download {file}. Status code: {response.status_code}")

        except Exception as e:
            print(f"Failed to download {file}. {e}")

    def initialize_project(self):
        self.create_data_path()
        self.download_master_files()
        self.extract_and_save_csv()        

SEC_downloader = SECDataDownloader(user_agent="kahn7770@mylaurier.ca")
SEC_downloader.download_filings()
