from pathlib import Path
import pandas as pd
import re 
from tqdm import tqdm

import requests
from urllib.request import urlopen
from zipfile import ZipFile, BadZipFile
from io import BytesIO
from datetime import datetime
from enum import Enum

import logging 
import configparser
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


class SECFileType(Enum):
    TEN_K = '10-K'
    TEN_Q = '10-Q'
    EIGHT_K = '8-K'
    THIRTEEN_F = '13F-HR'
    SCHEDULE_13D = 'SC 13D'
    SCHEDULE_13G = 'SC 13G'
    FORM_3 = '3'
    FORM_4 = '4'
    FORM_5 = '5'
    FORM_S_1 = 'S-1'
    FORM_144 = '144'

    @classmethod
    def from_int(cls, value):
        # Mapping from integers to SECFileType enum members
        mapping = {
            1: cls.TEN_K,
            2: cls.TEN_Q,
            3: cls.EIGHT_K,
            4: cls.THIRTEEN_F,
            5: cls.SCHEDULE_13D,
            6: cls.SCHEDULE_13G,
            7: cls.FORM_3,
            8: cls.FORM_4,
            9: cls.FORM_5,
            10: cls.FORM_S_1,
            11: cls.FORM_144,
        }

        return mapping.get(value)

    @classmethod
    def from_string(cls, value):
        # Mapping from strings to SECFileType enum members
        mapping = {
            '10-K': cls.TEN_K,
            '10-Q': cls.TEN_Q,
            '8-K': cls.EIGHT_K,
            '13F-HR': cls.THIRTEEN_F,
            'SC 13D': cls.SCHEDULE_13D,
            'SC 13G': cls.SCHEDULE_13G,
            '3': cls.FORM_3,
            '4': cls.FORM_4,
            '5': cls.FORM_5,
            'S-1': cls.FORM_S_1,
            '144': cls.FORM_144,
        }

        return mapping.get(value)

class SECDataDownloader:
    """
    class FilingType(Enum):
        _10K = auto()
        _10Q = auto()
        _8K = auto()
    """

    def __init__(self, data_path='./sec-data/data', start_date='1993-01-01', end_date='2023-06-30', user_agent=""):
        self.validate_user_agent(user_agent)
        self.validate_dates(start_date, end_date)

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

    def validate_dates(self, start_date_str, end_date_str):
        start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
        end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

        if start_date < datetime(1993, 1, 1) or start_date > datetime.now():
            raise ValueError("Invalid start date. It should be between 1993-01-01 and the current date.")

        if end_date < start_date or end_date > datetime.now(): 
            raise ValueError("Invalid end date. It should be between start_date and the current date.")


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
            self.logger.info(f"Downloading master index file for {yr}_{qr}")

            path = self.sec_path / f'{yr}_{qr}' / 'source'
            if not path.exists():
                path.mkdir(parents=True)
            else: 
                master_idx_file = path / 'master.idx'
                if master_idx_file.exists(): 
                    self.logger.info(f"Master index file already exists for {yr}_{qr}")
                    continue

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

            local_file = path / 'master.idx'
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


    def extract_and_save_csv(self):
        for (yr, qr) in tqdm(self.filing_periods, desc="Extracting and Saving CSV"):
            path = self.sec_path / f'{yr}_{qr}' / 'source'
            if not path.exists():
                continue

            file = "master.idx"
            local_file = path / 'master.idx'
            output_file = path / 'master.csv'

            if output_file.exists():
                print(f"CSV file already exists in {path}")
                continue

            self.logger.info(f"Parsing SEC index data for {yr}_{qr} into a csv")
            
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
            df['Downloaded'] = False
            df.to_csv(output_file, index=False)


    def download_filings(self, filing_type: SECFileType):
        """
        for (yr, qr) in tqdm(self.filing_periods[0:1], desc="Filtering and Downloading 10-K"):
            print(yr,qr)
        
        return 
        """
        for (yr, qr) in tqdm(self.filing_periods, desc=f"Filtering and Downloading {filing_type.value}"):
            path = self.sec_path / f'{yr}_{qr}' / 'source'
            if not path.exists():
                continue

            csv_file_path = path / 'master.csv'
            df = pd.read_csv(csv_file_path)
            sec_filing = df[(df['Form Type'] == filing_type.value) & (df['Downloaded'] == False)]
    
            self.logger.info(f"downloading {filing_type.value} filings from {yr} Q{qr}")
            path = self.sec_path / f'{yr}_{qr}' / filing_type.value
            
            if not path.exists():
                path.mkdir(parents=True)
            else:
                continue


            with ThreadPoolExecutor(max_workers=5) as executor:
                futures = [
                    executor.submit(self.download_sec_file, file, path)
                    for file in sec_filing['Filename']
                ]

                # Wait for all tasks to complete
                wait(futures)


    def download_sec_file(self, file, path):
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


    def validated_downloaded_filings(self, filing_type: SECFileType):
        for (yr, qr) in tqdm(self.filing_periods, desc=f"Validating {filing_type.value}"):
            path = self.sec_path / f'{yr}_{qr}' / 'source'
            if not path.exists():
                continue

            csv_file_path = path / 'master.csv'
            df = pd.read_csv(csv_file_path)
            sec_filing = df[df['Form Type'] == filing_type.value]

            path = self.sec_path / f'{yr}_{qr}' / filing_type.value
            
            if not path.exists():
                continue

            for index, row in df.iterrows():
                filename = rowp['Filename']
                file_path = path / filename
                file_downloaded = os.path.exists(file_path)

                df.at[index, 'Downloaded'] = file_downloaded
                        
            
            not_downloaded_count = len(df[df['Downloaded'] == False])
            total_count = len(df)

            self.logger.info(f"{yr}-{qr}: Number of files not downloaded: {not_downloaded_count} out of {total_count}")

            df.to_csv(output_file, index=False)


    def initialize_project(self):
        self.create_data_path()
        self.download_master_files()
        self.extract_and_save_csv()        



"""
main runner to execute program
"""
def main():
    config = configparser.ConfigParser()
    config.read("config.ini")

    downloader = SECDataDownloader(
        data_path=config.get("SECDataDownloader", "data_path"),
        start_date=config.get("SECDataDownloader", "start_date"),
        end_date=config.get("SECDataDownloader", "end_date"),
        user_agent=config.get("SECDataDownloader", "user_agent")
    )

    downloader.create_data_path()

    filing_type = config.get("SECDataDownloader", "filing_type")
    if filing_type.isdigit():
        filing_type_enum = SECFileType.from_int(int(filing_type))
    else:
        filing_type_enum = SECFileType.from_string(filing_type)
    
    if filing_type_enum is None:
        raise ValueError(f"Invalid filing type: {filing_type}. Choose from {', '.join([ftype.value for ftype in SECFileType])}")
 

    if config.getboolean("SECDataDownloader", "download_master"):
        downloader.download_master_files()

    if config.getboolean("SECDataDownloader", "process_master"):
        downloader.extract_and_save_csv()

    if config.getboolean("SECDataDownloader", "download_filings"):
        downloader.download_filings(filing_type=filing_type_enum)

    if config.getboolean("SECDataDownloader", "validate_downloads"):
        downloader.validated_downloaded_filings(filing_type=filing_type_enum)
 
if __name__ == "__main__":
    main()


