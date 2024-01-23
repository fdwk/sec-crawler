# SEC Crawler

## Overview
This repo contains a Python script for downloading and processing financial filings from the U.S. Securities and Exchange Commission (SEC) EDGAR database. This script uses a configuration file (config.ini) to parameterize its behavior.

### Prerequisites
Make sure you ahve the follonig installed
- Python (3.x recommended)
- Pip (Python package installer)

## Configuration
1. Open the `config.ini` file
2. Customize the following parameters: 
   - `data_path`: Path to the directory where SEC filings will be stored **default value is encouraged**.
   - `start_date`: Start date for fetching filings (format: 'YYYY-MM-DD').
   - `end_date`: End date for fetching filings (format: 'YYYY-MM-DD').
   - `user_agent`: Your email address as the user agent (required by SEC). 
3. Configure the run characteristics configs:

#### SEC `filing_type` : 


-    **10-K (1)**: Annual report filed by U.S. Securities and Exchange Commission (SEC).
-    **10-Q (2)**: Quarterly report filed by U.S. Securities and Exchange Commission (SEC).
-    **8-K (3)**: Report of unscheduled material events or corporate changes.
-    **13F-HR (4)**: Quarterly report filed by institutional investment managers.
-    **SC 13D (5)**: Statement of beneficial ownership of common stock by certain persons.
-    **SC 13G (6)**: Statement of beneficial ownership of common stock by certain persons.
-    **Form 3 (7)**: Initial statement of beneficial ownership of securities.
-    **Form 4 (8)**: Statement of changes in beneficial ownership of securities.
-    **Form 5 (9)**: Annual statement of beneficial ownership of securities.
-    **Form S-1 (10)**: Registration statement for securities.
-    **Form 144 (11)**: Notice of proposed sale of securities.

##### `download_master`
- Set true to download the quarterly `master.idx` files from SEC Edgar in the given date range
##### `process_master`
- Set true to parse `master.idx` files into a `.csv` table to initialize tracking. 
##### `download_filings`
- Set true to download all desired filing types in the given timeframe
##### `validate_downloads`
- Set true to track any missed filings and store them in the `.csv` table

