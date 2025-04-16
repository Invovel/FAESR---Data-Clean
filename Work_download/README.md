# FDA Drug Adverse Events URL Generator and File Checker

A Python-based system for generating FDA drug adverse events file URLs and checking downloaded files.

## Project Structure

- `download_files.py`: Generates URLs for FDA drug adverse events files (2014-2023)
- `download_check.py`: Checks downloaded files and logs missing ones
- `download_all_fda.py`: Downloads files in parallel batches using wget
- `all_urls.txt`: Contains all generated URLs
- `missing_files.txt`: Contains list of missing files

## Features

1. **URL Generation**
   - Generates URLs for FDA drug adverse events files from 2014 to 2023
   - Uses fixed file counts for each quarter based on historical data
   - Supports all quarters (Q1-Q4) for each year

2. **File Checking**
   - Validates downloaded files
   - Logs missing files to missing_files.txt
   - Provides detailed status of each file

3. **Parallel Downloading**
   - Downloads files in parallel batches of 5
   - Uses wget for reliable file downloads
   - Implements a 5-second delay between batches
   - Skips SSL certificate validation for better compatibility

## Requirements

- Python 3.6+
- requests package
- wget (must be installed and available in system PATH)

## Installation

1. Install Python dependencies:
   ```bash
   pip install requests
   ```

2. Install wget:
   - Windows: Download from [GNU Wget for Windows](https://eternallybored.org/misc/wget/)
   - Linux: `sudo apt-get install wget`
   - macOS: `brew install wget`

## Usage

1. Generate URLs and check files:
   ```bash
   python main.py
   ```

2. Download missing files:
   ```bash
   python download_all_fda.py
   ```

## Module Descriptions

### download_files.py
- `get_quarter_file_count(year, quarter)`: Returns the number of files for a given quarter
- `get_year_quarter_urls(year, quarter)`: Generates URLs for a specific quarter
- `get_all_urls()`: Generates URLs for all quarters from 2014 to 2023

### download_check.py
- `check_downloaded_files(urls)`: Checks if files are downloaded and logs missing ones

### download_all_fda.py
- `download_file(url, output_path)`: Downloads a single file using wget
- `download_files(urls, base_dir, batch_size)`: Downloads files in parallel batches

## Error Handling

- Handles missing dependencies gracefully
- Provides clear error messages
- Logs missing files for later processing
- Implements retry mechanism for failed downloads

## Notes

- Files are downloaded in parallel batches of 5 with a 5-second delay between batches
- All downloads are performed using wget for better reliability
- SSL certificate validation is skipped for better compatibility
- File counts are based on historical data from 2014 to 2023 