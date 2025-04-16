import os
import time
import subprocess
from concurrent.futures import ThreadPoolExecutor
from typing import List, Tuple, Dict
from download_check import get_file_info
import sys

# Fixed file counts for each quarter
QUARTER_FILE_COUNTS = {
    '2014': {'q1': 20, 'q2': 16, 'q3': 17, 'q4': 17},
    '2015': {'q1': 25, 'q2': 23, 'q3': 32, 'q4': 24},
    '2016': {'q1': 28, 'q2': 23, 'q3': 23, 'q4': 23},
    '2017': {'q1': 26, 'q2': 25, 'q3': 26, 'q4': 26},
    '2018': {'q1': 28, 'q2': 25, 'q3': 25, 'q4': 25},
    '2019': {'q1': 28, 'q2': 25, 'q3': 25, 'q4': 25},
    '2020': {'q1': 28, 'q2': 25, 'q3': 25, 'q4': 25},
    '2021': {'q1': 28, 'q2': 25, 'q3': 25, 'q4': 25},
    '2022': {'q1': 34, 'q2': 31, 'q3': 32, 'q4': 34},
    '2023': {'q1': 34, 'q2': 31, 'q3': 32, 'q4': 34}
}

# Base directory for categorized downloads
BASE_DIR = r"D:\Download\Drug Adverse Events"

def count_files_in_directory(directory: str) -> int:
    """Count the number of files in a directory."""
    try:
        return len([f for f in os.listdir(directory) if os.path.isfile(os.path.join(directory, f))])
    except:
        return 0

def verify_group_download(year: str, quarter: str, group_number: int) -> bool:
    """
    Verify if all files in a group (3 files) have been downloaded.
    
    Args:
        year: Year to check
        quarter: Quarter to check
        group_number: Group number (1-based)
        
    Returns:
        bool: True if all files in the group are downloaded, False otherwise
    """
    quarter_dir = os.path.join(BASE_DIR, year, quarter)
    start_file = (group_number - 1) * 3 + 1
    end_file = min(group_number * 3, QUARTER_FILE_COUNTS[year][quarter])
    
    for i in range(start_file, end_file + 1):
        filename = f"drug-event-{i:04d}-of-{QUARTER_FILE_COUNTS[year][quarter]:04d}.json.zip"
        file_path = os.path.join(quarter_dir, filename)
        if not os.path.exists(file_path):
            return False
    return True

def wait_for_group_completion(year: str, quarter: str, group_number: int, max_wait_time: int = 300) -> bool:
    """
    Wait until all files in a group are downloaded.
    
    Args:
        year: Year to check
        quarter: Quarter to check
        group_number: Group number (1-based)
        max_wait_time: Maximum time to wait in seconds
        
    Returns:
        bool: True if all files were downloaded, False if timeout
    """
    start_time = time.time()
    while time.time() - start_time < max_wait_time:
        if verify_group_download(year, quarter, group_number):
            return True
        print(f"Waiting for group {group_number} to complete...")
        time.sleep(30)
    return False

def download_file(url: str, output_path: str, max_retries: int = 3) -> bool:
    """
    Download a single file using PowerShell's Invoke-WebRequest with retries
    
    Args:
        url: URL of the file to download
        output_path: Path where the file should be saved
        max_retries: Maximum number of download attempts
        
    Returns:
        bool: True if download was successful, False otherwise
    """
    for attempt in range(max_retries):
        try:
            # Create directory if it doesn't exist
            os.makedirs(os.path.dirname(output_path), exist_ok=True)
            
            # Download using PowerShell's Invoke-WebRequest with headers
            headers = {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
                'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
                'Accept-Language': 'en-US,en;q=0.5',
                'Connection': 'keep-alive',
                'Upgrade-Insecure-Requests': '1'
            }
            
            # Create PowerShell script
            ps_script = f'''
            $headers = @{{
                "User-Agent" = "{headers['User-Agent']}"
                "Accept" = "{headers['Accept']}"
                "Accept-Language" = "{headers['Accept-Language']}"
                "Connection" = "{headers['Connection']}"
                "Upgrade-Insecure-Requests" = "{headers['Upgrade-Insecure-Requests']}"
            }}
            Invoke-WebRequest -Uri "{url}" -OutFile "{output_path}" -Headers $headers -UseBasicParsing
            '''
            
            # Save script to temporary file
            with open('temp.ps1', 'w') as f:
                f.write(ps_script)
            
            # Execute PowerShell script
            cmd = f'powershell -ExecutionPolicy Bypass -File temp.ps1'
            result = subprocess.run(cmd, shell=True, capture_output=True, text=True)
            
            # Clean up temporary file
            try:
                os.remove('temp.ps1')
            except:
                pass
            
            if result.returncode == 0:
                print(f"Successfully downloaded: {os.path.basename(output_path)}")
                return True
            else:
                print(f"Attempt {attempt + 1} failed for {os.path.basename(output_path)}")
                print(f"Error: {result.stderr}")
                
                # Wait before retry
                if attempt < max_retries - 1:
                    print(f"Waiting 30 seconds before retry...")
                    time.sleep(30)
        except Exception as e:
            print(f"Error downloading {url}: {str(e)}")
            if attempt < max_retries - 1:
                print(f"Waiting 30 seconds before retry...")
                time.sleep(30)
    
    return False

def download_files(urls: List[str], batch_size: int = 3) -> None:
    """
    Download files in batches with verification
    
    Args:
        urls: List of URLs to download
        batch_size: Number of files to download in parallel
    """
    if not urls:
        print("No URLs provided for download")
        return
    
    # Group URLs by year and quarter
    year_quarter_groups: Dict[str, Dict[str, List[str]]] = {}
    for url in urls:
        year, quarter, _ = get_file_info(url)
        if year not in year_quarter_groups:
            year_quarter_groups[year] = {}
        if quarter not in year_quarter_groups[year]:
            year_quarter_groups[year][quarter] = []
        year_quarter_groups[year][quarter].append(url)
    
    # Process each year and quarter
    for year in sorted(year_quarter_groups.keys()):
        for quarter in sorted(year_quarter_groups[year].keys()):
            print(f"\nProcessing {year}{quarter}...")
            urls_to_download = year_quarter_groups[year][quarter]
            total_groups = (len(urls_to_download) + batch_size - 1) // batch_size
            
            # Process in groups
            for group_num in range(1, total_groups + 1):
                start_idx = (group_num - 1) * batch_size
                end_idx = min(group_num * batch_size, len(urls_to_download))
                group_urls = urls_to_download[start_idx:end_idx]
                
                print(f"\nDownloading group {group_num} of {total_groups}...")
                
                # Create output paths for this group
                download_tasks = []
                for url in group_urls:
                    year, quarter, filename = get_file_info(url)
                    output_dir = os.path.join(BASE_DIR, year, quarter)
                    output_path = os.path.join(output_dir, filename)
                    download_tasks.append((url, output_path))
                
                # Download files in parallel
                with ThreadPoolExecutor(max_workers=batch_size) as executor:
                    futures = [executor.submit(download_file, url, path) for url, path in download_tasks]
                    
                    # Wait for all downloads in this group to complete
                    for future in futures:
                        future.result()
                
                # Wait and verify group completion
                if not wait_for_group_completion(year, quarter, group_num):
                    print(f"Warning: Group {group_num} did not complete within timeout period")
                    return False
                
                print(f"Group {group_num} completed successfully")
            
            print(f"All groups for {year}{quarter} completed successfully")
    
    return True

if __name__ == "__main__":
    # Read URLs from file
    try:
        with open("missing_files.txt", "r") as f:
            urls = [line.strip() for line in f if line.strip()]
        
        if urls:
            success = download_files(urls)
            if not success:
                print("Download process was interrupted due to group verification failure")
                sys.exit(1)
        else:
            print("No URLs found in missing_files.txt")
    except FileNotFoundError:
        print("Error: missing_files.txt not found")
    except Exception as e:
        print(f"Error: {str(e)}") 