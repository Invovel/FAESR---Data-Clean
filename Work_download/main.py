import os
import sys
import time
from typing import List, Dict, Tuple
from download_all_fda import download_files
from download_check import get_file_info
from colorama import init, Fore, Style
import concurrent.futures

# Initialize colorama
init()

# Base directory for categorized downloads
BASE_DIR = r"D:\Download\Drug Adverse Events"

# Define available colors
COLORS = [
    Fore.RED,          # 红色
    Fore.GREEN,        # 绿色
    Fore.YELLOW,       # 黄色
    Fore.BLUE,         # 蓝色
    Fore.MAGENTA,      # 紫色
    Fore.CYAN,         # 青色
    Fore.LIGHTRED_EX,  # 亮红色
    Fore.LIGHTGREEN_EX,# 亮绿色
    Fore.LIGHTYELLOW_EX,# 亮黄色
    Fore.LIGHTBLUE_EX  # 亮蓝色
]

# Define processing parameters
CHUNK_SIZE = 10 * 1024 * 1024  # 10MB chunks
MAX_WORKERS = 4  # Number of parallel workers
BUFFER_SIZE = 50 * 1024 * 1024  # 50MB buffer

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

class FileProcessor:
    def __init__(self, keywords, colors, output_dir=None):
        self.keywords = keywords
        self.colors = colors
        self.output_dir = output_dir
        self.processed_files = 0
        self.total_files = 0
        self.start_time = time.time()

    def color_text(self, text: str, keyword: str, color: str) -> str:
        """Color the specified keyword in the text."""
        return text.replace(keyword, f"{color}{keyword}{Style.RESET_ALL}")

    def process_chunk(self, chunk: str) -> str:
        """Process a chunk of text with all keywords."""
        for keyword, color in zip(self.keywords, self.colors):
            chunk = self.color_text(chunk, keyword, color)
        return chunk

    def process_file(self, file_path: str):
        """Process a single file with progress tracking."""
        try:
            # Create output path
            if self.output_dir:
                os.makedirs(self.output_dir, exist_ok=True)
                rel_path = os.path.relpath(file_path, os.path.dirname(file_path))
                output_path = os.path.join(self.output_dir, rel_path)
                os.makedirs(os.path.dirname(output_path), exist_ok=True)
            else:
                output_path = file_path + '.colored'

            # Process file in chunks
            with open(file_path, 'r', encoding='utf-8') as input_file, \
                 open(output_path, 'w', encoding='utf-8') as output_file:
                
                buffer = ""
                while True:
                    chunk = input_file.read(CHUNK_SIZE)
                    if not chunk:
                        break
                    
                    # Process and write chunk
                    buffer += chunk
                    if len(buffer) >= BUFFER_SIZE:
                        processed = self.process_chunk(buffer)
                        output_file.write(processed)
                        buffer = ""
                
                # Process remaining buffer
                if buffer:
                    processed = self.process_chunk(buffer)
                    output_file.write(processed)

            # Update progress
            self.processed_files += 1
            elapsed = time.time() - self.start_time
            files_per_second = self.processed_files / elapsed if elapsed > 0 else 0
            remaining_files = self.total_files - self.processed_files
            eta = remaining_files / files_per_second if files_per_second > 0 else 0
            
            print(f"\r{Fore.GREEN}Processed: {self.processed_files}/{self.total_files} files "
                  f"({self.processed_files/self.total_files*100:.1f}%) "
                  f"Speed: {files_per_second:.1f} files/s "
                  f"ETA: {eta/60:.1f} min{Style.RESET_ALL}", end="")

        except Exception as e:
            print(f"\n{Fore.RED}Error processing {file_path}: {str(e)}{Style.RESET_ALL}")

def get_keywords_and_colors():
    """Get keywords and automatically assign colors in sequence."""
    keywords = []
    colors = []
    
    print("\nColors will be assigned automatically in the following order:")
    print("颜色将按以下顺序自动分配：")
    for i, color in enumerate(COLORS, 1):
        print(f"{i}. {color}This is color {i}{Style.RESET_ALL}")
    
    print("\nEnter keywords separated by commas (e.g., keyword1, keyword2, keyword3):")
    print("请输入关键词，用逗号分隔（例如：关键词1, 关键词2, 关键词3）：")
    
    input_text = input().strip()
    if not input_text:
        return keywords, colors
        
    # Split input by comma and clean up whitespace
    input_keywords = [kw.strip() for kw in input_text.split(',') if kw.strip()]
    
    if len(input_keywords) > len(COLORS):
        print(f"{Fore.RED}Error: Maximum number of keywords ({len(COLORS)}) exceeded!{Style.RESET_ALL}")
        print(f"{Fore.RED}错误：超出最大关键词数量限制 ({len(COLORS)})！{Style.RESET_ALL}")
        return None, None
    
    # Assign colors to keywords
    for i, keyword in enumerate(input_keywords):
        color = COLORS[i]
        keywords.append(keyword)
        colors.append(color)
        print(f"{color}Keyword '{keyword}' will be colored with color {i+1}{Style.RESET_ALL}")
    
    return keywords, colors

def process_downloaded_files(directory: str, keywords: list, colors: list, output_dir: str = None):
    """Process downloaded FDA files with keyword coloring."""
    processor = FileProcessor(keywords, colors, output_dir)
    files_to_process = []
    
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith('.json'):
                files_to_process.append(os.path.join(root, file))
    
    processor.total_files = len(files_to_process)
    print(f"\nFound {processor.total_files} files to process")
    
    with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = [executor.submit(processor.process_file, file_path) 
                  for file_path in files_to_process]
        concurrent.futures.wait(futures)

def create_directory_structure():
    """Create the directory structure for all years and quarters."""
    print("Creating directory structure...")
    
    # Create base directory if it doesn't exist
    if not os.path.exists(BASE_DIR):
        os.makedirs(BASE_DIR)
        print(f"Created base directory: {BASE_DIR}")
    
    # Create year and quarter directories
    for year in QUARTER_FILE_COUNTS.keys():
        year_dir = os.path.join(BASE_DIR, year)
        if not os.path.exists(year_dir):
            os.makedirs(year_dir)
            print(f"Created year directory: {year}")
        
        for quarter in ['q1', 'q2', 'q3', 'q4']:
            quarter_dir = os.path.join(year_dir, quarter)
            if not os.path.exists(quarter_dir):
                os.makedirs(quarter_dir)
                print(f"Created quarter directory: {year}/{quarter}")
    
    print("Directory structure created successfully!")

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
            print(f"{Fore.RED}❌ Missing: {filename}{Style.RESET_ALL}")
            return False
        else:
            print(f"{Fore.GREEN}✅ Found: {filename}{Style.RESET_ALL}")
    return True

def verify_quarter_download(year: str, quarter: str) -> bool:
    """
    Verify if all files for a quarter have been downloaded.
    
    Args:
        year: Year to check
        quarter: Quarter to check
        
    Returns:
        bool: True if all files are downloaded, False otherwise
    """
    expected_count = QUARTER_FILE_COUNTS[year][quarter]
    quarter_dir = os.path.join(BASE_DIR, year, quarter)
    actual_count = count_files_in_directory(quarter_dir)
    status = f"{Fore.GREEN}✓{Style.RESET_ALL}" if actual_count >= expected_count else f"{Fore.RED}✗{Style.RESET_ALL}"
    print(f"Verifying {year}{quarter}: {status} {actual_count}/{expected_count} files downloaded")
    return actual_count >= expected_count

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

def download_group_with_retry(year: str, quarter: str, group_urls: List[str], group_number: int, max_retries: int = 3) -> bool:
    """
    Download a group of files with retry mechanism.
    
    Args:
        year: Year of the files
        quarter: Quarter of the files
        group_urls: List of URLs to download
        group_number: Group number (1-based)
        max_retries: Maximum number of retry attempts
        
    Returns:
        bool: True if all files were downloaded successfully, False otherwise
    """
    for attempt in range(max_retries):
        print(f"\nAttempt {attempt + 1} of {max_retries} for group {group_number}")
        download_files(group_urls)
        
        if wait_for_group_completion(year, quarter, group_number):
            print(f"Group {group_number} completed successfully")
            return True
        
        print(f"Group {group_number} failed to complete, retrying...")
    
    print(f"Failed to download group {group_number} after {max_retries} attempts")
    return False

def check_dependencies() -> bool:
    """Check if required dependencies are installed."""
    try:
        import requests
        return True
    except ImportError as e:
        print(f"Error: Required Python package not found: {str(e)}")
        print("Please install it using: pip install requests")
        return False

def main():
    # Create directory structure
    create_directory_structure()
    
    # Check dependencies first
    if not check_dependencies():
        sys.exit(1)
    
    try:
        # Import modules after dependency check
        from download_files import get_all_urls
        from download_check import check_downloaded_files, print_status_summary
    except ImportError as e:
        print(f"Error importing modules: {str(e)}")
        sys.exit(1)
    
    # Get keywords for coloring
    print("\nKeyword Configuration")
    print("-" * 20)
    keywords, colors = get_keywords_and_colors()
    
    if keywords is None:
        print(f"{Fore.RED}Program terminated due to error.{Style.RESET_ALL}")
        return
        
    if not keywords:
        print(f"{Fore.YELLOW}No keywords were entered. Exiting...{Style.RESET_ALL}")
        return
    
    # Generate URLs
    print("\nGenerating URLs for FDA drug adverse events files (2014-2023)...")
    urls = get_all_urls()
    with open("all_urls.txt", "w") as f:
        for url in urls:
            f.write(f"{url}\n")
    print(f"Generated {len(urls)} URLs and saved to all_urls.txt")
    
    # Check downloaded files
    print("\nChecking downloaded files...")
    result = check_downloaded_files(urls=urls, base_dir=BASE_DIR)
    print_status_summary(result)
    
    if result['missing_count'] > 0:
        print("\nMissing files have been logged to missing_files.txt")
        with open("missing_files.txt", "w") as f:
            for url in result['missing_files']:
                f.write(f"{url}\n")
        
        # Ask user if they want to download missing files
        response = input("\nDo you want to download the missing files? (y/n): ")
        if response.lower() == 'y':
            print("\nDownloading missing files...")
            print(f"Files will be downloaded to: {BASE_DIR}")
            
            # Group URLs by year and quarter
            year_quarter_groups: Dict[str, Dict[str, List[str]]] = {}
            for url in result['missing_files']:
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
                    total_groups = (len(urls_to_download) + 2) // 3  # Round up division
                    
                    # Process in groups of 3
                    for group_num in range(1, total_groups + 1):
                        start_idx = (group_num - 1) * 3
                        end_idx = min(group_num * 3, len(urls_to_download))
                        group_urls = urls_to_download[start_idx:end_idx]
                        
                        print(f"\nDownloading group {group_num} of {total_groups}...")
                        if not download_group_with_retry(year, quarter, group_urls, group_num):
                            print(f"Failed to download group {group_num}, stopping quarter {year}{quarter}")
                            break
                    
                    # Verify quarter completion
                    if not verify_quarter_download(year, quarter):
                        print(f"Warning: Quarter {year}{quarter} did not complete successfully")
                        print("Please check the missing files and try again")
                        sys.exit(1)
            
            print("\nDownload completed!")
            
            # Final verification
            print("\nVerifying all downloads...")
            verify_result = check_downloaded_files(urls=urls, base_dir=BASE_DIR)
            print_status_summary(verify_result)
    
    # Process downloaded files with keyword coloring
    print("\nProcessing downloaded files with keyword coloring...")
    process_downloaded_files(BASE_DIR, keywords, colors, BASE_DIR)
    
    print("\nAll operations completed!")

if __name__ == "__main__":
    main() 