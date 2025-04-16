import urllib.request
import urllib.error
import ssl
from typing import List, Dict
from urllib.parse import urljoin

def test_file_exists(url: str) -> bool:
    """
    Test if a file exists at the given URL using urllib.request
    
    Args:
        url: URL to test
        
    Returns:
        bool: True if file exists, False otherwise
    """
    try:
        # Create SSL context that doesn't verify certificates
        ssl_context = ssl._create_unverified_context()
        
        # Try to open the URL
        with urllib.request.urlopen(url, context=ssl_context) as response:
            return response.getcode() == 200
    except urllib.error.HTTPError as e:
        print(f"HTTP Error {e.code}: {e.reason} for URL: {url}")
        return False
    except urllib.error.URLError as e:
        print(f"URL Error: {e.reason} for URL: {url}")
        return False
    except Exception as e:
        print(f"Error checking URL {url}: {str(e)}")
        return False

def get_quarter_file_count(year: int, quarter: str) -> int:
    """
    Get the fixed number of files for a given year and quarter.
    
    Args:
        year: Year (e.g., 2014)
        quarter: Quarter (e.g., "q1")
        
    Returns:
        int: Number of files for that quarter
    """
    # Define fixed file counts for each quarter
    file_counts = {
        2014: {"q1": 20, "q2": 16, "q3": 17, "q4": 17},
        2015: {"q1": 25, "q2": 23, "q3": 32, "q4": 24},
        2016: {"q1": 28, "q2": 23, "q3": 23, "q4": 23},
        2017: {"q1": 26, "q2": 25, "q3": 26, "q4": 25},
        2018: {"q1": 30, "q2": 32, "q3": 30, "q4": 28},
        2019: {"q1": 30, "q2": 30, "q3": 32, "q4": 29},
        2020: {"q1": 32, "q2": 30, "q3": 29, "q4": 30},
        2021: {"q1": 33, "q2": 34, "q3": 36, "q4": 31},
        2022: {"q1": 34, "q2": 31, "q3": 32, "q4": 34},
        2023: {"q1": 31, "q2": 30, "q3": 30, "q4": 30}
    }
    
    return file_counts.get(year, {}).get(quarter, 0)

def get_year_quarter_urls(year: str, quarter: str, base_url: str = "https://download.open.fda.gov/drug/event") -> List[str]:
    """
    Generate URLs for a specific year-quarter.
    
    Args:
        year: Year string (e.g., "2023")
        quarter: Quarter string (e.g., "q4")
        base_url: Base URL for the files
        
    Returns:
        List[str]: List of valid URLs for this period
    """
    urls = []
    
    # Get the fixed number of files for this quarter
    total_files = get_quarter_file_count(int(year), quarter)
    if total_files == 0:
        return urls
    
    # Generate URLs for all files
    for file_num in range(1, total_files + 1):
        file_name = f"drug-event-{file_num:04d}-of-{total_files:04d}.json.zip"
        url = urljoin(base_url, f"{year}{quarter}/{file_name}")
        urls.append(url)
    
    return urls

def get_all_urls(base_url: str = "https://download.open.fda.gov/drug/event",
                 start_year: int = 2014,
                 end_year: int = 2023,
                 start_quarter: str = "q1",
                 end_quarter: str = "q4") -> List[str]:
    """
    Generate all URLs for the specified time range.
    
    Args:
        base_url: Base URL for the files
        start_year: Starting year (inclusive)
        end_year: Ending year (inclusive)
        start_quarter: Starting quarter (e.g., "q1")
        end_quarter: Ending quarter (e.g., "q4")
        
    Returns:
        List[str]: List of all valid URLs
    """
    all_urls = []
    quarters = ["q1", "q2", "q3", "q4"]
    
    for year in range(start_year, end_year + 1):
        # For all years, process all quarters
        for quarter in quarters:
            print(f"Generating URLs for {year}{quarter}...")
            urls = get_year_quarter_urls(str(year), quarter, base_url)
            all_urls.extend(urls)
            print(f"Found {len(urls)} files for {year}{quarter}")
    
    return all_urls

if __name__ == "__main__":
    # Example usage
    urls = get_all_urls()
    with open("all_urls.txt", "w") as f:
        for url in urls:
            f.write(f"{url}\n")
    print(f"Generated {len(urls)} URLs and saved to all_urls.txt") 