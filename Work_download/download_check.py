import os
import urllib.request
import urllib.error
import ssl
from typing import List, Dict, Tuple
from urllib.parse import urlparse

def test_downloaded_file(file_path: str) -> bool:
    """Check if a file exists and is valid."""
    try:
        return os.path.exists(file_path) and os.path.getsize(file_path) > 0
    except:
        return False

def get_file_info(url: str) -> Tuple[str, str, str]:
    """
    Extract year, quarter and filename from URL.
    
    Args:
        url: URL to parse
        
    Returns:
        Tuple of (year, quarter, filename)
    """
    url_parts = urlparse(url).path.split('/')
    year_quarter = url_parts[-2]  # e.g., "2023q4"
    year = year_quarter[:4]
    quarter = year_quarter[4:]
    filename = url_parts[-1]
    return year, quarter, filename

def check_downloaded_files(urls: List[str], base_dir: str = "downloads") -> Dict:
    """
    Check which files from the URL list have been downloaded
    
    Args:
        urls: List of URLs to check
        base_dir: Base directory where files should be located
        
    Returns:
        Dict containing:
            - total_files: Total number of files checked
            - missing_files: List of URLs for missing files
            - missing_count: Number of missing files
            - file_status: Dict of file status by year and quarter
    """
    missing_files = []
    file_status = {}
    
    for url in urls:
        year, quarter, filename = get_file_info(url)
        
        # Initialize year and quarter in status dict if not exists
        if year not in file_status:
            file_status[year] = {}
        if quarter not in file_status[year]:
            file_status[year][quarter] = {"total": 0, "downloaded": 0, "missing": 0}
        
        # Construct expected file path
        file_path = os.path.join(base_dir, year, quarter, filename)
        
        # Update status
        file_status[year][quarter]["total"] += 1
        if test_downloaded_file(file_path):
            file_status[year][quarter]["downloaded"] += 1
        else:
            file_status[year][quarter]["missing"] += 1
            missing_files.append(url)
    
    return {
        'total_files': len(urls),
        'missing_files': missing_files,
        'missing_count': len(missing_files),
        'file_status': file_status
    }

def print_status_summary(result: Dict) -> None:
    """Print a detailed summary of file status."""
    print("\nFile Status Summary:")
    print(f"Total Files: {result['total_files']}")
    print(f"Missing Files: {result['missing_count']}")
    
    print("\nStatus by Year and Quarter:")
    for year in sorted(result['file_status'].keys()):
        print(f"\nYear {year}:")
        for quarter in sorted(result['file_status'][year].keys()):
            status = result['file_status'][year][quarter]
            print(f"  {quarter.upper()}: {status['downloaded']}/{status['total']} files downloaded")

if __name__ == "__main__":
    # Example usage
    result = check_downloaded_files()
    print_status_summary(result)
    
    if result['missing_count'] > 0:
        print("\nMissing files have been logged to missing_files.txt")
        with open("missing_files.txt", "w") as f:
            for url in result['missing_files']:
                f.write(f"{url}\n") 