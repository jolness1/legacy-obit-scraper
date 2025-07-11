import csv
import requests
import time
import random
from datetime import datetime
import sys
import os
import json
from urllib.parse import quote

def search_legacy_obituary(first_name, last_name, max_retries=3):
    """
    Search for obituary with exponential backoff retry logic
    """
    # URL encode the names to handle special characters
    encoded_first = quote(first_name.strip())
    encoded_last = quote(last_name.strip())
    
    url = (
        "https://www.legacy.com/api/_frontend/search"
        "?countryIdList=1"
        "&endDate=12-01-2025"
        f"&firstName={encoded_first}"
        "&keyword="
        f"&lastName={encoded_last}"
        "&limit=50"
        "&noticeType=all"
        "&regionIdList=41"
        "&session_id="
        "&startDate=01-01-2023"
    )
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
        "accept": "*/*",
        "referer": "https://www.legacy.com/obituaries/search",
        "accept-language": "en-US,en;q=0.9",
        "accept-encoding": "gzip, deflate, br",
        "cache-control": "no-cache",
        "pragma": "no-cache"
    }
    
    for attempt in range(max_retries):
        try:
            response = requests.get(url, headers=headers, timeout=30)
            
            # Check for rate limiting or captcha
            if response.status_code == 429:
                print(f"Rate limited (429) on attempt {attempt + 1}")
                if attempt < max_retries - 1:
                    wait_time = (2 ** attempt) * 60  # Exponential backoff: 1, 2, 4 minutes
                    print(f"Waiting {wait_time} seconds before retry...")
                    time.sleep(wait_time)
                    continue
                else:
                    raise Exception("Rate limited - max retries exceeded")
            
            if response.status_code == 403:
                print("Blocked (403) - possible IP ban or captcha")
                raise Exception("Blocked by server")
            
            if "captcha" in response.text.lower():
                print("Captcha detected")
                raise Exception("Captcha required")
            
            if response.status_code != 200:
                print(f"HTTP {response.status_code}: {response.text[:100]}")
                if attempt < max_retries - 1:
                    time.sleep(5)
                    continue
                else:
                    return False  # Assume no match on persistent errors
            
            data = response.json()
            return data.get("totalRecordCount", 0) > 0
            
        except requests.exceptions.RequestException as e:
            print(f"Request error on attempt {attempt + 1}: {e}")
            if attempt < max_retries - 1:
                time.sleep(5)
                continue
            else:
                return False  # Assume no match on persistent connection errors
    
    return False

def save_progress(file_path, idx, additional_data=None):
    """Save progress with additional metadata"""
    progress_file = f"{os.path.splitext(os.path.basename(file_path))[0]}_progress.json"
    progress_data = {
        "last_processed_index": idx,
        "timestamp": datetime.now().isoformat(),
        "file_path": file_path
    }
    if additional_data:
        progress_data.update(additional_data)
    
    with open(progress_file, 'w') as pf:
        json.dump(progress_data, pf, indent=2)

def load_progress(file_path):
    """Load progress from file"""
    progress_file = f"{os.path.splitext(os.path.basename(file_path))[0]}_progress.json"
    try:
        with open(progress_file, 'r') as pf:
            data = json.load(pf)
            return data.get("last_processed_index", 0)
    except (FileNotFoundError, json.JSONDecodeError):
        return 0

def process_licenses(file_path, writer, delay_range=(8, 15)):
    """
    Process license file with improved error handling and progress tracking
    """
    start_idx = load_progress(file_path)
    total_found = 0
    total_processed = 0
    
    print(f"Starting processing of {file_path}")
    print(f"Resuming from index: {start_idx}")
    
    with open(file_path, mode='r') as file:
        reader = csv.DictReader(file)
        rows = list(reader)
        total_rows = len(rows)
        
        print(f"Total rows to process: {total_rows - start_idx}")
        
        for idx, row in enumerate(rows):
            if idx < start_idx:
                continue
                
            # Skip rows with invalid expiration dates
            exp_date = row.get('Expiration Date', '').strip()
            if not exp_date:
                continue
                
            try:
                # Handle different date formats
                if '/' in exp_date:
                    year = int(exp_date.split('/')[-1])
                elif '-' in exp_date:
                    year = int(exp_date.split('-')[-1])
                else:
                    continue
            except (ValueError, IndexError):
                continue
                
            # Only process recent expirations (likely deaths)
            if year <= 2023:
                continue
                
            first_name = row.get('First Name', '').strip()
            last_name = row.get('Last Name', '').strip()
            
            # Skip if names are empty or too short
            if not first_name or not last_name or len(first_name) < 2 or len(last_name) < 2:
                continue
            
            try:
                found = search_legacy_obituary(first_name, last_name)
                total_processed += 1
                
                if found:
                    total_found += 1
                    print(f"✓ FOUND: {first_name} {last_name} (Index: {idx})")
                    writer.writerow(row)
                    out_csv.flush()  # Force write to disk
                else:
                    print(f"✗ Not found: {first_name} {last_name} (Index: {idx})")
                
                # Save progress every 10 successful searches
                if total_processed % 10 == 0:
                    save_progress(file_path, idx, {
                        "total_found": total_found,
                        "total_processed": total_processed
                    })
                    print(f"Progress saved. Found {total_found}/{total_processed} matches so far.")
                
            except Exception as e:
                print(f"Error processing {first_name} {last_name}: {e}")
                save_progress(file_path, idx, {
                    "total_found": total_found,
                    "total_processed": total_processed,
                    "error": str(e)
                })
                print(f"Progress saved due to error. Processed {total_processed} entries, found {total_found} matches.")
                return False  # Indicate failure
            
            # Variable delay between requests
            delay = random.uniform(*delay_range)
            print(f"Waiting {delay:.1f} seconds... ({idx + 1}/{total_rows})")
            time.sleep(delay)
    
    # Save final progress
    save_progress(file_path, len(rows), {
        "total_found": total_found,
        "total_processed": total_processed,
        "completed": True
    })
    
    print(f"Completed {file_path}. Found {total_found}/{total_processed} matches.")
    return True

def main():
    """Main execution function"""
    nursing_file = './nursing-licenses.csv'
    physician_file = './physician-licenses.csv'
    test_file = './test-licenses.csv'
    output_file = 'possibilities.csv'
    
    # Determine which file to process
    files_to_process = [test_file]  # Change this to your desired files
    # files_to_process = [nursing_file, physician_file]
    
    # Get fieldnames from first file
    try:
        with open(files_to_process[0], 'r') as f:
            fieldnames = next(csv.reader(f))
    except FileNotFoundError:
        print(f"Error: Could not find file {files_to_process[0]}")
        sys.exit(1)
    
    # Check if output file exists and ask user if they want to append
    file_mode = 'w'
    if os.path.exists(output_file):
        response = input(f"{output_file} already exists. Append to it? (y/n): ")
        if response.lower() == 'y':
            file_mode = 'a'
        else:
            print("Will overwrite existing file.")
    
    with open(output_file, file_mode, newline='') as out_csv:
        writer = csv.DictWriter(out_csv, fieldnames=fieldnames)
        
        # Write header only if creating new file
        if file_mode == 'w':
            writer.writeheader()
        
        for file_path in files_to_process:
            print(f"\n{'='*50}")
            print(f"Processing: {file_path}")
            print(f"{'='*50}")
            
            if not os.path.exists(file_path):
                print(f"Warning: File {file_path} not found. Skipping.")
                continue
            
            success = process_licenses(file_path, writer, delay_range=(8, 15))
            
            if not success:
                print(f"Processing failed for {file_path}. Check the error and restart.")
                break
            
            print(f"Successfully completed {file_path}")
    
    print("\nAll processing complete!")

if __name__ == "__main__":
    main()