import csv
import requests
import time
import random
from datetime import datetime
import sys
import os
import json
from urllib.parse import quote

def search_legacy_obituary(first_name, last_name, max_retries=3, force_fail_at=None):
    """
    Search for obituary with ability to force failure for testing
    """
    # For testing: force failure at specific index
    if force_fail_at is not None:
        raise Exception(f"Forced failure for testing at index {force_fail_at}")
    
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
    
    print(f"Progress saved to {progress_file}: index {idx}")

def load_progress(file_path):
    """Load progress from file"""
    progress_file = f"{os.path.splitext(os.path.basename(file_path))[0]}_progress.json"
    try:
        with open(progress_file, 'r') as pf:
            data = json.load(pf)
            start_idx = data.get("last_processed_index", 0)
            print(f"Progress loaded from {progress_file}: resuming from index {start_idx}")
            return start_idx
    except (FileNotFoundError, json.JSONDecodeError):
        print(f"No progress file found for {file_path}, starting from beginning")
        return 0

def test_mode_process_licenses(file_path, writer, output_file, delay_range=(2, 4), fail_at_index=None, max_entries=None):
    """
    Test version with shorter delays and optional forced failure
    """
    start_idx = load_progress(file_path)
    total_found = 0
    total_processed = 0
    
    print(f"🧪 TEST MODE: Processing {file_path}")
    print(f"Starting from index: {start_idx}")
    if fail_at_index:
        print(f"Will simulate failure at index: {fail_at_index}")
    if max_entries:
        print(f"Will process max {max_entries} entries")
    
    with open(file_path, mode='r') as file:
        reader = csv.DictReader(file)
        rows = list(reader)
        total_rows = len(rows)
        
        # Limit entries for testing
        if max_entries and start_idx + max_entries < total_rows:
            end_idx = start_idx + max_entries
            print(f"Processing entries {start_idx} to {end_idx}")
        else:
            end_idx = total_rows
            print(f"Processing entries {start_idx} to {total_rows}")
        
        for idx, row in enumerate(rows):
            if idx < start_idx:
                continue
                
            if idx >= end_idx:
                print(f"Reached max entries limit ({max_entries})")
                break
                
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
                # Check if we should simulate failure
                force_fail = fail_at_index and idx == fail_at_index
                
                found = search_legacy_obituary(first_name, last_name, force_fail_at=idx if force_fail else None)
                total_processed += 1
                
                if found:
                    total_found += 1
                    print(f"✓ FOUND: {first_name} {last_name} (Index: {idx})")
                    writer.writerow(row)
                    output_file.flush()  # Force write to disk
                else:
                    print(f"✗ Not found: {first_name} {last_name} (Index: {idx})")
                
                # Save progress every 3 successful searches (more frequent for testing)
                if total_processed % 3 == 0:
                    save_progress(file_path, idx, {
                        "total_found": total_found,
                        "total_processed": total_processed
                    })
                    print(f"🔄 Progress checkpoint: Found {total_found}/{total_processed} matches")
                
            except Exception as e:
                print(f"❌ Error processing {first_name} {last_name}: {e}")
                save_progress(file_path, idx, {
                    "total_found": total_found,
                    "total_processed": total_processed,
                    "error": str(e)
                })
                print(f"💾 Progress saved due to error. Processed {total_processed} entries, found {total_found} matches.")
                return False  # Indicate failure
            
            # Shorter delay for testing
            delay = random.uniform(*delay_range)
            print(f"⏱️  Waiting {delay:.1f} seconds... ({idx + 1}/{total_rows})")
            time.sleep(delay)
    
    # Save final progress
    save_progress(file_path, end_idx, {
        "total_found": total_found,
        "total_processed": total_processed,
        "completed": True
    })
    
    print(f"✅ Test completed for {file_path}. Found {total_found}/{total_processed} matches.")
    return True

def show_progress_file(file_path):
    """Display the contents of the progress file"""
    progress_file = f"{os.path.splitext(os.path.basename(file_path))[0]}_progress.json"
    try:
        with open(progress_file, 'r') as pf:
            data = json.load(pf)
            print(f"\n📄 Progress file contents ({progress_file}):")
            print(json.dumps(data, indent=2))
    except FileNotFoundError:
        print(f"No progress file found: {progress_file}")

def cleanup_progress_files():
    """Remove all progress files for fresh testing"""
    progress_files = [f for f in os.listdir('.') if f.endswith('_progress.json')]
    for pf in progress_files:
        os.remove(pf)
        print(f"🗑️  Removed progress file: {pf}")

def main():
    """Main execution function with test options"""
    test_file = './test-licenses.csv'
    output_file = 'test_possibilities.csv'
    
    print("🧪 TESTING PROGRESS MECHANISM")
    print("=" * 50)
    
    # Test menu
    print("\nTest Options:")
    print("1. Clean start (remove progress files)")
    print("2. Process 5 entries, then simulate failure")
    print("3. Resume from saved progress")
    print("4. Process 10 entries normally")
    print("5. Show current progress file")
    print("6. Cleanup and exit")
    
    choice = input("\nEnter your choice (1-6): ").strip()
    
    if choice == '1':
        cleanup_progress_files()
        print("✅ Clean start ready. Run option 2 or 4 to begin processing.")
        return
    
    elif choice == '2':
        cleanup_progress_files()
        print("🎯 Starting fresh and will simulate failure at index 5...")
        
        # Process with forced failure
        try:
            with open(test_file, 'r') as f:
                fieldnames = next(csv.reader(f))
        except FileNotFoundError:
            print(f"❌ Error: Could not find file {test_file}")
            return
        
        with open(output_file, 'w', newline='') as out_csv:
            writer = csv.DictWriter(out_csv, fieldnames=fieldnames)
            writer.writeheader()
            
            test_mode_process_licenses(test_file, writer, out_csv, delay_range=(1, 2), fail_at_index=5, max_entries=10)
        
        print("\n🔍 After simulated failure:")
        show_progress_file(test_file)
        
    elif choice == '3':
        print("🔄 Resuming from saved progress...")
        
        try:
            with open(test_file, 'r') as f:
                fieldnames = next(csv.reader(f))
        except FileNotFoundError:
            print(f"❌ Error: Could not find file {test_file}")
            return
        
        with open(output_file, 'a', newline='') as out_csv:  # Append mode
            writer = csv.DictWriter(out_csv, fieldnames=fieldnames)
            
            test_mode_process_licenses(test_file, writer, out_csv, delay_range=(1, 2), max_entries=10)
        
        print("\n🔍 After resuming:")
        show_progress_file(test_file)
        
    elif choice == '4':
        cleanup_progress_files()
        print("🚀 Processing 10 entries normally...")
        
        try:
            with open(test_file, 'r') as f:
                fieldnames = next(csv.reader(f))
        except FileNotFoundError:
            print(f"❌ Error: Could not find file {test_file}")
            return
        
        with open(output_file, 'w', newline='') as out_csv:
            writer = csv.DictWriter(out_csv, fieldnames=fieldnames)
            writer.writeheader()
            
            test_mode_process_licenses(test_file, writer, out_csv, delay_range=(1, 2), max_entries=10)
        
        print("\n🔍 Final progress:")
        show_progress_file(test_file)
        
    elif choice == '5':
        show_progress_file(test_file)
        
    elif choice == '6':
        cleanup_progress_files()
        print("🧹 Cleanup complete. Goodbye!")
        
    else:
        print("❌ Invalid choice. Please run again.")

if __name__ == "__main__":
    main()