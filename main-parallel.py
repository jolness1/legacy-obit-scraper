import csv
import aiohttp
import asyncio
import time
import random
from datetime import datetime
import sys
import os
import json
from urllib.parse import quote
import aiofiles

class AsyncObituarySearcher:
    def __init__(self, max_concurrent=10, delay_range=(0.5, 1.5), max_retries=3):
        self.max_concurrent = max_concurrent
        self.delay_range = delay_range
        self.max_retries = max_retries
        self.session = None
        self.semaphore = asyncio.Semaphore(max_concurrent)
        self.rate_limiter = asyncio.Semaphore(max_concurrent)  # Additional rate limiting
        
        # Progress tracking
        self.total_found = 0
        self.total_processed = 0
        self.results = []
        self.lock = asyncio.Lock()  # For thread-safe operations
        
        # Headers for requests
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/138.0.0.0 Safari/537.36",
            "accept": "*/*",
            "referer": "https://www.legacy.com/obituaries/search",
            "accept-language": "en-US,en;q=0.9",
            "accept-encoding": "gzip, deflate, br",
            "cache-control": "no-cache",
            "pragma": "no-cache"
        }

    async def __aenter__(self):
        # Create session with connection limits
        connector = aiohttp.TCPConnector(
            limit=self.max_concurrent * 2,  # Total connection pool
            limit_per_host=self.max_concurrent,  # Per-host limit
            ttl_dns_cache=300,  # DNS cache TTL
            use_dns_cache=True,
        )
        
        timeout = aiohttp.ClientTimeout(total=30, connect=10)
        self.session = aiohttp.ClientSession(
            connector=connector,
            timeout=timeout,
            headers=self.headers
        )
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()

    async def search_legacy_obituary(self, first_name, last_name, row_data):
        """
        Search for obituary with semaphore-based rate limiting
        """
        async with self.semaphore:  # Limit concurrent requests
            async with self.rate_limiter:  # Additional rate limiting
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
                
                # Add random delay to spread out requests
                delay = random.uniform(*self.delay_range)
                await asyncio.sleep(delay)
                
                for attempt in range(self.max_retries):
                    try:
                        async with self.session.get(url) as response:
                            # Handle rate limiting
                            if response.status == 429:
                                wait_time = (2 ** attempt) * 30  # Exponential backoff
                                print(f"Rate limited (429) for {first_name} {last_name}, waiting {wait_time}s")
                                await asyncio.sleep(wait_time)
                                continue
                            
                            if response.status == 403:
                                print(f"Blocked (403) for {first_name} {last_name}")
                                return False, row_data
                            
                            response_text = await response.text()
                            
                            if "captcha" in response_text.lower():
                                print(f"Captcha detected for {first_name} {last_name}")
                                return False, row_data
                            
                            if response.status != 200:
                                print(f"HTTP {response.status} for {first_name} {last_name}")
                                if attempt < self.max_retries - 1:
                                    await asyncio.sleep(5)
                                    continue
                                else:
                                    return False, row_data
                            
                            data = await response.json()
                            found = data.get("totalRecordCount", 0) > 0
                            
                            # Thread-safe progress tracking
                            async with self.lock:
                                self.total_processed += 1
                                if found:
                                    self.total_found += 1
                                    self.results.append(row_data)
                                    print(f"✓ FOUND: {first_name} {last_name} ({self.total_found}/{self.total_processed})")
                                else:
                                    print(f"✗ Not found: {first_name} {last_name} ({self.total_found}/{self.total_processed})")
                            
                            return found, row_data
                            
                    except aiohttp.ClientError as e:
                        print(f"Request error for {first_name} {last_name} (attempt {attempt + 1}): {e}")
                        if attempt < self.max_retries - 1:
                            await asyncio.sleep(5)
                            continue
                        else:
                            return False, row_data
                    except asyncio.TimeoutError:
                        print(f"Timeout for {first_name} {last_name} (attempt {attempt + 1})")
                        if attempt < self.max_retries - 1:
                            await asyncio.sleep(5)
                            continue
                        else:
                            return False, row_data
                
                return False, row_data

    async def process_batch(self, batch_data):
        """Process a batch of records concurrently"""
        tasks = []
        for idx, row in batch_data:
            first_name = row.get('First Name', '').strip()
            last_name = row.get('Last Name', '').strip()
            
            # Skip if names are empty or too short
            if not first_name or not last_name or len(first_name) < 2 or len(last_name) < 2:
                continue
            
            task = self.search_legacy_obituary(first_name, last_name, row)
            tasks.append(task)
        
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)

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

def filter_valid_rows(rows):
    """Filter rows with valid expiration dates (2024+)"""
    valid_rows = []
    for idx, row in enumerate(rows):
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
            
        valid_rows.append((idx, row))
    
    return valid_rows

async def process_licenses_async(file_path, output_file, batch_size=50, max_concurrent=10):
    """
    Process license file asynchronously with batching
    """
    start_idx = load_progress(file_path)
    
    print(f"Starting async processing of {file_path}")
    print(f"Max concurrent requests: {max_concurrent}")
    print(f"Batch size: {batch_size}")
    print(f"Resuming from index: {start_idx}")
    
    # Read all rows
    with open(file_path, mode='r') as file:
        reader = csv.DictReader(file)
        all_rows = list(reader)
        fieldnames = reader.fieldnames
    
    # Filter valid rows and skip already processed ones
    valid_rows = filter_valid_rows(all_rows)
    valid_rows = [(idx, row) for idx, row in valid_rows if idx >= start_idx]
    
    print(f"Total valid rows to process: {len(valid_rows)}")
    
    if not valid_rows:
        print("No valid rows to process!")
        return
    
    # Process in batches
    async with AsyncObituarySearcher(max_concurrent=max_concurrent) as searcher:
        # Check if we should append to existing file
        file_mode = 'w'
        if os.path.exists(output_file):
            response = input(f"{output_file} already exists. Append to it? (y/n): ")
            if response.lower() == 'y':
                file_mode = 'a'
        
        # Process all batches
        for i in range(0, len(valid_rows), batch_size):
            batch = valid_rows[i:i + batch_size]
            last_idx = batch[-1][0]
            
            print(f"\nProcessing batch {i//batch_size + 1}/{(len(valid_rows) + batch_size - 1)//batch_size}")
            print(f"Batch range: {batch[0][0]} to {last_idx}")
            
            batch_start_time = time.time()
            await searcher.process_batch(batch)
            batch_time = time.time() - batch_start_time
            
            print(f"Batch completed in {batch_time:.1f}s")
            print(f"Total found so far: {searcher.total_found}/{searcher.total_processed}")
            
            # Save progress and results after each batch
            save_progress(file_path, last_idx, {
                "total_found": searcher.total_found,
                "total_processed": searcher.total_processed
            })
            
            # Write results to file
            if searcher.results:
                with open(output_file, file_mode, newline='') as out_csv:
                    writer = csv.DictWriter(out_csv, fieldnames=fieldnames)
                    if file_mode == 'w':
                        writer.writeheader()
                        file_mode = 'a'  # Switch to append mode after first write
                    
                    for result in searcher.results:
                        writer.writerow(result)
                
                # Clear results after writing to avoid duplicates
                searcher.results.clear()
            
            # Brief pause between batches to be respectful
            await asyncio.sleep(2)
    
    print(f"\nCompleted {file_path}")
    print(f"Final results: {searcher.total_found}/{searcher.total_processed}")

async def main():
    """Main execution function"""
    nursing_file = './nursing-licenses.csv'
    physician_file = './physician-licenses.csv'
    test_file = './test-licenses.csv'
    output_file = 'possibilities.csv'
    
    # Configuration
    MAX_CONCURRENT = 2  # adjust down if we get rate limited
    BATCH_SIZE = 20      # process this many records before saving progress
    
    # Determine which file to process
    files_to_process = [nursing_file, physician_file]  
    
    for file_path in files_to_process:
        print(f"\n{'='*50}")
        print(f"Processing: {file_path}")
        print(f"{'='*50}")
        
        if not os.path.exists(file_path):
            print(f"Warning: File {file_path} not found. Skipping.")
            continue
        
        try:
            await process_licenses_async(
                file_path, 
                output_file, 
                batch_size=BATCH_SIZE,
                max_concurrent=MAX_CONCURRENT
            )
            print(f"Successfully completed {file_path}")
        except Exception as e:
            print(f"Error processing {file_path}: {e}")
            break
    
    print("\nAll processing complete!")

if __name__ == "__main__":
    asyncio.run(main())