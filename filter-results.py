import csv
import aiohttp
import asyncio
import json
import os
import re
from urllib.parse import quote
from datetime import datetime
import unicodedata

class NameMatcher:
    def __init__(self):
        self.session = None
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
        timeout = aiohttp.ClientTimeout(total=30)
        self.session = aiohttp.ClientSession(timeout=timeout, headers=self.headers)
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    def normalize_name(self, name):
        """Normalize names for comparison"""
        if not name:
            return ""
        
        # Remove accents and normalize unicode
        name = unicodedata.normalize('NFKD', name).encode('ascii', 'ignore').decode('ascii')
        
        # Convert to lowercase and strip whitespace
        name = name.lower().strip()
        
        # Remove common suffixes/prefixes
        suffixes = ['jr', 'sr', 'ii', 'iii', 'iv', 'v', 'md', 'phd', 'rn', 'np', 'pa']
        prefixes = ['dr', 'mr', 'mrs', 'ms', 'miss']
        
        # Remove periods and split into parts
        parts = re.split(r'[.\s-]+', name)
        parts = [p for p in parts if p and p not in suffixes + prefixes]
        
        return ' '.join(parts)
    
    def get_name_variations(self, first_name, last_name):
        """Generate possible name variations for matching"""
        variations = []
        
        # Original names
        norm_first = self.normalize_name(first_name)
        norm_last = self.normalize_name(last_name)
        
        # Handle hyphenated first names
        if '-' in norm_first:
            first_parts = norm_first.split('-')
            variations.extend([
                (part, norm_last) for part in first_parts  # Each part individually
            ])
            variations.append((norm_first.replace('-', ' '), norm_last))  # Space instead of hyphen
        
        # Handle hyphenated last names
        if '-' in norm_last:
            last_parts = norm_last.split('-')
            variations.extend([
                (norm_first, part) for part in last_parts  # Each part individually
            ])
            variations.append((norm_first, norm_last.replace('-', ' ')))  # Space instead of hyphen
        
        # Handle both hyphenated
        if '-' in norm_first and '-' in norm_last:
            first_parts = norm_first.split('-')
            last_parts = norm_last.split('-')
            for fp in first_parts:
                for lp in last_parts:
                    variations.append((fp, lp))
        
        # Add original normalized version
        variations.append((norm_first, norm_last))
        
        # Remove duplicates while preserving order
        seen = set()
        unique_variations = []
        for var in variations:
            if var not in seen:
                seen.add(var)
                unique_variations.append(var)
        
        return unique_variations
    
    def check_name_match(self, license_first, license_last, obit_name_obj):
        """Check if names match using various strategies"""
        if not obit_name_obj:
            return False, "No name object"
        
        obit_first = obit_name_obj.get('firstName', '')
        obit_last = obit_name_obj.get('lastName', '')
        obit_middle = obit_name_obj.get('middleName', '')
        obit_nick = obit_name_obj.get('nickName', '')
        obit_maiden = obit_name_obj.get('maidenName', '')
        
        # Get all possible variations of the license name
        license_variations = self.get_name_variations(license_first, license_last)
        
        # Check against primary obit name
        obit_variations = self.get_name_variations(obit_first, obit_last)
        
        # Check for exact matches
        for lic_first, lic_last in license_variations:
            for obit_f, obit_l in obit_variations:
                if lic_first == obit_f and lic_last == obit_l:
                    return True, f"Exact match: {lic_first} {lic_last}"
        
        # Check with middle name as first name
        if obit_middle:
            middle_variations = self.get_name_variations(obit_middle, obit_last)
            for lic_first, lic_last in license_variations:
                for mid_f, mid_l in middle_variations:
                    if lic_first == mid_f and lic_last == mid_l:
                        return True, f"Middle name match: {lic_first} {lic_last}"
        
        # Check with nickname
        if obit_nick:
            nick_variations = self.get_name_variations(obit_nick, obit_last)
            for lic_first, lic_last in license_variations:
                for nick_f, nick_l in nick_variations:
                    if lic_first == nick_f and lic_last == nick_l:
                        return True, f"Nickname match: {lic_first} {lic_last}"
        
        # Check maiden name
        if obit_maiden:
            maiden_variations = self.get_name_variations(obit_first, obit_maiden)
            for lic_first, lic_last in license_variations:
                for maiden_f, maiden_l in maiden_variations:
                    if lic_first == maiden_f and lic_last == maiden_l:
                        return True, f"Maiden name match: {lic_first} {lic_last}"
        
        return False, f"No match found. License: {license_first} {license_last}, Obit: {obit_first} {obit_last}"
    
    async def get_obituary_details(self, first_name, last_name):
        """Get detailed obituary information including all matches"""
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
        
        try:
            async with self.session.get(url) as response:
                if response.status != 200:
                    return []
                
                data = await response.json()
                return data.get("searchResults", [])
        except Exception as e:
            print(f"Error fetching details for {first_name} {last_name}: {e}")
            return []

async def filter_possibilities(input_file, output_filtered, output_removed):
    """Filter possibilities based on strict name matching"""
    
    print(f"Starting name filtering process...")
    print(f"Input file: {input_file}")
    print(f"Filtered output: {output_filtered}")
    print(f"Removed output: {output_removed}")
    
    if not os.path.exists(input_file):
        print(f"Error: Input file {input_file} not found!")
        return
    
    filtered_records = []
    removed_records = []
    
    async with NameMatcher() as matcher:
        with open(input_file, 'r') as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            fieldnames = reader.fieldnames
        
        print(f"Processing {len(rows)} records...")
        
        for i, row in enumerate(rows):
            first_name = row.get('First Name', '').strip()
            last_name = row.get('Last Name', '').strip()
            
            if not first_name or not last_name:
                removed_records.append({
                    **row,
                    'removal_reason': 'Empty name fields',
                    'matched_obituaries': '[]'
                })
                continue
            
            print(f"Processing {i+1}/{len(rows)}: {first_name} {last_name}")
            
            # Get all obituary matches
            obituaries = await matcher.get_obituary_details(first_name, last_name)
            
            if not obituaries:
                removed_records.append({
                    **row,
                    'removal_reason': 'No obituaries found',
                    'matched_obituaries': '[]'
                })
                continue
            
            # Check each obituary for name matches
            matched_obituaries = []
            unmatched_obituaries = []
            
            for obit in obituaries:
                name_obj = obit.get('name', {})
                is_match, match_reason = matcher.check_name_match(first_name, last_name, name_obj)
                
                obit_info = {
                    'name': name_obj,
                    'id': obit.get('id'),
                    'obituaryUrl': obit.get('links', {}).get('obituaryUrl', {}).get('href', ''),
                    'match_reason': match_reason,
                    'is_match': is_match
                }
                
                if is_match:
                    matched_obituaries.append(obit_info)
                else:
                    unmatched_obituaries.append(obit_info)
            
            if matched_obituaries:
                # Keep record with matched obituaries
                filtered_records.append({
                    **row,
                    'matched_obituaries': json.dumps(matched_obituaries, indent=2),
                    'total_matches': len(matched_obituaries),
                    'total_obituaries_found': len(obituaries)
                })
                print(f"  ✓ Kept: {len(matched_obituaries)}/{len(obituaries)} obituaries matched")
            else:
                # Remove record with unmatched obituaries
                removed_records.append({
                    **row,
                    'removal_reason': 'No matching obituary names found',
                    'matched_obituaries': json.dumps(unmatched_obituaries, indent=2),
                    'total_obituaries_found': len(obituaries)
                })
                print(f"  ✗ Removed: 0/{len(obituaries)} obituaries matched")
            
            # Brief delay to be respectful
            await asyncio.sleep(0.5)
    
    # Write filtered results
    if filtered_records:
        filtered_fieldnames = list(fieldnames) + ['matched_obituaries', 'total_matches', 'total_obituaries_found']
        with open(output_filtered, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=filtered_fieldnames)
            writer.writeheader()
            writer.writerows(filtered_records)
        print(f"✓ Wrote {len(filtered_records)} filtered records to {output_filtered}")
    else:
        print("No records passed the filter!")
    
    # Write removed results
    if removed_records:
        removed_fieldnames = list(fieldnames) + ['removal_reason', 'matched_obituaries', 'total_obituaries_found']
        with open(output_removed, 'w', newline='') as f:
            writer = csv.DictWriter(f, fieldnames=removed_fieldnames)
            writer.writeheader()
            writer.writerows(removed_records)
        print(f"✓ Wrote {len(removed_records)} removed records to {output_removed}")
    
    print(f"\nFiltering complete!")
    print(f"Kept: {len(filtered_records)}")
    print(f"Removed: {len(removed_records)}")
    print(f"Total processed: {len(filtered_records) + len(removed_records)}")

async def main():
    """Main function"""
    input_file = 'possibilities.csv'
    output_filtered = 'filtered-possibilities.csv'
    output_removed = 'removed-possibilities.csv'
    
    await filter_possibilities(input_file, output_filtered, output_removed)

if __name__ == "__main__":
    asyncio.run(main())