import requests
import os
import zipfile
from pathlib import Path
import asyncio
import aiohttp
import aiofiles
from concurrent.futures import ThreadPoolExecutor
import time

download_uris = [
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2018_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q2.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q3.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2019_Q4.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2020_Q1.zip",
    "https://divvy-tripdata.s3.amazonaws.com/Divvy_Trips_2220_Q1.zip",  # This one might be invalid (year 2220)
]


def create_downloads_directory():
    """Create downloads directory if it doesn't exist"""
    downloads_dir = Path("downloads")
    downloads_dir.mkdir(exist_ok=True)
    return downloads_dir


def extract_filename_from_uri(uri):
    """Extract filename from URI"""
    return uri.split('/')[-1]


def download_file_sync(uri, downloads_dir):
    """Download a single file synchronously"""
    try:
        filename = extract_filename_from_uri(uri)
        filepath = downloads_dir / filename
        
        print(f"Downloading {filename}...")
        
        response = requests.get(uri, stream=True, timeout=30)
        response.raise_for_status()
        
        with open(filepath, 'wb') as f:
            for chunk in response.iter_content(chunk_size=8192):
                f.write(chunk)
        
        print(f"✓ Downloaded {filename}")
        return filepath
        
    except requests.exceptions.RequestException as e:
        print(f"✗ Failed to download {uri}: {e}")
        return None


def extract_zip_file(zip_filepath):
    """Extract CSV from zip file and delete the zip"""
    try:
        with zipfile.ZipFile(zip_filepath, 'r') as zip_ref:
            # Extract all files to the same directory
            extract_dir = zip_filepath.parent
            zip_ref.extractall(extract_dir)
            
            # Get list of extracted files
            extracted_files = zip_ref.namelist()
            
        # Delete the zip file
        os.remove(zip_filepath)
        
        print(f"✓ Extracted and cleaned up {zip_filepath.name}")
        return extracted_files
        
    except (zipfile.BadZipFile, Exception) as e:
        print(f"✗ Failed to extract {zip_filepath}: {e}")
        # Remove corrupted zip file
        if zip_filepath.exists():
            os.remove(zip_filepath)
        return None


def download_and_extract_sync(uri, downloads_dir):
    """Download and extract a single file"""
    zip_filepath = download_file_sync(uri, downloads_dir)
    if zip_filepath:
        extract_zip_file(zip_filepath)


# Synchronous version (basic solution)
def main_sync():
    """Main function - synchronous version"""
    print("Starting synchronous download...")
    start_time = time.time()
    
    downloads_dir = create_downloads_directory()
    
    for uri in download_uris:
        download_and_extract_sync(uri, downloads_dir)
    
    end_time = time.time()
    print(f"\nSynchronous download completed in {end_time - start_time:.2f} seconds")


# ThreadPoolExecutor version
def main_threaded():
    """Main function using ThreadPoolExecutor"""
    print("Starting threaded download...")
    start_time = time.time()
    
    downloads_dir = create_downloads_directory()
    
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = []
        for uri in download_uris:
            future = executor.submit(download_and_extract_sync, uri, downloads_dir)
            futures.append(future)
        
        # Wait for all downloads to complete
        for future in futures:
            future.result()
    
    end_time = time.time()
    print(f"\nThreaded download completed in {end_time - start_time:.2f} seconds")


# Async version with aiohttp
async def download_file_async(session, uri, downloads_dir):
    """Download a single file asynchronously"""
    try:
        filename = extract_filename_from_uri(uri)
        filepath = downloads_dir / filename
        
        print(f"Downloading {filename}...")
        
        async with session.get(uri, timeout=aiohttp.ClientTimeout(total=60)) as response:
            response.raise_for_status()
            
            async with aiofiles.open(filepath, 'wb') as f:
                async for chunk in response.content.iter_chunked(8192):
                    await f.write(chunk)
        
        print(f"✓ Downloaded {filename}")
        return filepath
        
    except Exception as e:
        print(f"✗ Failed to download {uri}: {e}")
        return None


async def download_and_extract_async(session, uri, downloads_dir):
    """Download and extract a single file asynchronously"""
    zip_filepath = await download_file_async(session, uri, downloads_dir)
    if zip_filepath:
        # Extract in a separate thread to avoid blocking
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, extract_zip_file, zip_filepath)


async def main_async():
    """Main function - async version"""
    print("Starting async download...")
    start_time = time.time()
    
    downloads_dir = create_downloads_directory()
    
    async with aiohttp.ClientSession() as session:
        tasks = []
        for uri in download_uris:
            task = download_and_extract_async(session, uri, downloads_dir)
            tasks.append(task)
        
        await asyncio.gather(*tasks)
    
    end_time = time.time()
    print(f"\nAsync download completed in {end_time - start_time:.2f} seconds")


def main():
    """Main entry point"""
    import sys
    
    # Check if running in non-interactive environment (like Docker)
    try:
        # Try to detect if we're in an interactive terminal
        if not sys.stdin.isatty():
            # Not interactive, run synchronous version
            print("Running in non-interactive mode, using synchronous download...")
            main_sync()
            return
        
        print("Choose download method:")
        print("1. Synchronous (basic)")
        print("2. Threaded")  
        print("3. Async")
        
        choice = input("Enter choice (1-3) or press Enter for sync: ").strip()
        
        if choice == "2":
            main_threaded()
        elif choice == "3":
            asyncio.run(main_async())
        else:
            main_sync()
            
    except (EOFError, KeyboardInterrupt):
        # Handle case where input is not available
        print("No input available, running synchronous download...")
        main_sync()


if __name__ == "__main__":
    main()