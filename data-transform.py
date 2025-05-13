# uv python pin 3.12
import requests
import os
import pandas as pd
import glob
import duckdb

# Base URL for the swisstopo STAC API
BASE_URL = "https://data.geo.admin.ch/api/stac/v0.9"

def query_stac_api(collection_id: str, datetime: str = None, params: dict = None) -> dict:
    """
    Query the swisstopo STAC API.

    Args:
        collection_id (str): The collection ID to query.
        datetime (str, optional): Date/time range in ISO 8601 format.
        params (dict, optional): Additional query parameters.

    Returns:
        dict: The API response as a JSON object.
    """
    endpoint = f"{BASE_URL}/collections/{collection_id}/items"
    
    # Initialize params dictionary
    query_params = {
        "datetime": datetime if datetime else None
    }
    
    # Update with additional params if provided
    if params:
        query_params.update(params)

    # Remove None values from params
    query_params = {k: v for k, v in query_params.items() if v is not None}

    response = requests.get(endpoint, params=query_params)
    response.raise_for_status()
    return response.json()

def download_parquet_files(collection_id: str, output_dir: str) -> None:
    """
    Download all Parquet files from a given collection if they don't exist locally.

    Args:
        collection_id (str): The collection ID to query.
        output_dir (str): Directory to save the downloaded files.
    """
    # Query the STAC API for the collection with pagination
    params = {"limit": 100}  # Maximum allowed limit
    next_url = f"{BASE_URL}/collections/{collection_id}/items"
    all_items = []
    
    while next_url:
        print("Querying URL:", next_url)
        response = requests.get(next_url, params=params)
        response.raise_for_status()
        items = response.json()
        
        all_items.extend(items.get("features", []))
        
        # Get next URL from links
        next_url = None
        for link in items.get("links", []):
            if link.get("rel") == "next":
                next_url = link.get("href")
                params = {}  # Clear params as next URL includes them
                break

    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)

    # Iterate over items and download Parquet files
    for item in all_items:
        for asset_key, asset in item.get("assets", {}).items():
            if asset.get("type") == "application/vnd.apache.parquet":
                file_url = asset.get("href")
                file_name = os.path.basename(file_url)
                
                # Determine subfolder based on filename
                subfolder = None
                if "forest" in file_name.lower():
                    subfolder = "forest"
                elif "vegetation" in file_name.lower():
                    subfolder = "vegetation"
                
                if subfolder:
                    # Create subfolder if it doesn't exist
                    subfolder_path = os.path.join(output_dir, subfolder)
                    os.makedirs(subfolder_path, exist_ok=True)
                    output_path = os.path.join(subfolder_path, file_name)
                else:
                    output_path = os.path.join(output_dir, file_name)

                # Check if file already exists
                if os.path.exists(output_path) and "_current_" not in file_name:
                    print(f"File {file_name} already exists, skipping...")
                    continue

                # Download the file
                print(f"Downloading {file_url} to {output_path}...")
                response = requests.get(file_url)
                response.raise_for_status()

                with open(output_path, "wb") as f:
                    f.write(response.content)
                print(f"Downloaded {file_name}.")
    return None


def process_parquet_to_duckdb(output_dir: str, db_path: str = 'vhi.duckdb') -> None:
    """
    Process Parquet files and create timeline tables in DuckDB.
    
    Args:
        output_dir (str): Directory containing Parquet files
        db_path (str): Path to DuckDB database file
    """
    con = duckdb.connect(db_path)
    
    try:
        # Create fresh tables for vegetation and forest timelines
        for data_type in ['vegetation', 'forest']:
            file_pattern = os.path.join(output_dir, data_type, "*.parquet")
            # Drop existing table if it exists and create a new one
            con.execute(f"""
                DROP TABLE IF EXISTS {data_type}_timeline;
                CREATE TABLE {data_type}_timeline (
                    region INTEGER,
                    date DATE,
                    day_of_year INTEGER,
                    vhi DOUBLE
                );
                
                INSERT INTO {data_type}_timeline
                SELECT 
                    REGION_NR as region,
                    date::DATE as date,
                    EXTRACT(DOY FROM date::DATE) as day_of_year,
                    AVG(vhi_mean) as vhi
                FROM read_parquet('{file_pattern}')
                WHERE availability_percentage > 20
                GROUP BY REGION_NR, date
                ORDER BY region, date
            """)
            
            print(f"Processed {data_type} timeline data")
    finally:
        con.close()


def timelines_to_csv(db_path: str, output_dir: str) -> None:
    """
    Save the timeline data to separate CSV files for vegetation and forest.
    
    Args:
        timelines (dict): Dictionary containing timeline data organized by region and type
        output_dir (str): Directory to save the CSV files
    """
    for data_type in ['vegetation', 'forest']:
        csv_path = os.path.join(output_dir, f'vhi_timeline_{data_type}.csv')
        
        # Connect to DuckDB and export table directly to CSV
        con = duckdb.connect(db_path)
        try:
            con.execute(f"""
            COPY (
            SELECT * FROM {data_type}_timeline 
            ORDER BY region, date
            ) TO '{csv_path}' (HEADER, DELIMITER ',')
            """)
            print(f"{data_type.capitalize()} timeline saved to {csv_path}")
        finally:
            con.close()


# Example usage
if __name__ == "__main__":
    collection_id = "ch.swisstopo.swisseo_vhi_v100"  # Collection ID
    output_dir = "./parquet_files"  # Directory to save Parquet files

    try:
        download_parquet_files(collection_id, output_dir)
    except requests.RequestException as e:
        print(f"An error occurred: {e}")    

    # Process parquet files using DuckDB
    print("Creating timeline tables in DuckDB...")
    process_parquet_to_duckdb(output_dir)

    # Create pandas-compatible timeline dict for plotting
    print("Exporting timeline tables to CSV files...")
    timelines_to_csv('vhi.duckdb', ".")

