import os
import json
import time
import pandas as pd

from bybit_info import get_bybit_info
from binance_info import get_binance_info

# Directory to store metadata
METADATA_DIR = "metadata"
METADATA_CSV = os.path.join(METADATA_DIR, "metadata.csv")
METADATA_TIMESTAMP = os.path.join(METADATA_DIR, "metadata_timestamp.json")

def is_metadata_recent():
    """Checks if the stored metadata is less than 1 hour old."""
    if not os.path.exists(METADATA_TIMESTAMP):
        return False  # No timestamp exists, so data is not recent

    try:
        with open(METADATA_TIMESTAMP, "r") as f:
            timestamp_data = json.load(f)
            last_generated = timestamp_data.get("last_generated", 0)
        
        current_time = time.time()
        return (current_time - last_generated) < 3600  # Less than 1 hour (3600 sec)
    
    except Exception as e:
        print(f"Error reading timestamp file: {e}")
        return False  # If error, treat as outdated

def save_metadata(df):
    """Saves metadata as a CSV and records the timestamp in JSON."""
    if not os.path.exists(METADATA_DIR):
        os.makedirs(METADATA_DIR)

    # Save metadata to CSV
    df.to_csv(METADATA_CSV, index=False)

    # Save timestamp
    timestamp_data = {"last_generated": time.time()}
    with open(METADATA_TIMESTAMP, "w") as f:
        json.dump(timestamp_data, f)

def load_metadata():
    """Loads metadata from the saved CSV file."""
    return pd.read_csv(METADATA_CSV)

def get_metadata():
    """Loads existing metadata if fresh, otherwise regenerates and saves new metadata."""
    if is_metadata_recent():
        print("Loading recent metadata...")
        return load_metadata()

    print("Generating new metadata...")
    
    # Generate new metadata
    binance_info = get_binance_info()
    binance_info = binance_info.rename(columns={
        'Network': 'Network (Binance)',
        'Withdrawal Fee': 'Withdrawal Fee (Binance)',
        'Min Withdrawal': 'Min Withdrawal (Binance)',
        'Reliability Score': 'Reliability Score (Binance)',
        'Maker Fee': 'Maker Fee (Binance)',
        'Taker Fee': 'Taker Fee (Binance)',
        '24h Volume': '24h Volume (Binance)'
    })
    binance_info = binance_info.drop(columns=['Max Withdrawal', 'Price Change %'])

    bybit_info = get_bybit_info()
    bybit_info = bybit_info.rename(columns={
        'Network': 'Network (Bybit)',
        'Withdrawal Fee': 'Withdrawal Fee (Bybit)',
        'Min Withdrawal': 'Min Withdrawal (Bybit)',
        'Reliability Score': 'Reliability Score (Bybit)',
        'Maker Fee': 'Maker Fee (Bybit)',
        'Taker Fee': 'Taker Fee (Bybit)',
        '24h Volume': '24h Volume (Bybit)'
    })

    merged_info = pd.merge(binance_info, bybit_info, on='Symbol', how='outer')
    merged_info = merged_info.dropna()

    

    # Save new metadata
    save_metadata(merged_info)

    return merged_info
