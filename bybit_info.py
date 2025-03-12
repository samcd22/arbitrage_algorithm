import requests
import time
import hmac
import hashlib
import json
import pandas as pd
import numpy as np

from utils import shorten_network_name, choose_best_network

# Load API keys from file
with open("keys.json", "r") as f:
    keys = json.load(f)
API_KEY, API_SECRET = keys["bybit"]["apiKey"], keys["bybit"]["secret"]

BASE_URL = "https://api.bybit.com"

# Required Endpoint
COIN_INFO_URL = f"{BASE_URL}/v5/asset/coin/query-info"

# API Endpoints
FEE_RATE_URL = f"{BASE_URL}/v5/account/fee-rate"
MARKET_TICKERS_URL = f"{BASE_URL}/v5/market/tickers"


def generate_signature(api_secret, params):
    """Generates HMAC SHA256 signature for private API requests."""
    sorted_params = sorted(params.items())  # Sort parameters
    query_string = "&".join(f"{key}={value}" for key, value in sorted_params)
    return hmac.new(api_secret.encode(), query_string.encode(), hashlib.sha256).hexdigest()


def get_bybit_withdrawal_fees():
    """Fetches withdrawal fee, min, max, and best network for all coins (Authenticated)."""
    timestamp = str(int(time.time() * 1000))

    # Prepare required parameters
    params = {
        "api_key": API_KEY,
        "timestamp": timestamp,
    }

    # Generate authentication signature
    params["sign"] = generate_signature(API_SECRET, params)

    # Make API request
    response = requests.get(COIN_INFO_URL, params=params)

    # Check HTTP response
    if response.status_code != 200:
        print(f"Error: Received status code {response.status_code}")
        print(response.text)
        return {}

    try:
        data = response.json()

        if data.get("retCode") != 0:
            print(f"API Error: {data.get('retMsg')}")
            return {}

        coin_info = pd.DataFrame({"Symbol": [], "Network": [], "Min Withdrawal": [], "Withdrawal Fee": [], "Reliability Score": []})


        for coin in data["result"]["rows"]:
            coin_symbol = coin.get("coin", "Unknown")
            networks = coin.get("chains", [])

            if not networks:
                continue  # Skip if no networks found

            # Prepare list of available networks with relevant details
            networks_and_details = []
            for net in networks:
                networks_and_details.append({
                    "Network": shorten_network_name(net["chainType"]),
                    "Min Withdrawal": net['withdrawMin'],
                    "Withdrawal Fee": net['withdrawFee'],
                }) 

            # Choose the best network
            best_network = choose_best_network(networks_and_details)
            best_network = pd.Series(best_network)
            best_network["Symbol"] = coin_symbol + 'USDT'

            coin_info = pd.concat([coin_info, pd.DataFrame([best_network])], ignore_index=True)

        return coin_info

    except Exception as e:
        print(f"Error parsing response: {e}")
        return {}


def get_fee_rates():
    """Fetches maker and taker fees for all spot trading pairs (Authenticated)."""
    try:
        timestamp = str(int(time.time() * 1000))
        params = {
            "api_key": API_KEY,
            "timestamp": timestamp,
            "category": "spot"  # Ensure we're fetching spot trading fees
        }
        params["sign"] = generate_signature(API_SECRET, params)

        headers = {"Content-Type": "application/json"}
        response = requests.get(FEE_RATE_URL, headers=headers, params=params)
        data = response.json()

        if data.get("retCode") != 0:
            print(f"API Error (Fee Rates): {data.get('retMsg')}")
            return {}

        fee_info = {}
        for fee in data["result"]["list"]:  # Ensure correct key for fees
            symbol = fee["symbol"]
            fee_info[symbol] = {
                "maker_fee": float(fee.get("makerFeeRate", 0)),  # Convert to float
                "taker_fee": float(fee.get("takerFeeRate", 0)),  # Convert to float
            }

        return fee_info

    except Exception as e:
        print(f"Error fetching fee rates: {e}")
        return {}

def get_24h_volume():
    """Fetches 24h trading volume for all spot trading pairs (Public API)."""
    try:
        params = {"category": "spot"}  # Ensure category is specified
        response = requests.get(MARKET_TICKERS_URL, params=params)
        data = response.json()

        if data.get("retCode") != 0:
            print(f"API Error (Market Tickers): {data.get('retMsg')}")
            return {}

        volume_info = {}
        for ticker in data["result"]["list"]:
            symbol = ticker["symbol"]
            volume_info[symbol] = float(ticker.get("volume24h", 0))  # Convert to float

        return volume_info

    except Exception as e:
        print(f"Error fetching 24h volume: {e}")
        return {}

def get_bybit_fees_liquidity():
    """Combines fee rates and liquidity data into a structured table."""
    fee_data = get_fee_rates()
    volume_data = get_24h_volume()

    # Create table data
    table_data = []
    for symbol in volume_data.keys():  # Iterate over available trading pairs
        maker_fee = fee_data.get(symbol, {}).get("maker_fee", "N/A")
        taker_fee = fee_data.get(symbol, {}).get("taker_fee", "N/A")
        volume = volume_data.get(symbol, "N/A")

        table_data.append([symbol, maker_fee, taker_fee, volume])

    # Convert to Pandas DataFrame
    df = pd.DataFrame(table_data, columns=["Symbol", "Maker Fee", "Taker Fee", "24h Volume"])
    
    # Sort by highest 24h volume (liquidity)
    df = df.sort_values(by="24h Volume", ascending=False)

    return df

def get_bybit_info():

    """Merge withdrawal fees and trading fees/liquidity for Bybit."""

    withdrawal_fee_info = get_bybit_withdrawal_fees()
    fees_liquidity_info = get_bybit_fees_liquidity()

    # Merge dataframes
    df_bybit_info = pd.merge(withdrawal_fee_info, fees_liquidity_info, on='Symbol', how='outer')
    
    # Drop rows with NaN values
    df_bybit_info.dropna(inplace=True)

    return df_bybit_info
