import requests
import time
import hmac
import hashlib
import json
import pandas as pd


from utils import choose_best_network

def get_binance_keys():
    with open('keys.json', 'r') as f:
        keys = json.load(f)
    return keys['binance']['apiKey'], keys['binance']['secret']

def get_withdrawal_fees():

    # Get API keys
    API_KEY, API_SECRET = get_binance_keys()

    BASE_URL = "https://api.binance.com"

    # Create a timestamp
    timestamp = int(time.time() * 1000)

    # Create query string
    query_string = f"timestamp={timestamp}"

    # Generate HMAC signature
    signature = hmac.new(API_SECRET.encode(), query_string.encode(), hashlib.sha256).hexdigest()

    # Set headers
    headers = {
        "X-MBX-APIKEY": API_KEY
    }

    # Send request with signature
    response = requests.get(f"{BASE_URL}/sapi/v1/capital/config/getall?{query_string}&signature={signature}", headers=headers)

    # Save response to a JSON file
    with open('withdrawal_fees.json', 'w') as json_file:
        json.dump(response.json(), json_file, indent=4)

    if response.status_code == 200:
        # Extract relevant information
        data = response.json()
        withdrawal_data = []
        for asset in data:
            for network in asset["networkList"]:
                withdrawal_data.append({
                    "Symbol": asset["coin"],
                    "Network": network["network"],
                    "Withdrawal Fee": float(network["withdrawFee"]),
                    "Min Withdrawal": float(network["withdrawMin"]),
                    "Max Withdrawal": float(network["withdrawMax"])
                })

        # Convert to DataFrame
        df_withdrawal_fees = pd.DataFrame(withdrawal_data)


        # Group by coin and list each of the rows within that grouped coin
        grouped = df_withdrawal_fees.groupby('Symbol').apply(lambda x: x.to_dict(orient='records')).to_dict()

        # Convert grouped data to DataFrame
        df_grouped = pd.DataFrame([
            {"Symbol": coin, "Details": details} for coin, details in grouped.items()
        ])

        new_df = pd.DataFrame(columns=['Symbol', 'Network', 'Withdrawal Fee', 'Min Withdrawal', 'Max Withdrawal', 'Reliability Score'])

        for _, row in df_grouped.iterrows():
            best_network_row = choose_best_network(row['Details'])
            if best_network_row is not None:
                best_network_row['Symbol'] = row['Symbol']
                new_df = pd.concat([new_df, pd.DataFrame([best_network_row])], ignore_index=True)
        new_df['Symbol'] = new_df['Symbol'] + 'USDT'
        return new_df

    else:
        raise Exception(f"Error: {response.json()}")

def get_binance_fees_liquidity():

    # Get API keys
    api_key, api_secret = get_binance_keys()

    BASE_URL = "https://api.binance.com"
    
    # Generate timestamp
    timestamp = int(time.time() * 1000)
    
    # Create query string
    query_string = f"timestamp={timestamp}"
    
    # Generate HMAC signature
    signature = hmac.new(api_secret.encode(), query_string.encode(), hashlib.sha256).hexdigest()
    
    # Headers for authenticated request
    headers = {"X-MBX-APIKEY": api_key}
    
    # Fetch trading fee data (requires API key & signature)
    trade_fee_url = f"{BASE_URL}/sapi/v1/asset/tradeFee?{query_string}&signature={signature}"
    trade_fee_response = requests.get(trade_fee_url, headers=headers)
    
    if trade_fee_response.status_code == 200:
        trade_fees = trade_fee_response.json()
    else:
        print(f"Error fetching trading fees: {trade_fee_response.json()}")
        return None

    # Fetch 24h liquidity data (public endpoint)
    liquidity_url = f"{BASE_URL}/api/v3/ticker/24hr"
    liquidity_response = requests.get(liquidity_url)
    
    if liquidity_response.status_code == 200:
        liquidity_data = liquidity_response.json()
    else:
        print(f"Error fetching liquidity data: {liquidity_response.json()}")
        return None

    # Process trade fees
    trade_fee_dict = {}
    for fee in trade_fees:
        symbol = fee["symbol"]
        trade_fee_dict[symbol] = {
            "Maker Fee": float(fee["makerCommission"]),
            "Taker Fee": float(fee["takerCommission"]),
        }

    # Process liquidity data
    liquidity_dict = {}
    for item in liquidity_data:
        symbol = item["symbol"]
        liquidity_dict[symbol] = {
            "24h Volume": float(item["volume"]),
            "Price Change %": float(item["priceChangePercent"]),
        }

    # Merge trading fees with liquidity
    merged_data = []
    for symbol in trade_fee_dict.keys():
        liquidity = liquidity_dict.get(symbol, {"24h Volume": None, "Price Change %": None})
        merged_data.append({
            "Symbol": symbol,
            "Maker Fee": trade_fee_dict[symbol]["Maker Fee"],
            "Taker Fee": trade_fee_dict[symbol]["Taker Fee"],
            "24h Volume": liquidity["24h Volume"],
            "Price Change %": liquidity["Price Change %"],
        })

    # Convert to DataFrame
    df_fees_liquidity = pd.DataFrame(merged_data)
    # Filter symbols with 'USDT' in the name
    df_fees_liquidity = df_fees_liquidity[df_fees_liquidity['Symbol'].str.contains('USDT')]
    return df_fees_liquidity

def get_binance_info():
    # Get API keys
    withdrawl_fee_info = get_withdrawal_fees()
    
    fees_liquidity_info = get_binance_fees_liquidity()

    # Merge dataframes
    df_binance_info = pd.merge(withdrawl_fee_info, fees_liquidity_info, on='Symbol', how='outer')
    # Drop rows with NaN values
    df_binance_info.dropna(inplace=True)

    return df_binance_info

