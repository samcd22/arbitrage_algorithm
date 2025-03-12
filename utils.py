import json
import pandas as pd

def choose_best_network(networks_and_details, reliability_threshold=70):

    df_networks_and_details = pd.DataFrame(networks_and_details)
    df_networks_and_details['Reliability Score'] = df_networks_and_details['Network'].apply(get_network_reliability_score)
    df_networks_and_details = df_networks_and_details.sort_values('Min Withdrawal', ascending=True)
    for _, row in df_networks_and_details.iterrows():
        if row['Reliability Score'] > reliability_threshold:
            return row
            
    return None


def get_network_reliability_score(network, json_file="network_reliability.json"):
    # Load data from JSON file
    try:
        with open(json_file, "r") as file:
            reliability_data = json.load(file)
    except Exception as e:
        return {"error": f"Failed to load JSON file: {str(e)}"}

    # If the network is not found, return a score of 0
    if network not in reliability_data:
        return 0
    stats = reliability_data[network]

    # Weights for scoring formula
    W_SPEED = 0.3
    W_CONGESTION = 0.2
    W_UPTIME = 0.4
    W_FAILURE = 0.1

    # Extract stats
    speed = stats["Block Time (sec)"]
    congestion = stats["Congestion"]
    uptime = stats["Uptime (%)"]
    failure_rate = stats["Failure Rate (%)"]

    # Normalize scores
    speed_score = max(0, 100 - (speed * 5))  # Faster = Better
    congestion_score = max(0, 100 - congestion)  # Less congestion = Better
    uptime_score = uptime  # Uptime already in percentage
    failure_score = max(0, 100 - failure_rate)  # Fewer failures = Better

    # Compute overall score
    NRS = (W_SPEED * speed_score) + (W_CONGESTION * congestion_score) + (W_UPTIME * uptime_score) + (W_FAILURE * failure_score)

    return NRS

def shorten_network_name(network):
    """Maps full network names to their commonly used symbols."""
    mapping = {
        "ethereum": "ETH",
        "ethereum eth": "ETH",
        "erc20": "ETH",
        "polygon": "MATIC",
        "polygon pos": "MATIC",
        "polygon(bridged)": "MATIC",
        "binance smart chain": "BSC",
        "bsc (bep20)": "BSC",
        "bnb smart chain": "BSC",
        "bnb (bep2)": "BNB",
        "avalanche": "AVAX",
        "avax-c chain": "AVAX",
        "cavax": "AVAX",
        "arbitrum one": "ARBITRUM",
        "arbitrum nova": "ARBITRUM",
        "optimism": "OPTIMISM",
        "op mainnet": "OPTIMISM",
        "solana": "SOL",
        "solana sol": "SOL",
        "sol": "SOL",
        "tron": "TRX",
        "trc20": "TRX",
        "dogecoin": "DOGE",
        "litecoin": "LTC",
        "ripple": "XRP",
        "stellar lumens": "XLM",
        "stellar": "XLM",
        "filecoin": "FIL",
        "polkadot": "DOT",
        "cardano": "ADA",
        "casper": "CSPR",
        "mantle network": "MANTLE",
        "mantle mainnet": "MANTLE",
        "zk sync lite": "ZKSYNC",
        "zk sync era": "ZKSYNC",
        "zeta chain": "ZETA",
        "zeta chain evm": "ZETA",
        "celeo": "CELO",
        "terra": "TERRA",
        "terra classic": "LUNC",
        "base mainnet": "BASE",
        "starknet": "STARK",
        "kaspa": "KAS",
        "kava": "KAVA",
        "sui": "SUI",
        "scroll": "SCROLL",
        "kaspa kaspa": "KAS",
        "blast": "BLAST",
        "moonbeam": "GLMR",
        "kadena": "KDA",
        "one": "ONE",
        "waves": "WAVES",
        "theta": "THETA",
        "xdc": "XDC",
        "xec": "XEC",
        "xym": "XYM",
        "zil": "ZIL",
        "qtum": "QTUM",
        "ravencoin": "RVN",
        "pokt": "POKT",
        "secretnetwork": "SCRT",
        "nibi": "NIBI",
        "icp": "ICP",
        "icx": "ICX",
        "ftm": "FTM",
        "egl": "EGLD",
        "elrond": "EGLD",
        "linea": "LINEA",
        "mode": "MODE",
        "oasis": "OAS",
        "venom": "VENOM",
        "vision": "VIC",
        "avalanche-c": "AVAX",
        "kas": "KAS",
        "ethw": "ETHW",
        "ethf": "ETHF",
        "chiliz chain": "CHZ",  # Chiliz supports multiple tokens like ACM, AFC, CITY, PSG
        "mantle network": "MANTLE",
        "bep20": "BSC",
        "brc20 - unisat": "BRC20",
    }
    
    # Convert to lowercase for case-insensitive matching
    return mapping.get(network.lower(), network)  # Default to original if no match
