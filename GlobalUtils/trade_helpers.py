"""
Lightweight, pure-Python trade helpers.
No network, web3, dotenv, or API dependencies -- safe to import anywhere.
"""

DECIMALS = {
    "BTC": 8,
    "ETH": 18,
    "SNX": 18,
    "SOL": 9,
    "W": 18,
    "WIF": 6,
    "ARB": 18,
    "BNB": 18,
    "ENA": 18,
    "DOGE": 8,
    "AVAX": 18,
    "PENDLE": 18,
    "NEAR": 24,
    "AAVE": 18,
    "ATOM": 6,
    "XRP": 6,
    "LINK": 18,
    "UNI": 18,
    "LTC": 8,
    "OP": 18,
    "GMX": 18,
    "PEPE": 18,
}

def get_decimals_for_symbol(symbol):
    return DECIMALS.get(symbol, None)

def normalize_symbol(symbol: str) -> str:
    return symbol.replace('USDT', '').replace('PERP', '').replace('USD', '')

def adjust_trade_size_for_direction(trade_size: float, is_long: bool) -> float:
    return trade_size if is_long else trade_size * -1
