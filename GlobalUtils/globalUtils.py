# web3 is imported lazily inside functions that need it (see initialise_client, etc.)
import os
try:
    from dotenv import load_dotenv
except ImportError:
    load_dotenv = lambda: None  # no-op if dotenv not installed
import requests
from decimal import Decimal, InvalidOperation
from enum import Enum
from GlobalUtils.logger import *
# from APICaller.Synthetix.SynthetixUtils import get_synthetix_client
import functools
import re
import time
import json

load_dotenv()

NULL_ADDRESS = '0x0000000000000000000000000000000000000000'

BLOCKS_PER_DAY_BASE = 43200
BLOCKS_PER_HOUR_BASE = 1800

# Lazy initialization for exchange clients to avoid import-time failures
_binance_client = None

def get_global_binance_client():
    """Get the global Binance client, creating it on first access."""
    global _binance_client
    if _binance_client is None:
        try:
            from APICaller.Binance.binanceUtils import get_binance_client
            _binance_client = get_binance_client()
            logger.info("globalUtils - Binance client initialized successfully")
        except Exception as e:
            logger.error(f"globalUtils - Failed to initialize Binance client: {e}")
            raise
    return _binance_client

# Backward compatibility — other modules may reference this directly
GLOBAL_BINANCE_CLIENT = None  # Use get_global_binance_client() instead
# GLOBAL_HMX_CLIENT = get_HMX_client()

# GLOBAL_OKX_PUBLIC_CLIENT = get_okx_pub_client()
# GLOBAL_OKX_TRADING_DATA_CLIENT = get_okx_trading_data_client()
# GLOBAL_OKX_ACCOUNT_CLIENT = get_okx_account_client()
# GLOBAL_OKX_TRADE_CLIENT = get_okx_trade_client()

class EventsDirectory(Enum):
    CLOSE_ALL_POSITIONS = "close_all_positions"
    CLOSE_POSITION_PAIR = "close_position_pair"
    OPPORTUNITY_FOUND = "opportunity_found"
    POSITION_OPENED = "position_opened"
    POSITION_CLOSED = "position_closed"
    TRADE_LOGGED = "trade_logged"

# Re-export pure helpers from trade_helpers for backward compatibility
from GlobalUtils.trade_helpers import DECIMALS, get_decimals_for_symbol, normalize_symbol, adjust_trade_size_for_direction

def initialise_client():
    try:
        from web3 import Web3
        client = Web3(Web3.HTTPProvider(os.getenv('BASE_PROVIDER_RPC')))
    except Exception as e:
        logger.error(f"GlobalUtils - Error initialising Web3 client: {e}")
        return None 
    return client

def get_gas_price() -> float:
    client = initialise_client()
    if client:
        try:
            price_in_wei = client.eth.gas_price
            price_in_gwei = client.from_wei(price_in_wei, 'gwei')
            return price_in_gwei
        except Exception as e:
            logger.error(f"GlobalUtils - Error fetching gas price: {e}")
            return None
    return 0.0

def get_asset_price(symbol: str) -> float:
    """Get the current price of an asset in USD using Binance ticker.
    Falls back to 0.0 on failure -- callers should handle this gracefully.
    """
    try:
        client = get_global_binance_client()
        ticker = client.get_symbol_ticker(symbol=symbol.upper() + 'USDT')
        price = float(ticker['price'])
        return price
    except Exception as e:
        logger.error(f"GlobalUtils - Error fetching price for {symbol} from Binance: {e}")
        return 0.0

def get_asset_amount_for_given_dollar_amount(asset: str, dollar_amount: float) -> float:
    """Convert a USD dollar amount to an equivalent asset amount using live price."""
    try:
        asset_price = get_asset_price(asset)
        if asset_price <= 0:
            logger.error(f"GlobalUtils - Cannot convert: price for {asset} is zero or unavailable")
            return 0.0
        asset_amount = dollar_amount / asset_price
        return asset_amount
    except ZeroDivisionError:
        logger.error(f"GlobalUtils - Error calculating asset amount for {asset}: Price is zero")
    return 0.0

def get_dollar_amount_for_given_asset_amount(asset: str, asset_amount: float) -> float:
    """Convert an asset amount to USD using live price."""
    try:
        asset_price = get_asset_price(asset)
        dollar_amount = asset_amount * asset_price
        return dollar_amount
    except Exception as e:
        logger.error(f"GlobalUtils - Error converting asset amount to dollar amount for {asset}: {e}")
    return 0.0

def get_base_block_number_by_timestamp(timestamp: int) -> int:
    apikey = os.getenv('BASESCAN_API_KEY')
    url = "https://api.basescan.org/api"
    params = {
        'module': 'block',
        'action': 'getblocknobytime',
        'timestamp': timestamp,
        'closest': 'before',
        'apikey': apikey
    }

    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        data = response.json()
        if data.get('status') == '1' and data.get('message') == 'OK':
            return int(data.get('result'))
        else:
            logger.info(f"GlobalUtils - Basescan API Error: {data}")
            return -1
    except requests.RequestException as e:
        print("GlobalUtils - Basescan API HTTP Request failed:", e)
        return -1

def get_base_block_number() -> int:
    try:
        client = initialise_client()
        block_number = client.eth.block_number
        return block_number
    except Exception as e:
        logger.error(f'GlobalUtils - Error while calling current block number for BASE network: {e}')
        return None

def get_binance_funding_event_schedule(current_block_number: int) -> list:
    try:
        coordination_block = 13664526
        interval_in_blocks = 14400

        intervals_since_last_event = (current_block_number - coordination_block) // interval_in_blocks
        next_funding_event = coordination_block + (intervals_since_last_event + 1) * interval_in_blocks
        next_three_funding_events = [next_funding_event + i * interval_in_blocks for i in range(3)]
        return next_three_funding_events

    except Exception as e:
        logger.error(f'GlobalUtils - Error while calling current block number for BASE network: {e}')
        return None

def normalize_funding_rate_to_8hrs(rate: float, hours: int) -> float:
    try:
        rate_per_hour = rate / hours
        normalized_rate = rate_per_hour * 8
        return normalized_rate

    except Exception as e:
        logger.error(f'GlobalUtils - Error while normalizing funding rate to 8hrs. Function inputs: rate={rate}, hours={hours} {e}')
        return None

def is_transaction_hash(tx_hash) -> bool:
    # Regular expression to match an Ethereum transaction hash
    pattern = r'^0x[a-fA-F0-9]{64}$'
    return re.match(pattern, tx_hash) is not None

def get_milliseconds_until_given_timestamp(timestamp: int) -> int:
    current_time = int(time.time() * 1000)
    return timestamp - current_time

def get_milliseconds_until_given_timestamp_timezone(timestamp: int, shift_timezone: bool) -> int:
    current_time = int(time.time() * 1000)
    if shift_timezone:
        current_time -= time.timezone * 1000
    return timestamp - current_time

def deco_retry(retry: int = 5, retry_sleep: int = 3):
    def deco_func(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            _retry = 5 if callable(retry) else retry
            _result = None
            for _i in range(1, _retry + 1):
                try:
                    _result = func(*args, **kwargs)
                    break

                except Exception as e:
                    logger.warning(f"{func.__name__}: {_i} :{e}")
                    if _i == _retry:
                        raise

                time.sleep(retry_sleep)
            return _result

        return wrapper

    return deco_func(retry) if callable(retry) else deco_func

def get_arbitrum_usdc_balance_global():
    try:
        provider = os.getenv('ARBITRUM_PROVIDER_RPC')
        from web3 import Web3
        web3_obj = Web3(Web3.HTTPProvider(provider))
        usdc_address = '0xaf88d065e77c8cC2239327C5EDb3A432268e5831'
        with open('GlobalUtils/ABIs/USDCArbitrum.json', 'r') as abi_file:
            token_abi = json.load(abi_file)

        wallet_address = os.getenv('ADDRESS')
        contract = web3_obj.eth.contract(address=usdc_address, abi=token_abi)
        balance = contract.functions.balanceOf(wallet_address).call()
        decimals = 6
        human_readable_balance = balance / (10 ** decimals)

        return human_readable_balance
    
    except Exception as e:
        logger.error(f'GlobalUtils - Failed to fetch USDC balance for address {wallet_address}. Error: {e}')
        return None

def get_price_coingecko(symbol):
    url = f'https://api.coingecko.com/api/v3/simple/price'
    params = {'ids': symbol, 'vs_currencies': 'usd'}
    response = requests.get(url, params=params)
    data = response.json()
    return data.get(symbol, {}).get('usd', 'Price not found')

SYMBOL_COINGECKO_MAP = {
    'ETH': 'ethereum',
    'BTC': 'bitcoin',
    'SOL': 'solana',
    'MATIC': 'matic-network',
    'TIA': 'celestia',
    "RLB": "rollbit-coin",
    "LINK": "chainlink",
    "BNB": "binancecoin",
    "XRP": "ripple",
    "ARB": "arbitrum",
    "JUP": "jupiter-exchange-solana",
    "MKR": "maker",
    "DOGE":"dogecoin",
    "AERO":  "aerodrome-finance",
    "POPCAT":  "popcat"
}