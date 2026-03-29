from GlobalUtils.logger import logger
from APICaller.Perennial.perennialCallerUtils import get_all_symbols
import json
import os

def _load_config():
    """Load configuration from config.json, with fallback to defaults."""
    # __file__ is APICaller/master/MasterUtils.py → go up 3 levels to reach project root
    project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    config_path = os.path.join(project_root, 'config.json')
    try:
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                config = json.load(f)
            logger.info(f"MasterUtils - Loaded configuration from {config_path}")
            return config
        else:
            logger.warning(f"MasterUtils - config.json not found at {config_path}, using defaults")
    except Exception as e:
        logger.warning(f"MasterUtils - Failed to load config.json at {config_path}, using defaults: {e}")
    
    # Fallback defaults
    return {
        "target_tokens": [
            {"token": "BTC", "is_target": True},
            {"token": "ETH", "is_target": True},
            {"token": "SNX", "is_target": False},
            {"token": "SOL", "is_target": False},
            {"token": "W", "is_target": False},
            {"token": "WIF", "is_target": False},
            {"token": "ARB", "is_target": False},
            {"token": "BNB", "is_target": False},
            {"token": "ENA", "is_target": False},
            {"token": "DOGE", "is_target": False},
            {"token": "AVAX", "is_target": False},
            {"token": "PENDLE", "is_target": False},
            {"token": "NEAR", "is_target": False},
            {"token": "AAVE", "is_target": False},
            {"token": "ATOM", "is_target": False},
            {"token": "LINK", "is_target": False},
            {"token": "UNI", "is_target": False},
            {"token": "LTC", "is_target": False},
            {"token": "OP", "is_target": False},
            {"token": "GMX", "is_target": False},
            {"token": "PEPE", "is_target": False},
        ],
        "target_exchanges": [
            {"exchange": "Synthetix", "is_target": False},
            {"exchange": "Binance", "is_target": False},
            {"exchange": "ByBit", "is_target": True},
            {"exchange": "HMX", "is_target": False},
            {"exchange": "OKX", "is_target": False},
            {"exchange": "GMX", "is_target": False},
            {"exchange": "Perennial", "is_target": True},
        ]
    }

_CONFIG = _load_config()
TARGET_TOKENS = _CONFIG['target_tokens']
TARGET_EXCHANGES = _CONFIG['target_exchanges']

def get_target_exchanges() -> list:
    try:
        exchanges = [exchange["exchange"] for exchange in TARGET_EXCHANGES if exchange["is_target"]]
        return exchanges
    except Exception as e:
        logger.error(f"MasterAPICallerUtils - Error retrieving target exchanges: {e}")
        return []

def get_all_target_token_lists() -> list:
    try:
        binance_token_list = get_target_tokens_for_binance()
        synthetix_token_list = get_target_tokens_for_synthetix()
        bybit_token_list = get_target_tokens_for_bybit()
        hmx_token_list = get_target_tokens_for_HMX()
        gmx_token_list = get_target_tokens_for_GMX()
        okx_token_list = get_target_tokens_for_OKX()
        perennial_token_list = get_target_tokens_for_perennial()
        all_target_token_lists = [
            synthetix_token_list,
            binance_token_list,
            bybit_token_list,
            hmx_token_list,
            gmx_token_list,
            okx_token_list,
            perennial_token_list
        ]
        return all_target_token_lists
    except Exception as e:
        logger.error(f"MasterAPICallerUtils - Error retrieving all target token lists: {e}")
        return []

def get_target_tokens_for_binance() -> list:
    try:
        symbols = [token["token"] + "USDT" for token in TARGET_TOKENS if token["is_target"]]
        return symbols
    except Exception as e:
        logger.error(f"MasterAPICallerUtils - Error retrieving target tokens for Binance: {e}")
        return []

def get_target_tokens_for_OKX() -> list:
    try:
        symbols = [token["token"] + "-USDT-SWAP" for token in TARGET_TOKENS if token["is_target"]]
        return symbols
    except Exception as e:
        logger.error(f"MasterAPICallerUtils - Error retrieving target tokens for OKX: {e}")
        return []

def get_target_tokens_for_synthetix() -> list:
    try:
        symbols = [token["token"] for token in TARGET_TOKENS if token["is_target"]]
        return symbols
    except Exception as e:
        logger.error(f"MasterAPICallerUtils - Error retrieving target tokens for Synthetix: {e}")
        return []

def get_target_tokens_for_bybit() -> list:
    try:
        symbols = [token["token"] + "USDT" for token in TARGET_TOKENS if token["is_target"]]
        return symbols
    except Exception as e:
        logger.error(f"MasterAPICallerUtils - Error retrieving target tokens for ByBit: {e}")
        return []

def get_target_tokens_for_HMX() -> list:
    try:
        symbols = [token["token"] + "USD" for token in TARGET_TOKENS if token["is_target"]]
        return symbols
    except Exception as e:
        logger.error(f"MasterAPICallerUtils - Error retrieving target tokens for ByBit: {e}")
        return []

def get_target_tokens_for_GMX() -> list:
    try:
        symbols = [token["token"] for token in TARGET_TOKENS if token["is_target"]]
        return symbols
    except Exception as e:
        logger.error(f"MasterAPICallerUtils - Error retrieving target tokens for GMX: {e}")
        return []

def get_target_tokens_for_perennial() -> list:
    try:
        symbols = get_all_symbols()
        if 'mog' in symbols:
            symbols.remove('mog')
        
        symbols = [s.upper() for s in symbols]
        
        return symbols
    except Exception as e:
        logger.error(f"MasterAPICallerUtils - Error retrieving target tokens for Perennial: {e}")
        return []