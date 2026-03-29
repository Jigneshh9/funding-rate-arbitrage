"""
Configuration Validator Module
Validates all required environment variables and config values at startup.
Fails fast with clear error messages if anything is missing or invalid.
"""
import os
import json
from dotenv import load_dotenv
from GlobalUtils.logger import logger

load_dotenv()


class ConfigValidationError(Exception):
    """Raised when a required configuration is missing or invalid."""
    pass


def validate_config():
    """
    Validate all required configuration at startup.
    Raises ConfigValidationError with a clear message on failure.
    """
    errors = []
    
    # Required env vars for core functionality
    required_env_vars = {
        'BASE_PROVIDER_RPC': 'Base chain RPC endpoint',
        'ADDRESS': 'Wallet address',
        'PRIVATE_KEY': 'Wallet private key',
    }
    
    for var, description in required_env_vars.items():
        value = os.getenv(var)
        if not value:
            errors.append(f"Missing required env var: {var} ({description})")
    
    # Numeric env vars with defaults
    numeric_vars = {
        'TRADE_LEVERAGE': ('Trade leverage multiplier', 1, 50),
        'DELTA_BOUND': ('Max delta bound', 0.001, 1.0),
        'PERCENTAGE_CAPITAL_PER_TRADE': ('Percentage of capital per trade', 1, 100),
        'DEFAULT_TRADE_DURATION_HOURS': ('Default trade duration in hours', 1, 168),
        'DEFAULT_TRADE_SIZE_USD': ('Default trade size in USD', 10, 100000),
    }
    
    for var, (description, min_val, max_val) in numeric_vars.items():
        value = os.getenv(var)
        if value:
            try:
                num = float(value)
                if num < min_val or num > max_val:
                    errors.append(f"Env var {var} value {num} outside valid range [{min_val}, {max_val}] ({description})")
            except ValueError:
                errors.append(f"Env var {var} must be numeric, got: '{value}' ({description})")
    
    # Validate config.json exists and is valid
    config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config.json')
    if os.path.exists(config_path):
        try:
            with open(config_path, 'r') as f:
                config = json.load(f)
            
            # Validate target_tokens
            if 'target_tokens' not in config:
                errors.append("config.json missing 'target_tokens' array")
            elif not isinstance(config['target_tokens'], list):
                errors.append("config.json 'target_tokens' must be an array")
            else:
                for i, token in enumerate(config['target_tokens']):
                    if 'token' not in token:
                        errors.append(f"config.json target_tokens[{i}] missing 'token' key")
                    if 'is_target' not in token:
                        errors.append(f"config.json target_tokens[{i}] missing 'is_target' key")
            
            # Validate target_exchanges
            if 'target_exchanges' not in config:
                errors.append("config.json missing 'target_exchanges' array")
            elif not isinstance(config['target_exchanges'], list):
                errors.append("config.json 'target_exchanges' must be an array")
            else:
                active_count = sum(1 for e in config['target_exchanges'] if e.get('is_target', False))
                if active_count < 2:
                    errors.append(f"config.json needs at least 2 active exchanges for arbitrage, found {active_count}")
        
        except json.JSONDecodeError as e:
            errors.append(f"config.json is not valid JSON: {e}")
    else:
        errors.append(f"config.json not found at {config_path}")
    
    # Exchange-specific API keys
    # Check if targeted exchanges have required keys
    try:
        if os.path.exists(config_path):
            with open(config_path, 'r') as f:
                config = json.load(f)
            
            active_exchanges = [e['exchange'] for e in config.get('target_exchanges', []) if e.get('is_target')]
            
            if 'Binance' in active_exchanges:
                if not os.getenv('BINANCE_API_KEY') or not os.getenv('BINANCE_API_SECRET'):
                    errors.append("Binance is targeted but BINANCE_API_KEY/BINANCE_API_SECRET not set")
            
            if 'ByBit' in active_exchanges:
                if not os.getenv('BYBIT_API_KEY') or not os.getenv('BYBIT_API_SECRET'):
                    errors.append("ByBit is targeted but BYBIT_API_KEY/BYBIT_API_SECRET not set")
    except Exception:
        pass  # Config validation errors already captured above
    
    if errors:
        error_msg = "Configuration validation failed:\n" + "\n".join(f"  - {e}" for e in errors)
        logger.error(f"ConfigValidator - {error_msg}")
        raise ConfigValidationError(error_msg)
    
    logger.info("ConfigValidator - All configuration validated successfully")
    return True


def load_config() -> dict:
    """Load and return the config.json file."""
    config_path = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'config.json')
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"ConfigValidator - Failed to load config.json: {e}")
        return {}
