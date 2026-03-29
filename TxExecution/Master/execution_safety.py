"""
Execution Safety Module
Provides pre-trade validation, idempotent order handling, and spread deterioration checks.
"""
import uuid
import time
from GlobalUtils.logger import logger


def generate_order_id() -> str:
    """Generate a unique idempotency key for each trade attempt."""
    return str(uuid.uuid4())


def validate_pre_trade_spread(opportunity: dict, current_rates: dict, max_deterioration: float = 0.0005) -> bool:
    """
    Re-validate the funding rate spread hasn't deteriorated beyond threshold
    between signal detection and order submission.
    
    Args:
        opportunity: The original opportunity dict from matching engine
        current_rates: Fresh funding rates dict keyed by exchange -> rate
        max_deterioration: Max allowed spread deterioration (absolute)
    
    Returns:
        True if spread is still acceptable, False if deteriorated too much
    """
    try:
        original_spread = abs(
            float(opportunity['short_exchange_funding_rate_8hr']) - 
            float(opportunity['long_exchange_funding_rate_8hr'])
        )
        
        long_exchange = opportunity['long_exchange']
        short_exchange = opportunity['short_exchange']
        
        current_long_rate = current_rates.get(long_exchange)
        current_short_rate = current_rates.get(short_exchange)
        
        if current_long_rate is None or current_short_rate is None:
            logger.error(f"ExecutionSafety - Cannot validate spread: missing current rates for {long_exchange} or {short_exchange}")
            return False
        
        current_spread = abs(float(current_short_rate) - float(current_long_rate))
        deterioration = original_spread - current_spread
        
        if deterioration > max_deterioration:
            logger.warning(
                f"ExecutionSafety - Spread deteriorated beyond threshold. "
                f"Original: {original_spread:.6f}, Current: {current_spread:.6f}, "
                f"Deterioration: {deterioration:.6f}, Max allowed: {max_deterioration:.6f}"
            )
            return False
        
        logger.info(f"ExecutionSafety - Spread validation passed. Deterioration: {deterioration:.6f}")
        return True
        
    except Exception as e:
        logger.error(f"ExecutionSafety - Error validating pre-trade spread: {e}")
        return False


def execute_with_retry(execute_func, max_retries: int = 2, base_delay: float = 1.0, **kwargs):
    """
    Execute a trade function with exponential backoff retry logic.
    
    Args:
        execute_func: The trade execution function to call
        max_retries: Maximum number of retry attempts
        base_delay: Base delay in seconds (doubled on each retry)
        **kwargs: Arguments to pass to execute_func
    
    Returns:
        The result of the trade execution, or None on total failure
    """
    last_exception = None
    for attempt in range(max_retries + 1):
        try:
            result = execute_func(**kwargs)
            if result is not None:
                return result
            logger.warning(f"ExecutionSafety - Attempt {attempt + 1}/{max_retries + 1} returned None")
        except Exception as e:
            last_exception = e
            logger.warning(f"ExecutionSafety - Attempt {attempt + 1}/{max_retries + 1} failed: {e}")
        
        if attempt < max_retries:
            delay = base_delay * (2 ** attempt)
            logger.info(f"ExecutionSafety - Retrying in {delay}s...")
            time.sleep(delay)
    
    logger.error(f"ExecutionSafety - All {max_retries + 1} attempts failed. Last error: {last_exception}")
    return None


class OrderTracker:
    """
    Tracks order IDs and execution state to prevent duplicate exposure
    from retries.
    """
    def __init__(self):
        self._active_orders = {}  # order_id -> {exchange, symbol, side, status}
    
    def register_order(self, order_id: str, exchange: str, symbol: str, side: str):
        """Register a new order attempt."""
        self._active_orders[order_id] = {
            'exchange': exchange,
            'symbol': symbol,
            'side': side,
            'status': 'submitted',
            'timestamp': time.time()
        }
        logger.info(f"ExecutionSafety - Registered order {order_id} for {symbol} on {exchange} ({side})")
    
    def mark_filled(self, order_id: str):
        """Mark an order as filled."""
        if order_id in self._active_orders:
            self._active_orders[order_id]['status'] = 'filled'
    
    def mark_failed(self, order_id: str):
        """Mark an order as failed."""
        if order_id in self._active_orders:
            self._active_orders[order_id]['status'] = 'failed'
    
    def is_duplicate(self, exchange: str, symbol: str, side: str) -> bool:
        """
        Check if there's already an active (submitted/filled) order 
        for the same exchange/symbol/side combination.
        """
        for order in self._active_orders.values():
            if (order['exchange'] == exchange and 
                order['symbol'] == symbol and 
                order['side'] == side and 
                order['status'] in ('submitted', 'filled')):
                return True
        return False
    
    def clear_completed(self):
        """Remove completed/failed orders older than 1 hour."""
        cutoff = time.time() - 3600
        self._active_orders = {
            oid: details for oid, details in self._active_orders.items()
            if details['status'] == 'submitted' or details['timestamp'] > cutoff
        }
