"""
Exchange Adapter Interface
Defines a common interface that all exchange integrations must implement.
"""
from abc import ABC, abstractmethod


class ExchangeAdapter(ABC):
    """
    Abstract base class for exchange integrations.
    All exchange-specific controllers should implement this interface
    to ensure consistent behavior across exchanges.
    """
    
    @abstractmethod
    def get_funding_rates(self, symbols: list) -> dict:
        """
        Get current funding rates for the given symbols.
        
        Returns:
            dict mapping symbol -> {funding_rate, next_funding_time, ...}
        """
        pass
    
    @abstractmethod
    def execute_trade(self, opportunity: dict, is_long: bool, trade_size: float) -> dict:
        """
        Execute a trade on this exchange.
        
        Args:
            opportunity: The opportunity dict from matching engine
            is_long: True for long position, False for short
            trade_size: Size of the trade in collateral units
        
        Returns:
            dict with position data including size, entry price, liquidation price, etc.
            Returns None on failure.
        """
        pass
    
    @abstractmethod
    def close_position(self, symbol: str, reason: str) -> bool:
        """
        Close an open position on this exchange.
        
        Args:
            symbol: The trading symbol
            reason: Reason for closing
        
        Returns:
            True on success, False on failure
        """
        pass
    
    @abstractmethod
    def get_available_collateral(self) -> float:
        """
        Get the available collateral on this exchange.
        
        Returns:
            Available collateral in USD
        """
        pass
    
    @abstractmethod
    def is_already_position_open(self) -> bool:
        """
        Check if there's an existing open position on this exchange.
        
        Returns:
            True if position is open, False otherwise
        """
        pass
    
    @abstractmethod
    def get_open_position(self, symbol: str) -> dict:
        """
        Get details of an open position for a symbol.
        
        Args:
            symbol: The trading symbol
        
        Returns:
            dict with position details, or None if no open position
        """
        pass
