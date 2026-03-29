"""
Risk Management Module
Provides exposure limits, leverage checks, loss caps, kill switch, and funding horizon enforcement.
"""
import os
import time
import json
import sqlite3
from datetime import datetime, timedelta
from dotenv import load_dotenv
from GlobalUtils.logger import logger

load_dotenv()


class RiskManager:
    """
    Centralized risk management with configurable limits.
    Integrated as a pre-trade gate in MasterPositionController.
    """
    
    def __init__(self, db_path='trades.db'):
        self.db_path = db_path
        
        # Exposure limits
        self.max_exposure_per_asset_usd = float(os.getenv('MAX_EXPOSURE_PER_ASSET_USD', '5000'))
        self.max_exposure_per_exchange_usd = float(os.getenv('MAX_EXPOSURE_PER_EXCHANGE_USD', '10000'))
        self.max_total_exposure_usd = float(os.getenv('MAX_TOTAL_EXPOSURE_USD', '20000'))
        
        # Leverage and liquidation
        self.max_leverage = float(os.getenv('MAX_LEVERAGE', '10'))
        self.min_liquidation_buffer_pct = float(os.getenv('MIN_LIQUIDATION_BUFFER_PCT', '15'))
        
        # Loss caps
        self.daily_loss_cap_usd = float(os.getenv('DAILY_LOSS_CAP_USD', '500'))
        self.emergency_kill_switch = os.getenv('EMERGENCY_KILL_SWITCH', 'false').lower() == 'true'
        
        # Funding horizon
        self.max_funding_horizon_hours = float(os.getenv('MAX_FUNDING_HORIZON_HOURS', '72'))
        
        # Delta bounds
        self.delta_bound = float(os.getenv('DELTA_BOUND', '0.03'))
        self.rehedge_threshold = float(os.getenv('REHEDGE_THRESHOLD', '0.05'))
        
        logger.info(f"RiskManager initialized - Max exposure/asset: ${self.max_exposure_per_asset_usd}, "
                     f"Max leverage: {self.max_leverage}x, Daily loss cap: ${self.daily_loss_cap_usd}")

    def is_kill_switch_active(self) -> bool:
        """Check if the emergency kill switch is engaged."""
        if self.emergency_kill_switch:
            logger.warning("RiskManager - EMERGENCY KILL SWITCH IS ACTIVE. No new trades allowed.")
            return True
        return False
    
    def check_all_pre_trade(self, opportunity: dict, trade_size_usd: float) -> tuple:
        """
        Run all pre-trade risk checks. Returns (passed: bool, reason: str).
        """
        if self.is_kill_switch_active():
            return False, "Emergency kill switch is active"
        
        passed, reason = self.check_exposure_limits(opportunity, trade_size_usd)
        if not passed:
            return False, reason
        
        passed, reason = self.check_leverage(opportunity)
        if not passed:
            return False, reason
        
        passed, reason = self.check_daily_loss()
        if not passed:
            return False, reason
        
        return True, "All risk checks passed"
    
    def check_exposure_limits(self, opportunity: dict, trade_size_usd: float) -> tuple:
        """
        Check that adding this trade would not exceed exposure limits.
        Returns (passed: bool, reason: str).
        """
        try:
            symbol = opportunity['symbol']
            long_exchange = opportunity['long_exchange']
            short_exchange = opportunity['short_exchange']
            
            # Check per-asset exposure
            current_asset_exposure = self._get_current_exposure_for_asset(symbol)
            new_asset_exposure = current_asset_exposure + trade_size_usd
            if new_asset_exposure > self.max_exposure_per_asset_usd:
                reason = (f"Asset exposure limit exceeded for {symbol}: "
                         f"current=${current_asset_exposure:.2f} + new=${trade_size_usd:.2f} "
                         f"> limit=${self.max_exposure_per_asset_usd:.2f}")
                logger.warning(f"RiskManager - {reason}")
                return False, reason
            
            # Check per-exchange exposure
            for exchange in [long_exchange, short_exchange]:
                current_exchange_exposure = self._get_current_exposure_for_exchange(exchange)
                new_exchange_exposure = current_exchange_exposure + (trade_size_usd / 2)
                if new_exchange_exposure > self.max_exposure_per_exchange_usd:
                    reason = (f"Exchange exposure limit exceeded for {exchange}: "
                             f"current=${current_exchange_exposure:.2f} + new=${trade_size_usd / 2:.2f} "
                             f"> limit=${self.max_exposure_per_exchange_usd:.2f}")
                    logger.warning(f"RiskManager - {reason}")
                    return False, reason
            
            # Check total exposure
            current_total_exposure = self._get_total_exposure()
            new_total_exposure = current_total_exposure + trade_size_usd
            if new_total_exposure > self.max_total_exposure_usd:
                reason = (f"Total exposure limit exceeded: "
                         f"current=${current_total_exposure:.2f} + new=${trade_size_usd:.2f} "
                         f"> limit=${self.max_total_exposure_usd:.2f}")
                logger.warning(f"RiskManager - {reason}")
                return False, reason
            
            return True, "Exposure limits OK"
        
        except Exception as e:
            logger.error(f"RiskManager - Error checking exposure limits: {e}")
            return False, f"Error checking exposure: {e}"
    
    def check_leverage(self, opportunity: dict) -> tuple:
        """Check that configured leverage doesn't exceed maximum."""
        try:
            configured_leverage = float(os.getenv('TRADE_LEVERAGE', '5'))
            if configured_leverage > self.max_leverage:
                reason = f"Leverage {configured_leverage}x exceeds max {self.max_leverage}x"
                logger.warning(f"RiskManager - {reason}")
                return False, reason
            return True, "Leverage OK"
        except Exception as e:
            logger.error(f"RiskManager - Error checking leverage: {e}")
            return False, f"Error checking leverage: {e}"
    
    def check_daily_loss(self) -> tuple:
        """Check if daily realized losses exceed the cap."""
        try:
            daily_loss = self._get_daily_realized_loss()
            if daily_loss >= self.daily_loss_cap_usd:
                reason = (f"Daily loss cap reached: ${daily_loss:.2f} >= ${self.daily_loss_cap_usd:.2f}")
                logger.warning(f"RiskManager - {reason}")
                return False, reason
            return True, f"Daily loss OK: ${daily_loss:.2f} < ${self.daily_loss_cap_usd:.2f}"
        except Exception as e:
            logger.error(f"RiskManager - Error checking daily loss: {e}")
            return False, f"Error checking daily loss: {e}"
    
    def should_force_exit(self, open_time_str: str) -> bool:
        """Check if a position has exceeded the max funding horizon."""
        try:
            open_time = datetime.strptime(open_time_str, '%Y-%m-%d %H:%M:%S.%f')
            elapsed_hours = (datetime.now() - open_time).total_seconds() / 3600
            if elapsed_hours > self.max_funding_horizon_hours:
                logger.warning(
                    f"RiskManager - Position open for {elapsed_hours:.1f}h exceeds "
                    f"max horizon of {self.max_funding_horizon_hours}h. Forcing exit."
                )
                return True
            return False
        except Exception as e:
            logger.error(f"RiskManager - Error checking funding horizon: {e}")
            return False
    
    def check_delta_drift(self, positions: dict, exchanges: list) -> tuple:
        """
        Enhanced delta drift check with re-hedging recommendation.
        Returns (within_bounds: bool, needs_rehedge: bool, delta: float).
        """
        try:
            total_notional = 0
            net_delta = 0
            for exchange in exchanges:
                pos = positions.get(exchange)
                if not pos:
                    return False, False, 0.0
                size = float(pos['size_in_asset'])
                if pos['side'].upper() == 'SHORT':
                    size = -size
                net_delta += size
                total_notional += abs(float(pos['size_in_asset']))
            
            if total_notional == 0:
                return True, False, 0.0
            
            relative_delta = abs(net_delta) / total_notional
            within_bounds = relative_delta <= self.delta_bound
            needs_rehedge = relative_delta > self.rehedge_threshold
            
            if needs_rehedge:
                logger.warning(f"RiskManager - Delta drift {relative_delta:.4f} exceeds rehedge threshold {self.rehedge_threshold}")
            elif not within_bounds:
                logger.warning(f"RiskManager - Delta drift {relative_delta:.4f} exceeds bound {self.delta_bound}")
            
            return within_bounds, needs_rehedge, relative_delta
            
        except Exception as e:
            logger.error(f"RiskManager - Error checking delta drift: {e}")
            return False, False, 0.0
    
    # Private helpers
    
    def _get_current_exposure_for_asset(self, symbol: str) -> float:
        """Get current USD notional exposure for a given asset."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                # Multiply size_in_asset by fill_price to get USD notional
                # Fall back to size_in_asset if fill_price is NULL (legacy rows)
                cursor.execute(
                    """SELECT COALESCE(SUM(ABS(size_in_asset) * COALESCE(fill_price, 1)), 0) 
                       FROM trade_log WHERE symbol = ? AND open_close = 'Open'""",
                    (symbol,)
                )
                return float(cursor.fetchone()[0])
        except Exception as e:
            logger.error(f"RiskManager - Error getting asset exposure for {symbol}: {e}")
            return 0.0
    
    def _get_current_exposure_for_exchange(self, exchange: str) -> float:
        """Get current USD notional exposure for a given exchange."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """SELECT COALESCE(SUM(ABS(size_in_asset) * COALESCE(fill_price, 1)), 0) 
                       FROM trade_log WHERE exchange = ? AND open_close = 'Open'""",
                    (exchange,)
                )
                return float(cursor.fetchone()[0])
        except Exception as e:
            logger.error(f"RiskManager - Error getting exchange exposure for {exchange}: {e}")
            return 0.0
    
    def _get_total_exposure(self) -> float:
        """Get total USD notional exposure across all open positions."""
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """SELECT COALESCE(SUM(ABS(size_in_asset) * COALESCE(fill_price, 1)), 0) 
                       FROM trade_log WHERE open_close = 'Open'"""
                )
                return float(cursor.fetchone()[0])
        except Exception as e:
            logger.error(f"RiskManager - Error getting total exposure: {e}")
            return 0.0
    
    def _get_daily_realized_loss(self) -> float:
        """Get total realized losses for today."""
        try:
            today_start = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute(
                    """SELECT COALESCE(SUM(pnl), 0) FROM trade_log 
                       WHERE open_close = 'Close' AND pnl < 0 
                       AND close_time >= ?""",
                    (today_start.strftime('%Y-%m-%d %H:%M:%S'),)
                )
                return abs(float(cursor.fetchone()[0]))
        except Exception as e:
            logger.error(f"RiskManager - Error getting daily loss: {e}")
            return 0.0
