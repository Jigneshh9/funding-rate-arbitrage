"""
Unit tests for the RiskManager module.
Tests exposure limits, leverage checks, daily loss caps, kill switch, and funding horizon.
"""
import unittest
import sqlite3
import os
import sys
import tempfile
import warnings
from datetime import datetime, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestRiskManager(unittest.TestCase):
    """Tests for the RiskManager class."""
    
    def setUp(self):
        """Set up a temp db and risk manager with known config."""
        self.temp_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        self.temp_db.close()
        
        # Create schema
        with sqlite3.connect(self.temp_db.name) as conn:
            conn.execute('''CREATE TABLE IF NOT EXISTS trade_log (
                id INTEGER PRIMARY KEY,
                strategy_execution_id TEXT NOT NULL,
                exchange TEXT NOT NULL,
                symbol TEXT NOT NULL,
                side TEXT NOT NULL,
                is_hedge TEXT NOT NULL,
                size_in_asset REAL NOT NULL,
                fill_price REAL,
                liquidation_price REAL NOT NULL,
                open_close TEXT NOT NULL,
                open_time DATETIME,
                close_time DATETIME,
                pnl REAL,
                accrued_funding REAL,
                close_reason TEXT
            )''')
            conn.commit()
        
        # Set env vars for risk manager
        os.environ['MAX_EXPOSURE_PER_ASSET_USD'] = '5000'
        os.environ['MAX_EXPOSURE_PER_EXCHANGE_USD'] = '10000'
        os.environ['MAX_TOTAL_EXPOSURE_USD'] = '20000'
        os.environ['MAX_LEVERAGE'] = '10'
        os.environ['DAILY_LOSS_CAP_USD'] = '500'
        os.environ['EMERGENCY_KILL_SWITCH'] = 'false'
        os.environ['MAX_FUNDING_HORIZON_HOURS'] = '72'
        os.environ['TRADE_LEVERAGE'] = '5'
        
        warnings.simplefilter('ignore', ResourceWarning)
        
        from GlobalUtils.risk_manager import RiskManager
        self.rm = RiskManager(db_path=self.temp_db.name)
    
    def tearDown(self):
        # Explicitly close any SQLite connections held by RiskManager
        import sqlite3
        import gc
        # Drop reference to RiskManager so its connections can be GC'd
        del self.rm
        gc.collect()
        # Also force-close any lingering connections to this specific file
        try:
            conn = sqlite3.connect(self.temp_db.name)
            conn.close()
        except Exception:
            pass
        try:
            os.unlink(self.temp_db.name)
        except PermissionError:
            pass  # Windows file locking; temp file will be cleaned up later
    
    def test_kill_switch_inactive(self):
        self.assertFalse(self.rm.is_kill_switch_active())
    
    def test_kill_switch_active(self):
        self.rm.emergency_kill_switch = True
        self.assertTrue(self.rm.is_kill_switch_active())
    
    def test_kill_switch_blocks_trades(self):
        self.rm.emergency_kill_switch = True
        opportunity = {'symbol': 'BTC', 'long_exchange': 'Binance', 'short_exchange': 'ByBit'}
        passed, reason = self.rm.check_all_pre_trade(opportunity, 1000)
        self.assertFalse(passed)
        self.assertIn("kill switch", reason)
    
    def test_exposure_under_limit(self):
        opportunity = {'symbol': 'BTC', 'long_exchange': 'Binance', 'short_exchange': 'ByBit'}
        passed, reason = self.rm.check_exposure_limits(opportunity, 1000)
        self.assertTrue(passed)
    
    def test_exposure_exceeds_asset_limit(self):
        # Insert an existing open position
        with sqlite3.connect(self.temp_db.name) as conn:
            conn.execute(
                "INSERT INTO trade_log (strategy_execution_id, exchange, symbol, side, is_hedge, size_in_asset, fill_price, liquidation_price, open_close, open_time) VALUES ('exec1', 'Binance', 'BTC', 'long', 'False', 0.07, 65000.0, 0, 'Open', ?)",
                (datetime.now().isoformat(),)
            )
            conn.commit()
        
        opportunity = {'symbol': 'BTC', 'long_exchange': 'Binance', 'short_exchange': 'ByBit'}
        passed, reason = self.rm.check_exposure_limits(opportunity, 1000)
        self.assertFalse(passed)
        self.assertIn("Asset exposure", reason)
    
    def test_leverage_within_limit(self):
        os.environ['TRADE_LEVERAGE'] = '5'
        opportunity = {'symbol': 'BTC'}
        passed, _ = self.rm.check_leverage(opportunity)
        self.assertTrue(passed)
    
    def test_leverage_exceeds_limit(self):
        os.environ['TRADE_LEVERAGE'] = '15'
        opportunity = {'symbol': 'BTC'}
        passed, reason = self.rm.check_leverage(opportunity)
        self.assertFalse(passed)
        self.assertIn("Leverage", reason)
    
    def test_daily_loss_under_cap(self):
        passed, _ = self.rm.check_daily_loss()
        self.assertTrue(passed)
    
    def test_daily_loss_exceeds_cap(self):
        now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        with sqlite3.connect(self.temp_db.name) as conn:
            conn.execute(
                "INSERT INTO trade_log (strategy_execution_id, exchange, symbol, side, is_hedge, size_in_asset, fill_price, liquidation_price, open_close, open_time, close_time, pnl, accrued_funding, close_reason) VALUES ('exec1', 'Binance', 'BTC', 'long', 'False', 0.01, 60000.0, 0, 'Close', ?, ?, -600, 0, 'pnl_exit')",
                (now, now)
            )
            conn.commit()
        
        passed, reason = self.rm.check_daily_loss()
        self.assertFalse(passed)
        self.assertIn("Daily loss cap", reason)
    
    def test_funding_horizon_within_limit(self):
        open_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')
        self.assertFalse(self.rm.should_force_exit(open_time))
    
    def test_funding_horizon_exceeded(self):
        old_time = (datetime.now() - timedelta(hours=100)).strftime('%Y-%m-%d %H:%M:%S.%f')
        self.assertTrue(self.rm.should_force_exit(old_time))
    
    def test_all_checks_pass(self):
        opportunity = {'symbol': 'BTC', 'long_exchange': 'Binance', 'short_exchange': 'ByBit'}
        passed, reason = self.rm.check_all_pre_trade(opportunity, 1000)
        self.assertTrue(passed)
        self.assertIn("passed", reason)


if __name__ == '__main__':
    unittest.main()
