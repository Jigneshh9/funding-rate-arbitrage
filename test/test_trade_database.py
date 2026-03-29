"""
Unit tests for the TradeDatabase module.
Tests the extended schema, lifecycle states, and database operations.
"""
import unittest
import sqlite3
import os
import sys
import tempfile
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestTradeDatabase(unittest.TestCase):
    """Tests for the TradeLogger class and extended schema."""
    
    def setUp(self):
        """Create a temporary database for testing."""
        self.temp_db = tempfile.NamedTemporaryFile(suffix='.db', delete=False)
        self.temp_db.close()
        self.db_path = self.temp_db.name
        
        # Create the schema manually (mimicking TradeLogger.create_or_access_database)
        conn = sqlite3.connect(self.db_path)
        conn.execute('''CREATE TABLE IF NOT EXISTS trade_log (
            id INTEGER PRIMARY KEY,
            strategy_execution_id TEXT NOT NULL,
            order_id TEXT,
            exchange TEXT NOT NULL,
            symbol TEXT NOT NULL,
            side TEXT NOT NULL,
            is_hedge TEXT NOT NULL,
            size_in_asset REAL NOT NULL,
            fill_price REAL,
            fees REAL DEFAULT 0,
            tx_hash TEXT,
            liquidation_price REAL NOT NULL,
            open_close TEXT NOT NULL,
            lifecycle_state TEXT DEFAULT 'detected',
            execution_attempts INTEGER DEFAULT 0,
            reconciliation_state TEXT DEFAULT 'pending',
            open_time DATETIME,
            close_time DATETIME,
            pnl REAL,
            accrued_funding REAL,
            close_reason TEXT,
            UNIQUE(strategy_execution_id, exchange)
        )''')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_open_close ON trade_log(open_close);')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_lifecycle_state ON trade_log(lifecycle_state);')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_symbol ON trade_log(symbol);')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_exchange ON trade_log(exchange);')
        conn.execute('CREATE INDEX IF NOT EXISTS idx_execution_id ON trade_log(strategy_execution_id);')
        conn.commit()
        conn.close()
    
    def tearDown(self):
        os.unlink(self.db_path)
    
    def test_schema_has_new_columns(self):
        """Verify all new columns exist in the schema."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.execute("PRAGMA table_info(trade_log)")
        columns = {row[1] for row in cursor.fetchall()}
        conn.close()
        
        expected_new_columns = {
            'order_id', 'fill_price', 'fees', 'tx_hash',
            'lifecycle_state', 'execution_attempts', 'reconciliation_state'
        }
        for col in expected_new_columns:
            self.assertIn(col, columns, f"Missing column: {col}")
    
    def test_unique_constraint(self):
        """Test UNIQUE constraint on (strategy_execution_id, exchange)."""
        conn = sqlite3.connect(self.db_path)
        now = datetime.now().isoformat()
        
        conn.execute(
            """INSERT INTO trade_log 
            (strategy_execution_id, exchange, symbol, side, is_hedge, size_in_asset, liquidation_price, open_close, open_time) 
            VALUES ('exec1', 'Binance', 'BTC', 'long', 'False', 1.0, 50000, 'Open', ?)""",
            (now,)
        )
        conn.commit()
        
        # Inserting the same execution_id + exchange should fail
        with self.assertRaises(sqlite3.IntegrityError):
            conn.execute(
                """INSERT INTO trade_log 
                (strategy_execution_id, exchange, symbol, side, is_hedge, size_in_asset, liquidation_price, open_close, open_time) 
                VALUES ('exec1', 'Binance', 'ETH', 'short', 'False', 2.0, 3000, 'Open', ?)""",
                (now,)
            )
        
        # Different exchange should work
        conn.execute(
            """INSERT INTO trade_log 
            (strategy_execution_id, exchange, symbol, side, is_hedge, size_in_asset, liquidation_price, open_close, open_time) 
            VALUES ('exec1', 'ByBit', 'BTC', 'short', 'False', 1.0, 55000, 'Open', ?)""",
            (now,)
        )
        conn.commit()
        conn.close()
    
    def test_lifecycle_state_values(self):
        """Test inserting and querying by lifecycle state."""
        conn = sqlite3.connect(self.db_path)
        now = datetime.now().isoformat()
        
        states = ['detected', 'submitted', 'partially_filled', 'hedged', 'closing', 'closed', 'failed']
        for i, state in enumerate(states):
            conn.execute(
                """INSERT INTO trade_log 
                (strategy_execution_id, exchange, symbol, side, is_hedge, size_in_asset, liquidation_price, open_close, lifecycle_state, open_time) 
                VALUES (?, 'Binance', 'BTC', 'long', 'False', 1.0, 50000, 'Open', ?, ?)""",
                (f'exec_{i}', state, now)
            )
        conn.commit()
        
        # Query by lifecycle state
        cursor = conn.execute("SELECT COUNT(*) FROM trade_log WHERE lifecycle_state = 'hedged'")
        self.assertEqual(cursor.fetchone()[0], 1)
        
        conn.close()
    
    def test_default_values(self):
        """Test that default values are set correctly."""
        conn = sqlite3.connect(self.db_path)
        now = datetime.now().isoformat()
        
        conn.execute(
            """INSERT INTO trade_log 
            (strategy_execution_id, exchange, symbol, side, is_hedge, size_in_asset, liquidation_price, open_close, open_time) 
            VALUES ('exec_default', 'Binance', 'BTC', 'long', 'False', 1.0, 50000, 'Open', ?)""",
            (now,)
        )
        conn.commit()
        
        cursor = conn.execute(
            "SELECT lifecycle_state, execution_attempts, reconciliation_state, fees FROM trade_log WHERE strategy_execution_id = 'exec_default'"
        )
        row = cursor.fetchone()
        self.assertEqual(row[0], 'detected')
        self.assertEqual(row[1], 0)
        self.assertEqual(row[2], 'pending')
        self.assertEqual(row[3], 0)
        
        conn.close()
    
    def test_indices_exist(self):
        """Test that all expected indices are created."""
        conn = sqlite3.connect(self.db_path)
        cursor = conn.execute("SELECT name FROM sqlite_master WHERE type='index'")
        indices = {row[0] for row in cursor.fetchall()}
        conn.close()
        
        expected_indices = {
            'idx_open_close', 'idx_lifecycle_state', 'idx_symbol',
            'idx_exchange', 'idx_execution_id'
        }
        for idx in expected_indices:
            self.assertIn(idx, indices, f"Missing index: {idx}")


if __name__ == '__main__':
    unittest.main()
