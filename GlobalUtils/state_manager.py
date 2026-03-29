"""
State Manager Module
Persists strategy state and enables crash recovery for orphaned legs.
"""
import json
import os
import sqlite3
import time
from datetime import datetime
from GlobalUtils.logger import logger


STATE_FILE = 'strategy_state.json'


class StateManager:
    """
    Persists strategy state to disk and detects orphaned legs on startup.
    """
    
    def __init__(self, state_file=STATE_FILE, db_path='trades.db'):
        self.state_file = state_file
        self.db_path = db_path
        self._state = self._load_state()
    
    def _load_state(self) -> dict:
        """Load persisted state from disk."""
        try:
            if os.path.exists(self.state_file):
                with open(self.state_file, 'r') as f:
                    state = json.load(f)
                logger.info(f"StateManager - Loaded state from {self.state_file}")
                return state
        except Exception as e:
            logger.error(f"StateManager - Error loading state: {e}")
        return {
            'active_pairs': [],
            'last_scan_time': None,
            'startup_count': 0,
            'last_startup': None
        }
    
    def save_state(self):
        """Save current state to disk."""
        try:
            self._state['last_save_time'] = datetime.now().isoformat()
            with open(self.state_file, 'w') as f:
                json.dump(self._state, f, indent=2)
            logger.info(f"StateManager - State saved to {self.state_file}")
        except Exception as e:
            logger.error(f"StateManager - Error saving state: {e}")
    
    def record_startup(self):
        """Record a new startup event."""
        self._state['startup_count'] = self._state.get('startup_count', 0) + 1
        self._state['last_startup'] = datetime.now().isoformat()
        self.save_state()
        logger.info(f"StateManager - Startup #{self._state['startup_count']} recorded")
    
    def record_scan(self):
        """Record the last scan time."""
        self._state['last_scan_time'] = datetime.now().isoformat()
    
    def add_active_pair(self, execution_id: str, symbol: str, long_exchange: str, short_exchange: str):
        """Track an active trading pair."""
        pair = {
            'execution_id': execution_id,
            'symbol': symbol,
            'long_exchange': long_exchange,
            'short_exchange': short_exchange,
            'opened_at': datetime.now().isoformat()
        }
        self._state['active_pairs'].append(pair)
        self.save_state()
        logger.info(f"StateManager - Added active pair: {symbol} ({long_exchange}/{short_exchange})")
    
    def remove_active_pair(self, execution_id: str):
        """Remove a closed trading pair."""
        self._state['active_pairs'] = [
            p for p in self._state['active_pairs'] 
            if p['execution_id'] != execution_id
        ]
        self.save_state()
    
    def detect_orphaned_legs(self) -> list:
        """
        Detect orphaned legs by comparing persisted state with actual database state.
        Returns list of orphaned legs that need to be unwound.
        """
        orphaned = []
        try:
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                
                # Find execution IDs with only 1 open leg (should always be pairs of 2)
                cursor.execute("""
                    SELECT strategy_execution_id, COUNT(*) as leg_count,
                           GROUP_CONCAT(exchange) as exchanges,
                           GROUP_CONCAT(symbol) as symbols,
                           GROUP_CONCAT(side) as sides
                    FROM trade_log 
                    WHERE open_close = 'Open'
                    GROUP BY strategy_execution_id
                    HAVING leg_count = 1
                """)
                
                rows = cursor.fetchall()
                for row in rows:
                    orphaned.append({
                        'execution_id': row[0],
                        'leg_count': row[1],
                        'exchanges': row[2].split(','),
                        'symbols': row[3].split(','),
                        'sides': row[4].split(',')
                    })
                
                if orphaned:
                    logger.warning(f"StateManager - Found {len(orphaned)} orphaned leg(s): {orphaned}")
                else:
                    logger.info("StateManager - No orphaned legs detected")
                    
        except Exception as e:
            logger.error(f"StateManager - Error detecting orphaned legs: {e}")
        
        return orphaned
    
    def reconcile_positions(self, position_controller) -> dict:
        """
        Reconcile open positions from all exchanges with the trade database on startup.
        Returns a summary of the reconciliation.
        """
        summary = {
            'db_open_positions': 0,
            'exchange_positions': 0,
            'orphaned_legs': [],
            'reconciled': True
        }
        
        try:
            # Check for orphaned legs in DB
            orphaned = self.detect_orphaned_legs()
            summary['orphaned_legs'] = orphaned
            
            # Count DB open positions
            with sqlite3.connect(self.db_path) as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT COUNT(*) FROM trade_log WHERE open_close = 'Open'")
                summary['db_open_positions'] = cursor.fetchone()[0]
            
            if orphaned:
                summary['reconciled'] = False
                logger.warning(
                    f"StateManager - Reconciliation found {len(orphaned)} orphaned legs. "
                    f"Manual intervention may be required."
                )
                
                # Attempt to safely close orphaned legs
                for leg in orphaned:
                    for i, exchange in enumerate(leg['exchanges']):
                        symbol = leg['symbols'][i]
                        try:
                            logger.info(f"StateManager - Attempting to close orphaned leg: {symbol} on {exchange}")
                            close_method = getattr(position_controller, exchange.lower(), None)
                            if close_method and hasattr(close_method, 'close_position'):
                                close_method.close_position(symbol=symbol, reason='ORPHAN_RECOVERY')
                                logger.info(f"StateManager - Successfully closed orphaned leg: {symbol} on {exchange}")
                        except Exception as e:
                            logger.error(f"StateManager - Failed to close orphaned leg {symbol} on {exchange}: {e}")
            else:
                logger.info("StateManager - Position reconciliation complete, no issues found.")
            
        except Exception as e:
            logger.error(f"StateManager - Error during reconciliation: {e}")
            summary['reconciled'] = False
        
        return summary
