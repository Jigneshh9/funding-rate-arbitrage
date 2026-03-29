"""
Metrics Collection and Alerting Module
Tracks operational metrics and provides alerting for critical conditions.
"""
import sqlite3
import time
from datetime import datetime
from GlobalUtils.logger import logger


class MetricsCollector:
    """
    Collects and persists operational metrics for the arbitrage bot.
    Metrics are stored in a SQLite database for durability.
    """
    
    def __init__(self, db_path='metrics.db'):
        self.db_path = db_path
        self._create_tables()
        self._counters = {
            'opportunities_found': 0,
            'trades_executed': 0,
            'trades_rejected': 0,
            'trades_failed': 0,
            'risk_rejections': 0,
        }
        self._timers = {}
    
    def _create_tables(self):
        try:
            conn = sqlite3.connect(self.db_path)
            conn.execute('''CREATE TABLE IF NOT EXISTS metrics (
                id INTEGER PRIMARY KEY,
                metric_name TEXT NOT NULL,
                metric_value REAL NOT NULL,
                tags TEXT,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )''')
            conn.execute('''CREATE TABLE IF NOT EXISTS alerts (
                id INTEGER PRIMARY KEY,
                alert_type TEXT NOT NULL,
                severity TEXT NOT NULL,
                message TEXT NOT NULL,
                acknowledged INTEGER DEFAULT 0,
                timestamp DATETIME DEFAULT CURRENT_TIMESTAMP
            )''')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_metric_name ON metrics(metric_name);')
            conn.execute('CREATE INDEX IF NOT EXISTS idx_alert_type ON alerts(alert_type);')
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"MetricsCollector - Error creating tables: {e}")
    
    def record(self, metric_name: str, value: float, tags: str = None):
        """Record a metric value."""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.execute(
                "INSERT INTO metrics (metric_name, metric_value, tags) VALUES (?, ?, ?)",
                (metric_name, value, tags)
            )
            conn.commit()
            conn.close()
        except Exception as e:
            logger.error(f"MetricsCollector - Error recording metric {metric_name}: {e}")
    
    def increment(self, counter_name: str, amount: int = 1):
        """Increment a counter metric."""
        self._counters[counter_name] = self._counters.get(counter_name, 0) + amount
        self.record(f"counter.{counter_name}", self._counters[counter_name])
    
    def start_timer(self, timer_name: str):
        """Start a timer for latency measurement."""
        self._timers[timer_name] = time.time()
    
    def stop_timer(self, timer_name: str) -> float:
        """Stop a timer and record the elapsed time in milliseconds."""
        if timer_name in self._timers:
            elapsed_ms = (time.time() - self._timers[timer_name]) * 1000
            self.record(f"timer.{timer_name}", elapsed_ms)
            del self._timers[timer_name]
            return elapsed_ms
        return 0.0
    
    def record_opportunity(self, symbol: str, spread: float, long_exchange: str, short_exchange: str):
        """Record an arbitrage opportunity detection."""
        self.increment('opportunities_found')
        self.record('opportunity.spread', spread, f"{symbol}:{long_exchange}:{short_exchange}")
    
    def record_trade_execution(self, symbol: str, size_usd: float, fill_latency_ms: float):
        """Record a successful trade execution."""
        self.increment('trades_executed')
        self.record('trade.size_usd', size_usd, symbol)
        self.record('trade.fill_latency_ms', fill_latency_ms, symbol)
    
    def record_pnl(self, symbol: str, realized_pnl: float, unrealized_pnl: float = 0):
        """Record PnL metrics."""
        self.record('pnl.realized', realized_pnl, symbol)
        self.record('pnl.unrealized', unrealized_pnl, symbol)
    
    def record_funding_earned(self, symbol: str, amount: float):
        """Record funding earned from an open position."""
        self.record('funding.earned', amount, symbol)
    
    def get_summary(self, hours: int = 24) -> dict:
        """Get a summary of metrics over the last N hours."""
        try:
            conn = sqlite3.connect(self.db_path)
            cutoff = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            summary = {}
            for counter_name, value in self._counters.items():
                summary[counter_name] = value
            
            # Get recent PnL
            cursor = conn.execute(
                "SELECT COALESCE(SUM(metric_value), 0) FROM metrics WHERE metric_name = 'pnl.realized' AND timestamp >= datetime('now', ?)",
                (f'-{hours} hours',)
            )
            summary['realized_pnl_24h'] = cursor.fetchone()[0]
            
            # Get avg fill latency
            cursor = conn.execute(
                "SELECT COALESCE(AVG(metric_value), 0) FROM metrics WHERE metric_name = 'trade.fill_latency_ms' AND timestamp >= datetime('now', ?)",
                (f'-{hours} hours',)
            )
            summary['avg_fill_latency_ms'] = cursor.fetchone()[0]
            
            conn.close()
            return summary
        except Exception as e:
            logger.error(f"MetricsCollector - Error getting summary: {e}")
            return {}


class AlertManager:
    """
    Manages alerts for critical conditions.
    """
    
    def __init__(self, metrics: MetricsCollector):
        self.metrics = metrics
        self.db_path = metrics.db_path
    
    def alert(self, alert_type: str, severity: str, message: str):
        """Create a new alert."""
        try:
            conn = sqlite3.connect(self.db_path)
            conn.execute(
                "INSERT INTO alerts (alert_type, severity, message) VALUES (?, ?, ?)",
                (alert_type, severity, message)
            )
            conn.commit()
            conn.close()
            
            log_func = logger.warning if severity == 'WARNING' else logger.error
            log_func(f"ALERT [{severity}] {alert_type}: {message}")
        except Exception as e:
            logger.error(f"AlertManager - Error creating alert: {e}")
    
    def alert_unhedged_exposure(self, symbol: str, exchange: str, size: float):
        """Alert when a position is unhedged (one leg failed)."""
        self.alert(
            'UNHEDGED_EXPOSURE', 'CRITICAL',
            f"Unhedged position: {symbol} on {exchange}, size: {size}"
        )
    
    def alert_api_failure(self, exchange: str, error: str):
        """Alert on exchange API failure."""
        self.alert(
            'API_FAILURE', 'WARNING',
            f"API failure on {exchange}: {error}"
        )
    
    def alert_collateral_imbalance(self, long_exchange: str, short_exchange: str, ratio: float):
        """Alert when collateral ratio between exchanges is too skewed."""
        self.alert(
            'COLLATERAL_IMBALANCE', 'WARNING',
            f"Collateral imbalance between {long_exchange} and {short_exchange}: ratio={ratio:.4f}"
        )
    
    def alert_daily_loss_approaching(self, current_loss: float, cap: float):
        """Alert when daily loss is approaching the cap."""
        self.alert(
            'DAILY_LOSS_WARNING', 'WARNING',
            f"Daily loss ${current_loss:.2f} approaching cap ${cap:.2f}"
        )
    
    def get_unacknowledged_alerts(self) -> list:
        """Get all unacknowledged alerts."""
        try:
            conn = sqlite3.connect(self.db_path)
            cursor = conn.execute(
                "SELECT id, alert_type, severity, message, timestamp FROM alerts WHERE acknowledged = 0 ORDER BY timestamp DESC"
            )
            alerts = [{'id': r[0], 'type': r[1], 'severity': r[2], 'message': r[3], 'time': r[4]} for r in cursor.fetchall()]
            conn.close()
            return alerts
        except Exception as e:
            logger.error(f"AlertManager - Error getting alerts: {e}")
            return []
