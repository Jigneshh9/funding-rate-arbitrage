"""
Unit tests for profitability checks and execution safety.
"""
import unittest
import os
import sys
import time

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestExecutionSafety(unittest.TestCase):
    """Tests for the execution safety module."""
    
    def test_generate_order_id_uniqueness(self):
        """Each call should generate a unique order ID."""
        from TxExecution.Master.execution_safety import generate_order_id
        ids = {generate_order_id() for _ in range(100)}
        self.assertEqual(len(ids), 100)
    
    def test_order_tracker_register_and_check(self):
        """Test order tracking and duplicate detection."""
        from TxExecution.Master.execution_safety import OrderTracker
        tracker = OrderTracker()
        
        # No duplicates initially
        self.assertFalse(tracker.is_duplicate('Binance', 'BTC', 'long'))
        
        # Register an order
        tracker.register_order('order-1', 'Binance', 'BTC', 'long')
        
        # Now it should be detected as duplicate
        self.assertTrue(tracker.is_duplicate('Binance', 'BTC', 'long'))
        
        # Different symbol should not be duplicate
        self.assertFalse(tracker.is_duplicate('Binance', 'ETH', 'long'))
        
        # Different exchange should not be duplicate
        self.assertFalse(tracker.is_duplicate('ByBit', 'BTC', 'long'))
    
    def test_order_tracker_mark_failed_removes_duplicate(self):
        """Failed orders should not block new orders."""
        from TxExecution.Master.execution_safety import OrderTracker
        tracker = OrderTracker()
        
        tracker.register_order('order-1', 'Binance', 'BTC', 'long')
        self.assertTrue(tracker.is_duplicate('Binance', 'BTC', 'long'))
        
        tracker.mark_failed('order-1')
        self.assertFalse(tracker.is_duplicate('Binance', 'BTC', 'long'))
    
    def test_spread_validation_acceptable(self):
        """Test that a stable spread passes validation."""
        from TxExecution.Master.execution_safety import validate_pre_trade_spread
        
        opportunity = {
            'short_exchange_funding_rate_8hr': '0.005',
            'long_exchange_funding_rate_8hr': '-0.002',
            'long_exchange': 'ByBit',
            'short_exchange': 'Binance'
        }
        current_rates = {
            'ByBit': '-0.002',
            'Binance': '0.005'
        }
        
        self.assertTrue(validate_pre_trade_spread(opportunity, current_rates))
    
    def test_spread_validation_deteriorated(self):
        """Test that a deteriorated spread fails validation."""
        from TxExecution.Master.execution_safety import validate_pre_trade_spread
        
        opportunity = {
            'short_exchange_funding_rate_8hr': '0.005',
            'long_exchange_funding_rate_8hr': '-0.002',
            'long_exchange': 'ByBit',
            'short_exchange': 'Binance'
        }
        # Spread has collapsed
        current_rates = {
            'ByBit': '0.001',
            'Binance': '0.001'
        }
        
        self.assertFalse(validate_pre_trade_spread(opportunity, current_rates, max_deterioration=0.001))
    
    def test_execute_with_retry_success(self):
        """Test retry logic with a successful function."""
        from TxExecution.Master.execution_safety import execute_with_retry
        
        call_count = 0
        def success_func(**kwargs):
            nonlocal call_count
            call_count += 1
            return {'status': 'ok'}
        
        result = execute_with_retry(success_func, max_retries=2, base_delay=0.01)
        self.assertIsNotNone(result)
        self.assertEqual(call_count, 1)  # Should succeed on first try
    
    def test_execute_with_retry_eventual_success(self):
        """Test retry logic that succeeds on second attempt."""
        from TxExecution.Master.execution_safety import execute_with_retry
        
        call_count = 0
        def flaky_func(**kwargs):
            nonlocal call_count
            call_count += 1
            if call_count < 2:
                raise Exception("Temporary failure")
            return {'status': 'ok'}
        
        result = execute_with_retry(flaky_func, max_retries=2, base_delay=0.01)
        self.assertIsNotNone(result)
        self.assertEqual(call_count, 2)
    
    def test_execute_with_retry_total_failure(self):
        """Test retry logic when all attempts fail."""
        from TxExecution.Master.execution_safety import execute_with_retry
        
        def always_fail(**kwargs):
            raise Exception("Permanent failure")
        
        result = execute_with_retry(always_fail, max_retries=1, base_delay=0.01)
        self.assertIsNone(result)


class TestCollateralUtils(unittest.TestCase):
    """Test collateral-related utility functions."""
    
    def test_collateral_ratio_acceptable_balanced(self):
        """Test that balanced collateral passes the ratio check."""
        from TxExecution.Master.MasterPositionControllerUtils import is_collateral_ratio_acceptable
        
        result = is_collateral_ratio_acceptable({
            'long_exchange': 1000,
            'short_exchange': 900
        })
        self.assertTrue(result)
    
    def test_collateral_ratio_unacceptable(self):
        """Test that heavily imbalanced collateral fails the ratio check."""
        from TxExecution.Master.MasterPositionControllerUtils import is_collateral_ratio_acceptable
        
        result = is_collateral_ratio_acceptable({
            'long_exchange': 1000,
            'short_exchange': 5  # Tiny amount
        })
        self.assertFalse(result)
    
    def test_collateral_ratio_uses_both_sides(self):
        """Regression test: previously both sides read from 'long_exchange'."""
        from TxExecution.Master.MasterPositionControllerUtils import is_collateral_ratio_acceptable
        
        # With the old bug, both sides would be 1000, so ratio would be 1.0 (acceptable)
        # With the fix, the ratio should reflect the actual imbalance
        result = is_collateral_ratio_acceptable({
            'long_exchange': 1000,
            'short_exchange': 0.001  # Extremely imbalanced
        })
        self.assertFalse(result)


if __name__ == '__main__':
    unittest.main()
