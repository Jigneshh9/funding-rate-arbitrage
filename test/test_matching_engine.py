"""
Unit tests for the MatchingEngine module.
Tests funding rate arbitrage detection logic, edge cases, and symbol normalization.

NOTE: Tests use the FLAT LIST format that production code expects:
  [{'symbol': 'BTCUSDT', 'exchange': 'Binance', 'funding_rate': 0.001, 'skew_usd': 100}, ...]
NOT the nested dict format {exchange: {symbol: {...}}} that earlier tests incorrectly used.
"""
import unittest
import sys
import os
from unittest.mock import patch

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestMatchingEngine(unittest.TestCase):
    """Test the core matching engine logic."""

    def _make_rate(self, symbol, exchange, funding_rate, skew_usd=0):
        """Helper to build a single rate entry in the production shape."""
        return {
            'symbol': symbol,
            'exchange': exchange,
            'funding_rate': funding_rate,
            'skew_usd': skew_usd,
        }

    @patch('MatchingEngine.MatchingEngine.get_base_block_number', return_value=12345678)
    def test_find_arbitrage_opposite_rates(self, _mock_block):
        """Test detection when one rate is positive and the other negative."""
        from MatchingEngine.MatchingEngine import matchingEngine
        engine = matchingEngine()

        rates = [
            self._make_rate('BTCUSDT', 'Binance', 0.001, 1000000),
            self._make_rate('BTCUSDT', 'ByBit', -0.002, -500000),
        ]

        opportunities = engine.find_delta_neutral_arbitrage_opportunities(rates)
        self.assertIsNotNone(opportunities)
        self.assertGreater(len(opportunities), 0)
        opp = opportunities[0]
        self.assertEqual(opp['symbol'], 'BTC')
        # The exchange with negative rate should be long
        self.assertEqual(opp['long_exchange'], 'ByBit')
        self.assertEqual(opp['short_exchange'], 'Binance')

    @patch('MatchingEngine.MatchingEngine.get_base_block_number', return_value=12345678)
    def test_find_arbitrage_same_sign_rates(self, _mock_block):
        """Test detection when both rates are positive but different."""
        from MatchingEngine.MatchingEngine import matchingEngine
        engine = matchingEngine()

        rates = [
            self._make_rate('ETHUSDT', 'Binance', 0.005, 2000000),
            self._make_rate('ETHUSDT', 'ByBit', 0.001, 1000000),
        ]

        opportunities = engine.find_delta_neutral_arbitrage_opportunities(rates)
        self.assertIsNotNone(opportunities)
        self.assertGreater(len(opportunities), 0)
        opp = opportunities[0]
        # Lower rate exchange should be long
        self.assertEqual(opp['long_exchange'], 'ByBit')
        self.assertEqual(opp['short_exchange'], 'Binance')

    @patch('MatchingEngine.MatchingEngine.get_base_block_number', return_value=12345678)
    def test_zero_rates_no_opportunity(self, _mock_block):
        """Test that zero/zero rates don't create an opportunity."""
        from MatchingEngine.MatchingEngine import matchingEngine
        engine = matchingEngine()

        rates = [
            self._make_rate('BTCUSDT', 'Binance', 0, 0),
            self._make_rate('BTCUSDT', 'ByBit', 0, 0),
        ]

        opportunities = engine.find_delta_neutral_arbitrage_opportunities(rates)
        self.assertEqual(len(opportunities) if opportunities else 0, 0)

    @patch('MatchingEngine.MatchingEngine.get_base_block_number', return_value=12345678)
    def test_single_zero_rate(self, _mock_block):
        """Test that one zero and one non-zero rate still works."""
        from MatchingEngine.MatchingEngine import matchingEngine
        engine = matchingEngine()

        rates = [
            self._make_rate('BTCUSDT', 'Binance', 0.003, 500000),
            self._make_rate('BTCUSDT', 'ByBit', 0, 0),
        ]

        opportunities = engine.find_delta_neutral_arbitrage_opportunities(rates)
        self.assertIsNotNone(opportunities)

    @patch('MatchingEngine.MatchingEngine.get_base_block_number', return_value=12345678)
    def test_no_common_symbols(self, _mock_block):
        """Test when exchanges have no overlapping symbols."""
        from MatchingEngine.MatchingEngine import matchingEngine
        engine = matchingEngine()

        rates = [
            self._make_rate('BTCUSDT', 'Binance', 0.001, 100),
            self._make_rate('ETHUSDT', 'ByBit', -0.001, -100),
        ]

        opportunities = engine.find_delta_neutral_arbitrage_opportunities(rates)
        self.assertEqual(len(opportunities) if opportunities else 0, 0)

    @patch('MatchingEngine.MatchingEngine.get_base_block_number', return_value=12345678)
    def test_single_exchange(self, _mock_block):
        """Test graceful handling of a single exchange."""
        from MatchingEngine.MatchingEngine import matchingEngine
        engine = matchingEngine()

        rates = [
            self._make_rate('BTCUSDT', 'Binance', 0.001, 100),
        ]

        opportunities = engine.find_delta_neutral_arbitrage_opportunities(rates)
        self.assertEqual(len(opportunities) if opportunities else 0, 0)

    def test_empty_rates(self):
        """Test graceful handling of empty rates."""
        from MatchingEngine.MatchingEngine import matchingEngine
        engine = matchingEngine()

        opportunities = engine.find_delta_neutral_arbitrage_opportunities([])
        self.assertEqual(len(opportunities) if opportunities else 0, 0)

    @patch('MatchingEngine.MatchingEngine.get_base_block_number', return_value=12345678)
    def test_three_exchanges(self, _mock_block):
        """Test with three exchanges — should find pairwise opportunities."""
        from MatchingEngine.MatchingEngine import matchingEngine
        engine = matchingEngine()

        rates = [
            self._make_rate('BTCUSDT', 'Binance', 0.005, 100000),
            self._make_rate('BTCUSDT', 'ByBit', -0.001, -50000),
            self._make_rate('BTCUSDT', 'GMX', 0.002, 30000),
        ]

        opportunities = engine.find_delta_neutral_arbitrage_opportunities(rates)
        self.assertIsNotNone(opportunities)
        # With 3 exchanges there should be up to 3 pairwise opportunities
        self.assertGreaterEqual(len(opportunities), 1)


class TestSymbolNormalization(unittest.TestCase):
    """Test symbol normalization across different exchange formats."""

    def test_normalize_binance_symbol(self):
        """Test Binance symbol format (BTCUSDT -> BTC)."""
        from MatchingEngine.MatchingEngineUtils import normalize_symbol
        self.assertEqual(normalize_symbol('BTCUSDT'), 'BTC')
        self.assertEqual(normalize_symbol('ETHUSDT'), 'ETH')

    def test_normalize_perp_symbol(self):
        """Test PERP symbol format (BTCPERP -> BTC)."""
        from MatchingEngine.MatchingEngineUtils import normalize_symbol
        self.assertEqual(normalize_symbol('BTCPERP'), 'BTC')
        self.assertEqual(normalize_symbol('ETHPERP'), 'ETH')

    def test_normalize_usd_symbol(self):
        """Test USD symbol format (BTCUSD -> BTC)."""
        from MatchingEngine.MatchingEngineUtils import normalize_symbol
        self.assertEqual(normalize_symbol('BTCUSD'), 'BTC')

    def test_normalize_plain_symbol(self):
        """Test plain symbol passes through unchanged."""
        from MatchingEngine.MatchingEngineUtils import normalize_symbol
        self.assertEqual(normalize_symbol('BTC'), 'BTC')
        self.assertEqual(normalize_symbol('ETH'), 'ETH')


class TestGroupBySymbol(unittest.TestCase):
    """Test group_by_symbol with real production data shape."""

    def test_groups_by_normalized_symbol(self):
        """group_by_symbol should group a flat list by normalized symbol."""
        from MatchingEngine.MatchingEngineUtils import group_by_symbol
        rates = [
            {'symbol': 'BTCUSDT', 'exchange': 'Binance', 'funding_rate': 0.001, 'skew_usd': 100},
            {'symbol': 'BTCPERP', 'exchange': 'GMX', 'funding_rate': -0.002, 'skew_usd': -50},
            {'symbol': 'ETHUSDT', 'exchange': 'Binance', 'funding_rate': 0.003, 'skew_usd': 200},
        ]
        result = group_by_symbol(rates)
        self.assertIn('BTC', result)
        self.assertIn('ETH', result)
        self.assertEqual(len(result['BTC']), 2)
        self.assertEqual(len(result['ETH']), 1)

    def test_empty_list(self):
        """group_by_symbol should return empty dict for empty input."""
        from MatchingEngine.MatchingEngineUtils import group_by_symbol
        result = group_by_symbol([])
        self.assertEqual(result, {})


if __name__ == '__main__':
    unittest.main()
