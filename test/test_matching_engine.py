"""
Unit tests for the MatchingEngine module.
Tests funding rate arbitrage detection logic, edge cases, and symbol normalization.
"""
import unittest
import sys
import os

# Add project root to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestMatchingEngine(unittest.TestCase):
    """Test the core matching engine logic."""
    
    def test_find_arbitrage_opposite_rates(self):
        """Test detection when one rate is positive and the other negative."""
        from MatchingEngine.MatchingEngine import matchingEngine
        engine = matchingEngine()
        
        rates = {
            'Binance': {'BTC': {'funding_rate': 0.001, 'skew_usd': 1000000}},
            'ByBit': {'BTC': {'funding_rate': -0.002, 'skew_usd': -500000}}
        }
        
        opportunities = engine.find_delta_neutral_arbitrage_opportunities(rates)
        self.assertIsNotNone(opportunities)
        if opportunities:
            opp = opportunities[0]
            self.assertEqual(opp['symbol'], 'BTC')
            # The exchange with negative rate should be long
            self.assertEqual(opp['long_exchange'], 'ByBit')
            self.assertEqual(opp['short_exchange'], 'Binance')
    
    def test_find_arbitrage_same_sign_rates(self):
        """Test detection when both rates are positive but different."""
        from MatchingEngine.MatchingEngine import matchingEngine
        engine = matchingEngine()
        
        rates = {
            'Binance': {'ETH': {'funding_rate': 0.005, 'skew_usd': 2000000}},
            'ByBit': {'ETH': {'funding_rate': 0.001, 'skew_usd': 1000000}}
        }
        
        opportunities = engine.find_delta_neutral_arbitrage_opportunities(rates)
        self.assertIsNotNone(opportunities)
        if opportunities:
            opp = opportunities[0]
            # Lower rate exchange should be long
            self.assertEqual(opp['long_exchange'], 'ByBit')
            self.assertEqual(opp['short_exchange'], 'Binance')
    
    def test_zero_rates_no_opportunity(self):
        """Test that zero/zero rates don't create an opportunity (was a bug)."""
        from MatchingEngine.MatchingEngine import matchingEngine
        engine = matchingEngine()
        
        rates = {
            'Binance': {'BTC': {'funding_rate': 0, 'skew_usd': 0}},
            'ByBit': {'BTC': {'funding_rate': 0, 'skew_usd': 0}}
        }
        
        # This should not crash (was causing UnboundLocalError before fix)
        opportunities = engine.find_delta_neutral_arbitrage_opportunities(rates)
        # Zero-rate pairs are skipped, so no opportunities
        self.assertEqual(len(opportunities) if opportunities else 0, 0)
    
    def test_single_zero_rate(self):
        """Test that one zero and one non-zero rate still works."""
        from MatchingEngine.MatchingEngine import matchingEngine
        engine = matchingEngine()
        
        rates = {
            'Binance': {'BTC': {'funding_rate': 0.003, 'skew_usd': 500000}},
            'ByBit': {'BTC': {'funding_rate': 0, 'skew_usd': 0}}
        }
        
        # Should not crash and should produce an opportunity
        opportunities = engine.find_delta_neutral_arbitrage_opportunities(rates)
        self.assertIsNotNone(opportunities)
    
    def test_no_common_symbols(self):
        """Test when exchanges have no overlapping symbols."""
        from MatchingEngine.MatchingEngine import matchingEngine
        engine = matchingEngine()
        
        rates = {
            'Binance': {'BTC': {'funding_rate': 0.001, 'skew_usd': 100}},
            'ByBit': {'ETH': {'funding_rate': -0.001, 'skew_usd': -100}}
        }
        
        opportunities = engine.find_delta_neutral_arbitrage_opportunities(rates)
        self.assertEqual(len(opportunities) if opportunities else 0, 0)
    
    def test_single_exchange(self):
        """Test graceful handling of a single exchange."""
        from MatchingEngine.MatchingEngine import matchingEngine
        engine = matchingEngine()
        
        rates = {
            'Binance': {'BTC': {'funding_rate': 0.001, 'skew_usd': 100}}
        }
        
        opportunities = engine.find_delta_neutral_arbitrage_opportunities(rates)
        self.assertEqual(len(opportunities) if opportunities else 0, 0)
    
    def test_empty_rates(self):
        """Test graceful handling of empty rates."""
        from MatchingEngine.MatchingEngine import matchingEngine
        engine = matchingEngine()
        
        opportunities = engine.find_delta_neutral_arbitrage_opportunities({})
        self.assertEqual(len(opportunities) if opportunities else 0, 0)


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


if __name__ == '__main__':
    unittest.main()
