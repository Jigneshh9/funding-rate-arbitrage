from Backtesting.Binance.binanceBacktester import BinanceBacktester
from Backtesting.Synthetix.SynthetixBacktester import SynthetixBacktester
from Backtesting.utils.backtestingUtils import *
from Backtesting.Binance.binanceBacktesterUtils import *
from Backtesting.Synthetix.SynthetixBacktesterUtils import *
from Backtesting.MasterBacktester.MasterBacktesterUtils import *
from APICaller.master.MasterUtils import TARGET_TOKENS
from GlobalUtils.logger import logger
from GlobalUtils.MarketDirectories.SynthetixMarketDirectory import SynthetixMarketDirectory
import time
import pandas as pd
import matplotlib.pyplot as plt

class MasterBacktester:
    def __init__(self, slippage_bps=5.0, fee_bps=10.0, order_delay_blocks=2):
        """
        Args:
            slippage_bps: Estimated slippage in basis points per trade
            fee_bps: Trading fees in basis points per leg
            order_delay_blocks: Number of blocks delay between signal and fill
        """
        self.binance = BinanceBacktester()
        self.synthetix = SynthetixBacktester()
        self.slippage_bps = slippage_bps
        self.fee_bps = fee_bps
        self.order_delay_blocks = order_delay_blocks

    def run_updates(self):
        try:
            self.synthetix.fetch_and_process_events_for_all_tokens()

            for token_info in TARGET_TOKENS:
                if token_info["is_target"]:
                    self.binance.get_historical_data(token_info["token"])
                    time.sleep(3)
        except Exception as e:
            logger.error(f'MasterBacktester - Error encountered while updating data: {e}')
    
    def _calculate_slippage_cost(self, trade_size_usd: float) -> float:
        """Calculate estimated slippage cost for a trade."""
        return trade_size_usd * (self.slippage_bps / 10000)
    
    def _calculate_fee_cost(self, trade_size_usd: float) -> float:
        """Calculate fee cost for both legs of a trade."""
        return trade_size_usd * (self.fee_bps / 10000) * 2  # Both legs
    
    def _apply_order_delay(self, entry_block: int) -> int:
        """Simulate order delay by shifting entry block."""
        return entry_block + self.order_delay_blocks

    def backtest_arbitrage_strategy(self, symbol: str, entry_threshold=0.0001, exit_threshold=0.00005):
        try:
            synthetix_funding_events = self.synthetix.load_data_from_json(symbol)
            binance_funding_events = self.binance.load_data_from_json(symbol)

            synthetix_df = pd.DataFrame(synthetix_funding_events)
            binance_df = pd.DataFrame(binance_funding_events).sort_values('block_number')

            start_block: int = 16352864
            snx_df_filtered = synthetix_df.loc[synthetix_df['block_number'] > start_block]
            binance_df_filtered = binance_df.loc[binance_df['block_number'] > start_block]

            indicative_profit = 0.0  # Raw funding PnL
            realistic_profit = 0.0   # After fees, slippage, delays
            trades = []

            potential_trades = determine_trade_entry_exit_points(snx_df_filtered, binance_df_filtered, entry_threshold, exit_threshold)

            for trade in potential_trades:
                trade_size_in_asset = trade['size_in_asset']
                
                # Simulate order delay
                delayed_entry_binance = self._apply_order_delay(trade['entry_block_binance'])
                delayed_entry_snx = self._apply_order_delay(trade['entry_block_snx'])
                
                binance_trade_events = extract_funding_events(binance_df, delayed_entry_binance, trade['exit_block_binance'])
                binance_funding_impact = calculate_total_funding_impact(binance_trade_events, trade_size_in_asset)

                new_funding_velocity = SynthetixMarketDirectory.calculate_new_funding_velocity(symbol, trade_size_in_asset, trade_size_in_asset)
                synthetix_trade_data = synthetix_df[(synthetix_df['block_number'] >= delayed_entry_snx) & (synthetix_df['block_number'] <= trade['exit_block_snx'])]
                synthetix_funding_impact = accumulate_funding_costs(synthetix_trade_data, delayed_entry_snx, trade['exit_block_snx'], trade_size_in_asset)

                trade_details = calculate_profit_or_loss_for_trade(trade, synthetix_funding_impact, binance_funding_impact)
                
                # Calculate indicative PnL (raw funding)
                raw_pnl = trade_details['profit']['total']
                indicative_profit += raw_pnl
                
                # Calculate realistic PnL (after costs)
                trade_size_usd = abs(trade_size_in_asset * trade.get('entry_price', 1.0))
                slippage_cost = self._calculate_slippage_cost(trade_size_usd)
                fee_cost = self._calculate_fee_cost(trade_size_usd)
                realistic_pnl = raw_pnl - slippage_cost - fee_cost
                realistic_profit += realistic_pnl
                
                trade_details['indicative_pnl'] = raw_pnl
                trade_details['realistic_pnl'] = realistic_pnl
                trade_details['slippage_cost'] = slippage_cost
                trade_details['fee_cost'] = fee_cost
                trades.append(trade_details)

            result = {
                'symbol': symbol,
                'indicative_total_profit': indicative_profit,
                'realistic_total_profit': realistic_profit,
                'total_trades': len(trades),
                'trades': trades,
                'config': {
                    'slippage_bps': self.slippage_bps,
                    'fee_bps': self.fee_bps,
                    'order_delay_blocks': self.order_delay_blocks,
                    'entry_threshold': entry_threshold,
                    'exit_threshold': exit_threshold
                }
            }

            logger.info(
                f"MasterBacktester - {symbol}: {len(trades)} trades, "
                f"Indicative PnL: ${indicative_profit:.2f}, "
                f"Realistic PnL: ${realistic_profit:.2f}"
            )

            return result
        
        except Exception as e:
            logger.error(f'MasterBacktester - Error while backtesting arbitrage strategy for symbol {symbol}: {e}')
            return None
    
    def backtest_portfolio(self, symbols: list = None, entry_threshold=0.0001, exit_threshold=0.00005) -> dict:
        """
        Run backtests across multiple symbols simultaneously to get 
        portfolio-level metrics.
        
        Args:
            symbols: List of symbols to backtest. Defaults to all target tokens.
            entry_threshold: Funding rate spread threshold to enter
            exit_threshold: Funding rate spread threshold to exit
        
        Returns:
            dict with portfolio-level metrics and per-symbol results
        """
        if symbols is None:
            symbols = [t['token'] for t in TARGET_TOKENS if t['is_target']]
        
        all_results = {}
        portfolio_indicative = 0.0
        portfolio_realistic = 0.0
        total_trades = 0
        
        for symbol in symbols:
            try:
                result = self.backtest_arbitrage_strategy(symbol, entry_threshold, exit_threshold)
                if result:
                    all_results[symbol] = result
                    portfolio_indicative += result['indicative_total_profit']
                    portfolio_realistic += result['realistic_total_profit']
                    total_trades += result['total_trades']
            except Exception as e:
                logger.error(f"MasterBacktester - Portfolio backtest failed for {symbol}: {e}")
                all_results[symbol] = {'error': str(e)}
        
        summary = {
            'portfolio_indicative_pnl': portfolio_indicative,
            'portfolio_realistic_pnl': portfolio_realistic,
            'total_trades_across_symbols': total_trades,
            'symbols_tested': len(all_results),
            'per_symbol_results': all_results
        }
        
        logger.info(
            f"MasterBacktester - Portfolio backtest: {len(all_results)} symbols, "
            f"{total_trades} total trades, "
            f"Indicative: ${portfolio_indicative:.2f}, "
            f"Realistic: ${portfolio_realistic:.2f}"
        )
        
        return summary