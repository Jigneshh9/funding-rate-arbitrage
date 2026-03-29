from GlobalUtils.logger import *
from pubsub import pub
from APICaller.master.MasterCaller import MasterCaller
from MatchingEngine.MatchingEngine import matchingEngine
from MatchingEngine.profitabilityChecks.checkProfitability import ProfitabilityChecker
from TxExecution.Master.MasterPositionController import MasterPositionController
from PositionMonitor.Master.MasterPositionMonitor import MasterPositionMonitor
from PositionMonitor.Master.MasterPositionMonitorUtils import *
from PositionMonitor.TradeDatabase.TradeDatabase import TradeLogger
from GlobalUtils.globalUtils import *
from GlobalUtils.MarketDirectories.SynthetixMarketDirectory import SynthetixMarketDirectory
from GlobalUtils.MarketDirectories.GMXMarketDirectory import GMXMarketDirectory
from GlobalUtils.config_validator import validate_config, ConfigValidationError
from GlobalUtils.state_manager import StateManager
import time
import signal
import sys

class Main:
    def __init__(self):
        # Validate configuration at startup — fail fast with clear messages
        try:
            validate_config()
        except ConfigValidationError as e:
            logger.error(f"MainClass - Startup aborted due to config error: {e}")
            print(f"\n❌ Configuration Error:\n{e}\n")
            sys.exit(1)
        
        setup_topics()
        self.caller = MasterCaller()
        self.matching_engine = matchingEngine()
        self.profitability_checker = ProfitabilityChecker()
        self.position_controller = MasterPositionController()
        self.position_controller.subscribe_to_events()
        self.position_monitor = MasterPositionMonitor()
        self.trade_logger = TradeLogger()
        self.state_manager = StateManager()
        self._running = True
        
        # Register signal handlers for graceful shutdown
        signal.signal(signal.SIGINT, self._shutdown_handler)
        signal.signal(signal.SIGTERM, self._shutdown_handler)
        
        # Reconcile positions on startup
        self._reconcile_on_startup()
    
    def _reconcile_on_startup(self):
        """Reconcile open positions from all exchanges before scanning for new trades."""
        try:
            self.state_manager.record_startup()
            summary = self.state_manager.reconcile_positions(self.position_controller)
            
            if summary['orphaned_legs']:
                logger.warning(
                    f"MainClass - Startup reconciliation found {len(summary['orphaned_legs'])} orphaned legs. "
                    f"Check logs for details."
                )
            
            logger.info(f"MainClass - Startup reconciliation complete. DB open positions: {summary['db_open_positions']}")
        except Exception as e:
            logger.error(f"MainClass - Error during startup reconciliation: {e}")
    
    def _shutdown_handler(self, signum, frame):
        """Handle graceful shutdown on SIGINT/SIGTERM."""
        logger.info(f"MainClass - Received shutdown signal ({signum}). Initiating graceful shutdown...")
        self._running = False
        
        try:
            # Save current state
            self.state_manager.save_state()
            logger.info("MainClass - State saved. Bot shutting down gracefully.")
            logger.info("MainClass - Note: Open positions must be closed manually or will be reconciled on next startup.")
        except Exception as e:
            logger.error(f"MainClass - Error during shutdown: {e}")
    
    def search_for_opportunities(self):
        try:
            funding_rates = self.caller.get_funding_rates()
            opportunities = self.matching_engine.find_delta_neutral_arbitrage_opportunities(funding_rates)
            opportunity = self.profitability_checker.find_most_profitable_opportunity(opportunities, is_demo=False)
            if opportunity is not None:
                pub.sendMessage(EventsDirectory.OPPORTUNITY_FOUND.value, opportunity=opportunity)
            else:
                logger.info("MainClass - No profitable opportunities found in this scan.")

        except Exception as e:
            logger.error(f"MainClass - An error occurred during search_for_opportunities: {e}", exc_info=True)
            
    def start_search(self):
        try:
            logger.info("MainClass - Starting continuous scan loop...")
            while self._running:
                if not self.position_controller.is_already_position_open():
                    self.search_for_opportunities()
                
                # Periodically save state
                self.state_manager.record_scan()
                if not self._running:
                    break
                time.sleep(30) 
        
        except Exception as e:
            logger.error(f"MainClass - An error occurred during start_search: {e}", exc_info=True)
        finally:
            self.state_manager.save_state()
            logger.info("MainClass - Scan loop ended. State saved.")

