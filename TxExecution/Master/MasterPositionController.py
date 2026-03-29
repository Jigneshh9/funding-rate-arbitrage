from TxExecution.Binance.BinancePositionController import BinancePositionController
from TxExecution.ByBit.ByBitPositionController import ByBitPositionController
from TxExecution.OKX.OKXPositionController import OKXPositionController
from TxExecution.GMX.GMXPositionController import GMXPositionController
from TxExecution.Perennial.PerennialPositionController import PerennialPositionController

from TxExecution.Master.MasterPositionControllerUtils import *
from TxExecution.Master.execution_safety import generate_order_id, OrderTracker, execute_with_retry

from PositionMonitor.Master.MasterPositionMonitorUtils import *
from APICaller.master.MasterUtils import get_target_exchanges
from GlobalUtils.risk_manager import RiskManager
from pubsub import pub
from GlobalUtils.logger import *
from GlobalUtils.globalUtils import *

class MasterPositionController:
    def __init__(self):
        self.binance = BinancePositionController()
        self.bybit = ByBitPositionController()
        self.gmx = GMXPositionController()
        self.perennial = PerennialPositionController()
        self.risk_manager = RiskManager()
        self.order_tracker = OrderTracker()

    #######################
    ### WRITE FUNCTIONS ###
    #######################

    def execute_trades(self, opportunity: dict):
        symbol: str = opportunity['symbol']

        try:
            if self.is_already_position_open():
                logger.info("MasterPositionController - Position already open, skipping opportunity.")
                return

            # Pre-trade risk checks
            trade_size = self.get_trade_size(opportunity)
            if trade_size is None:
                logger.error("MasterPositionController - Could not determine trade size, aborting.")
                return

            risk_passed, risk_reason = self.risk_manager.check_all_pre_trade(opportunity, trade_size)
            if not risk_passed:
                logger.warning(f"MasterPositionController - Risk check failed: {risk_reason}. Skipping opportunity.")
                return

            exchanges = {
                'long_exchange': opportunity['long_exchange'],
                'short_exchange': opportunity['short_exchange']
            }

            is_hedge = get_is_hedge(opportunity)

            position_data_dict = {}

            for role, exchange_name in exchanges.items():
                is_long=(role == 'long_exchange')
                side = 'long' if is_long else 'short'

                # Check for duplicate orders
                if self.order_tracker.is_duplicate(exchange_name, symbol, side):
                    logger.warning(f"MasterPositionController - Duplicate order detected for {symbol} on {exchange_name} ({side}). Skipping.")
                    continue

                # Generate a unique order ID per leg to avoid overwriting the first registration
                order_id = generate_order_id()
                self.order_tracker.register_order(order_id, exchange_name, symbol, side)

                execute_trade_method = getattr(self, exchange_name.lower()).execute_trade
                position_data = execute_with_retry(
                    execute_trade_method,
                    max_retries=2,
                    opportunity=opportunity, 
                    is_long=is_long, 
                    trade_size=trade_size
                )

                is_hedge_flag = True if is_long and is_hedge['long'] == True else False

                logger.info(f"MasterPositionController - {exchange_name} trade execution response: {position_data}")

                if position_data:
                    self.order_tracker.mark_filled(order_id)
                    position_data_dict[role] = position_data
                    position_data_dict[role]['exchange'] = exchanges['long_exchange'] if role == 'long_exchange' else exchanges['short_exchange']
                    if is_hedge_flag == True:
                        position_data_dict[role]['is_hedge'] = 'True'
                    elif is_hedge_flag == False:
                        position_data_dict[role]['is_hedge'] = 'False'
                else:
                    self.order_tracker.mark_failed(order_id)

            if len(position_data_dict) == 2:
                pub.sendMessage(EventsDirectory.POSITION_OPENED.value, position_data=position_data_dict)
                logger.info("MasterPositionController:execute_trades - Trades executed successfully for opportunity.")
            else:
                # Close any legs that did succeed
                succeeded_exchanges = [exchanges[role] for role in position_data_dict.keys()]
                failed_roles = set(exchanges.keys()) - set(position_data_dict.keys())
                failed_exchanges = [exchanges[role] for role in failed_roles]
                if succeeded_exchanges:
                    self.close_position_pair(symbol=symbol, reason=PositionCloseReason.POSITION_OPEN_ERROR.value, exchanges=succeeded_exchanges)
                logger.error(f"MasterPositionController:execute_trades - Failed to execute trades on: {failed_exchanges}. Rolling back succeeded legs: {succeeded_exchanges}.")

            self.order_tracker.clear_completed()

        except Exception as e:
            logger.error(f"MasterPositionController:execute_trades - Failed to process trades for {symbol}. Error: {e}")
            self.close_position_pair(symbol=symbol, reason=PositionCloseReason.POSITION_OPEN_ERROR.value, exchanges=list(exchanges.values()))

    def close_position_pair(self, symbol: str, reason: str, exchanges: list):
        for exchange_name in exchanges:
            try:
                close_position_method = getattr(self, exchange_name.lower()).close_position
                close_position_method(symbol=symbol, reason=reason)

            except Exception as e:
                logger.error(f"MasterPositionController - Failed to close position for {symbol} on {exchange_name}. Error: {e}")
                return None

        return True


    def subscribe_to_events(self):
        pub.subscribe(self.execute_trades, EventsDirectory.OPPORTUNITY_FOUND.value)
        pub.subscribe(self.close_position_pair, EventsDirectory.CLOSE_POSITION_PAIR.value)

    ######################
    ### READ FUNCTIONS ###
    ######################

    def get_trade_size(self, opportunity: dict) -> float:
        try:
            long_exchange = opportunity['long_exchange']
            short_exchange = opportunity['short_exchange']
            exchanges = {
                'long_exchange': long_exchange,
                'short_exchange': short_exchange
            }

            collateral_amounts = self.get_available_collateral_for_exchanges(exchanges)
            trade_size = adjust_collateral_allocation(
                collateral_amounts, 
                long_exchange, 
                short_exchange
            )

            return trade_size

        except Exception as e:
            logger.error(f"MasterPositionController:get_trade_size - Failed to calculate trade size for exchanges {exchanges}. Error: {e}", exc_info=True)
            return None


    def get_available_collateral_for_exchange(self, exchange: str) -> float:
        try:
            exchange_object = getattr(self, exchange.lower(), None)
            if not callable(getattr(exchange_object, 'get_available_collateral', None)):
                logger.error(f"MasterPositionController:get_available_collateral_for_exchange - '{exchange}' does not support 'get_available_collateral' or is not callable.")
                return None

            collateral = float(exchange_object.get_available_collateral())
            logger.info(f'MasterPositionController:get_available_collateral_for_exchange - collateral = {collateral} for exchange {exchange}')
            return collateral
            
        except Exception as e:
            logger.error(f'MasterPositionController:get_available_collateral_for_exchange - Error while getting available collateral for exchange {exchange}: {str(e)}')
            return None


    def get_available_collateral_for_exchanges(self, exchanges: dict) -> dict:
        try:
            if 'long_exchange' not in exchanges or 'short_exchange' not in exchanges:
                logger.error("MasterPositionController - Missing 'long_exchange' or 'short_exchange' keys in exchanges input.")
                return None

            collateral = {}
            long_exchange = str(exchanges['long_exchange'])
            short_exchange = str(exchanges['short_exchange'])
            long_collateral = self.get_available_collateral_for_exchange(long_exchange)
            short_collateral = self.get_available_collateral_for_exchange(short_exchange)

            collateral['long_exchange'] = long_collateral
            collateral['short_exchange'] = short_collateral

            return collateral
        except KeyError as ke:
            logger.error(f"MasterPositionController - KeyError in getting collateral: {ke}")
            return None
        except Exception as e:
            logger.error(f"MasterPositionController - Failed to get available collateral by exchange. Error: {e}")
            return None


    def is_already_position_open(self) -> bool:
        try:
            is_synthetix_position = False
            is_hmx_position = False
            is_binance_position = False
            is_bybit_position = False
            is_perennial_position = False

            target_exchange_list = get_target_exchanges()
            is_synthetix_target = 'Synthetix' in target_exchange_list
            is_hmx_target = 'HMX' in target_exchange_list
            is_binance_target = 'Binance' in target_exchange_list
            is_bybit_target = 'ByBit' in target_exchange_list
            is_okx_target = 'OKX' in target_exchange_list
            is_gmx_target = 'GMX' in target_exchange_list
            is_perennial_target = 'Perennial' in target_exchange_list

            # try:
            #     if is_synthetix_target:
            #         is_synthetix_position = self.synthetix.is_already_position_open()
            # except Exception as e:
            #     logger.error(f'MasterPositionController:is_already_position_open - Error checking Synthetix position: {e}')

            # try:
            #     if is_hmx_target:
            #         is_hmx_position = self.hmx.is_already_position_open()
            # except Exception as e:
            #     logger.error(f'MasterPositionController:is_already_position_open - Error checking HMX position: {e}')

            try:
                if is_binance_target:
                    is_binance_position = self.binance.is_already_position_open()
            except Exception as e:
                logger.error(f'MasterPositionController:is_already_position_open - Error checking Binance position: {e}')

            try:
                if is_bybit_target:
                    is_bybit_position = self.bybit.is_already_position_open()
            except Exception as e:
                logger.error(f'MasterPositionController:is_already_position_open - Error checking ByBit position: {e}')

            try:
                if is_okx_target:
                    # is_okx_position = self.okx.is_already_position_open()
                    pass
            except Exception as e:
                logger.error(f'MasterPositionController:is_already_position_open - Error checking OKX position: {e}')

            try:    
                if is_gmx_target:
                    is_gmx_position = self.gmx.is_already_position_open()
            except Exception as e:
                logger.error(f'MasterPositionController:is_already_position_open - Error checking GMX position: {e}')
            
            try:
                if is_perennial_target:
                    is_perennial_position = self.perennial.is_already_position_open()
            except Exception as e:
                logger.error(f'MasterPositionController:is_already_position_open - Error checking Perennial position: {e}')

            positions_open = [
                is_synthetix_position,
                is_hmx_position,
                is_binance_position,
                is_bybit_position,
                is_gmx_position,
                is_perennial_position
            ]

            if any(positions_open):
                logger.info(f"MasterPositionController - Position already open: SNX: {is_synthetix_position}, HMX: {is_hmx_position}, Binance: {is_binance_position}, ByBit: {is_bybit_position}, GMX: {is_gmx_position}, Perennial: {is_perennial_position}")
                return True
            else:
                logger.info(f"MasterPositionController - No positions open.")
                return False

        except Exception as e:
            logger.error(f'MasterPositionController - Unexpected error when checking positions: {e}')
            return False
