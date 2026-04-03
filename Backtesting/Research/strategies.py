from dataclasses import asdict, dataclass


LONG_SNX_SHORT_BINANCE = "long_snx_short_binance"
SHORT_SNX_LONG_BINANCE = "short_snx_long_binance"


@dataclass(frozen=True)
class BacktestConfig:
    entry_threshold: float = 0.0001
    exit_threshold: float = 0.00005
    leg_notional_usd: float = 1000.0
    fee_bps_per_leg: float = 5.0
    slippage_bps_per_leg: float = 2.0
    max_holding_observations: int = 24
    fixed_holding_observations: int = 8

    def round_trip_cost_usd(self) -> float:
        total_bps = (self.fee_bps_per_leg + self.slippage_bps_per_leg) * 4
        return self.leg_notional_usd * (total_bps / 10000)


@dataclass(frozen=True)
class TradeRecord:
    symbol: str
    strategy_name: str
    direction: str
    entry_index: int
    exit_index: int
    entry_synthetix_block: int
    exit_synthetix_block: int
    entry_spread: float
    exit_spread: float
    holding_observations: int
    gross_pnl_usd: float
    net_pnl_usd: float
    total_cost_usd: float
    exit_reason: str

    def to_dict(self) -> dict:
        return asdict(self)


def _direction_for_spread(spread: float) -> str:
    return SHORT_SNX_LONG_BINANCE if spread > 0 else LONG_SNX_SHORT_BINANCE


def _interval_pnl_usd(observation, direction: str, leg_notional_usd: float) -> float:
    if direction == SHORT_SNX_LONG_BINANCE:
        return observation.spread * leg_notional_usd
    return -observation.spread * leg_notional_usd


def _build_trade(strategy_name: str, symbol: str, observations: list, entry_index: int, exit_index: int, direction: str, config: BacktestConfig, exit_reason: str) -> TradeRecord:
    entry_observation = observations[entry_index]
    exit_observation = observations[exit_index]
    gross_pnl_usd = sum(
        _interval_pnl_usd(observation, direction, config.leg_notional_usd)
        for observation in observations[entry_index + 1: exit_index + 1]
    )
    total_cost_usd = config.round_trip_cost_usd()
    net_pnl_usd = gross_pnl_usd - total_cost_usd

    return TradeRecord(
        symbol=symbol,
        strategy_name=strategy_name,
        direction=direction,
        entry_index=entry_index,
        exit_index=exit_index,
        entry_synthetix_block=entry_observation.synthetix_block_number,
        exit_synthetix_block=exit_observation.synthetix_block_number,
        entry_spread=entry_observation.spread,
        exit_spread=exit_observation.spread,
        holding_observations=exit_index - entry_index,
        gross_pnl_usd=gross_pnl_usd,
        net_pnl_usd=net_pnl_usd,
        total_cost_usd=total_cost_usd,
        exit_reason=exit_reason,
    )


class ThresholdConvergenceStrategy:
    name = "threshold_convergence"

    def generate_trades(self, symbol: str, observations: list, config: BacktestConfig) -> list:
        trades = []
        if not observations:
            return trades

        entry_index = None
        direction = None

        for index, observation in enumerate(observations):
            spread = observation.spread

            if entry_index is None:
                if abs(spread) >= config.entry_threshold:
                    entry_index = index
                    direction = _direction_for_spread(spread)
                continue

            holding = index - entry_index
            exit_reason = None
            expected_direction = _direction_for_spread(spread) if spread != 0 else direction

            if abs(spread) <= config.exit_threshold:
                exit_reason = "spread_converged"
            elif expected_direction != direction:
                exit_reason = "spread_reversed"
            elif holding >= config.max_holding_observations:
                exit_reason = "max_holding"

            if exit_reason:
                trades.append(_build_trade(self.name, symbol, observations, entry_index, index, direction, config, exit_reason))
                entry_index = None
                direction = None

        if entry_index is not None:
            trades.append(_build_trade(self.name, symbol, observations, entry_index, len(observations) - 1, direction, config, "end_of_data"))

        return trades


class FixedHorizonStrategy:
    name = "fixed_horizon_baseline"

    def generate_trades(self, symbol: str, observations: list, config: BacktestConfig) -> list:
        trades = []
        if not observations:
            return trades

        index = 0
        while index < len(observations):
            observation = observations[index]
            if abs(observation.spread) < config.entry_threshold:
                index += 1
                continue

            direction = _direction_for_spread(observation.spread)
            exit_index = min(index + config.fixed_holding_observations, len(observations) - 1)
            trades.append(_build_trade(self.name, symbol, observations, index, exit_index, direction, config, "fixed_horizon"))
            index = exit_index + 1

        return trades


class SignFlipStrategy:
    name = "sign_flip_baseline"

    def generate_trades(self, symbol: str, observations: list, config: BacktestConfig) -> list:
        trades = []
        if not observations:
            return trades

        entry_index = None
        direction = None

        for index, observation in enumerate(observations):
            if entry_index is None:
                if abs(observation.spread) >= config.entry_threshold:
                    entry_index = index
                    direction = _direction_for_spread(observation.spread)
                continue

            expected_direction = _direction_for_spread(observation.spread) if observation.spread != 0 else direction
            if expected_direction != direction:
                trades.append(_build_trade(self.name, symbol, observations, entry_index, index, direction, config, "sign_flip"))
                entry_index = None
                direction = None

        if entry_index is not None:
            trades.append(_build_trade(self.name, symbol, observations, entry_index, len(observations) - 1, direction, config, "end_of_data"))

        return trades


STRATEGY_REGISTRY = {
    ThresholdConvergenceStrategy.name: ThresholdConvergenceStrategy,
    FixedHorizonStrategy.name: FixedHorizonStrategy,
    SignFlipStrategy.name: SignFlipStrategy,
}
