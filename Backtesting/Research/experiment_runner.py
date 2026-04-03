import csv
import json
import os
from dataclasses import asdict, dataclass
from datetime import UTC, datetime

from Backtesting.Research.dataset import load_aligned_symbol_dataset
from Backtesting.Research.metrics import summarize_trades
from Backtesting.Research.strategies import BacktestConfig, STRATEGY_REGISTRY


@dataclass(frozen=True)
class ExperimentResult:
    symbol: str
    strategy_name: str
    config: dict
    metrics: dict
    observations_used: int
    trades: list

    def to_dict(self) -> dict:
        return {
            "symbol": self.symbol,
            "strategy_name": self.strategy_name,
            "config": self.config,
            "metrics": self.metrics,
            "observations_used": self.observations_used,
            "trades": self.trades,
        }


class ResearchExperimentRunner:
    def __init__(self, data_root: str = None):
        self.data_root = data_root

    def run_symbol_strategy(self, symbol: str, strategy_name: str, config: BacktestConfig) -> ExperimentResult:
        if strategy_name not in STRATEGY_REGISTRY:
            raise ValueError(f"Unknown strategy '{strategy_name}'")

        observations = load_aligned_symbol_dataset(symbol, data_root=self.data_root)
        strategy = STRATEGY_REGISTRY[strategy_name]()
        trades = strategy.generate_trades(symbol, observations, config)
        metrics = summarize_trades(trades)

        return ExperimentResult(
            symbol=symbol,
            strategy_name=strategy_name,
            config=asdict(config),
            metrics=metrics,
            observations_used=len(observations),
            trades=[trade.to_dict() for trade in trades],
        )

    def run_suite(self, symbols: list, strategy_names: list, config: BacktestConfig) -> dict:
        results = []
        for symbol in symbols:
            for strategy_name in strategy_names:
                results.append(self.run_symbol_strategy(symbol, strategy_name, config))

        return {
            "generated_at": datetime.now(UTC).isoformat().replace("+00:00", "Z"),
            "symbols": symbols,
            "strategies": strategy_names,
            "config": asdict(config),
            "results": [result.to_dict() for result in results],
        }

    def export_suite(self, suite: dict, output_dir: str) -> dict:
        os.makedirs(output_dir, exist_ok=True)
        timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
        json_path = os.path.join(output_dir, f"research_suite_{timestamp}.json")
        csv_path = os.path.join(output_dir, f"research_suite_{timestamp}.csv")

        with open(json_path, "w", encoding="utf-8") as file:
            json.dump(suite, file, indent=2)

        with open(csv_path, "w", newline="", encoding="utf-8") as file:
            writer = csv.DictWriter(
                file,
                fieldnames=[
                    "symbol",
                    "strategy_name",
                    "observations_used",
                    "trade_count",
                    "gross_pnl_usd",
                    "net_pnl_usd",
                    "total_cost_usd",
                    "win_rate",
                    "profit_factor",
                    "average_trade_pnl_usd",
                    "average_holding_observations",
                    "best_trade_usd",
                    "worst_trade_usd",
                    "expectancy_usd",
                    "max_drawdown_usd",
                    "max_drawdown_pct",
                    "ending_equity_usd",
                    "return_pct",
                    "sharpe_like",
                ],
            )
            writer.writeheader()
            for result in suite["results"]:
                row = {
                    "symbol": result["symbol"],
                    "strategy_name": result["strategy_name"],
                    "observations_used": result["observations_used"],
                }
                row.update(result["metrics"])
                writer.writerow(row)

        return {
            "json_path": json_path,
            "csv_path": csv_path,
        }
