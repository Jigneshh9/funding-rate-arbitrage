from dataclasses import asdict, replace
from itertools import product
import math

from Backtesting.Research.experiment_runner import ResearchExperimentRunner


def _safe_mean(values: list) -> float:
    cleaned = [value for value in values if value is not None and not (isinstance(value, float) and math.isnan(value))]
    return sum(cleaned) / len(cleaned) if cleaned else 0.0


def _safe_mean_profit_factor(values: list) -> float:
    finite_values = [value for value in values if value not in (None, float("inf"))]
    if finite_values:
        return sum(finite_values) / len(finite_values)
    return float("inf") if any(value == float("inf") for value in values) else 0.0


def build_config_variants(base_config, parameter_grid: dict) -> list:
    if not parameter_grid:
        return [{"variant_name": "base", "config": base_config, "parameters": {}}]

    fields = list(parameter_grid.keys())
    variants = []
    for values in product(*[parameter_grid[field] for field in fields]):
        parameters = dict(zip(fields, values))
        config_variant = replace(base_config, **parameters)
        variant_name = ", ".join(f"{key}={value}" for key, value in parameters.items())
        variants.append({
            "variant_name": variant_name,
            "config": config_variant,
            "parameters": parameters,
        })
    return variants


def aggregate_suite_results(results: list) -> dict:
    if not results:
        return {
            "overall": {},
            "by_strategy": {},
        }

    def aggregate(rows: list) -> dict:
        metrics = [row["metrics"] for row in rows]
        return {
            "symbols_tested": len(rows),
            "total_trade_count": sum(metric["trade_count"] for metric in metrics),
            "total_gross_pnl_usd": sum(metric["gross_pnl_usd"] for metric in metrics),
            "total_net_pnl_usd": sum(metric["net_pnl_usd"] for metric in metrics),
            "total_cost_usd": sum(metric["total_cost_usd"] for metric in metrics),
            "avg_win_rate": _safe_mean([metric["win_rate"] for metric in metrics]),
            "avg_return_pct": _safe_mean([metric["return_pct"] for metric in metrics]),
            "avg_sharpe_like": _safe_mean([metric["sharpe_like"] for metric in metrics]),
            "avg_max_drawdown_pct": _safe_mean([metric["max_drawdown_pct"] for metric in metrics]),
            "avg_profit_factor": _safe_mean_profit_factor([metric["profit_factor"] for metric in metrics]),
        }

    strategies = sorted({row["strategy_name"] for row in results})
    return {
        "overall": aggregate(results),
        "by_strategy": {
            strategy: aggregate([row for row in results if row["strategy_name"] == strategy])
            for strategy in strategies
        }
    }


def run_ablation_study(symbols: list, strategy_names: list, base_config, parameter_grid: dict = None, data_root: str = None) -> dict:
    runner = ResearchExperimentRunner(data_root=data_root)
    variants = build_config_variants(base_config, parameter_grid or {})
    runs = []

    for variant in variants:
        suite = runner.run_suite(symbols, strategy_names, variant["config"])
        aggregated = aggregate_suite_results(suite["results"])
        runs.append({
            "variant_name": variant["variant_name"],
            "parameters": variant["parameters"],
            "config": asdict(variant["config"]),
            "aggregated": aggregated,
            "suite": suite,
        })

    return {
        "symbols": symbols,
        "strategies": strategy_names,
        "base_config": asdict(base_config),
        "parameter_grid": parameter_grid or {},
        "runs": runs,
    }
