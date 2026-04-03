import argparse
from dataclasses import fields
import json
import os

from Backtesting.Research.ablation import run_ablation_study
from Backtesting.Research.experiment_runner import ResearchExperimentRunner
from Backtesting.Research.reporting import generate_ablation_report, generate_suite_report
from Backtesting.Research.strategies import BacktestConfig, STRATEGY_REGISTRY


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run research-grade arbitrage backtest experiments.")
    parser.add_argument("--symbols", nargs="+", default=["BTC", "ETH"], help="Symbols to test.")
    parser.add_argument(
        "--strategies",
        nargs="+",
        default=list(STRATEGY_REGISTRY.keys()),
        choices=list(STRATEGY_REGISTRY.keys()),
        help="Strategies/baselines to run.",
    )
    parser.add_argument("--entry-threshold", type=float, default=0.0001)
    parser.add_argument("--exit-threshold", type=float, default=0.00005)
    parser.add_argument("--leg-notional-usd", type=float, default=1000.0)
    parser.add_argument("--fee-bps-per-leg", type=float, default=5.0)
    parser.add_argument("--slippage-bps-per-leg", type=float, default=2.0)
    parser.add_argument("--max-holding-observations", type=int, default=24)
    parser.add_argument("--fixed-holding-observations", type=int, default=8)
    parser.add_argument(
        "--output-dir",
        default=os.path.join("Backtesting", "Research", "results"),
        help="Where to write exported suite results.",
    )
    parser.add_argument(
        "--ablation",
        action="append",
        default=[],
        help="Parameter sweep in the form field=value1,value2,... Example: --ablation entry_threshold=0.00005,0.0001",
    )
    parser.add_argument(
        "--report",
        action="store_true",
        help="Generate CSV/Markdown/PNG report artifacts in addition to raw exports.",
    )
    return parser


def _parse_ablation_specs(specs: list) -> dict:
    if not specs:
        return {}

    field_types = {field.name: field.type for field in fields(BacktestConfig)}
    parsed = {}
    for spec in specs:
        if "=" not in spec:
            raise ValueError(f"Invalid ablation spec '{spec}'. Expected field=value1,value2")
        field_name, raw_values = spec.split("=", 1)
        field_name = field_name.strip()
        if field_name not in field_types:
            raise ValueError(f"Unknown BacktestConfig field '{field_name}'")

        converter = int if field_types[field_name] is int else float
        parsed[field_name] = [converter(value.strip()) for value in raw_values.split(",") if value.strip()]
    return parsed


def main():
    args = build_parser().parse_args()
    config = BacktestConfig(
        entry_threshold=args.entry_threshold,
        exit_threshold=args.exit_threshold,
        leg_notional_usd=args.leg_notional_usd,
        fee_bps_per_leg=args.fee_bps_per_leg,
        slippage_bps_per_leg=args.slippage_bps_per_leg,
        max_holding_observations=args.max_holding_observations,
        fixed_holding_observations=args.fixed_holding_observations,
    )

    ablation_grid = _parse_ablation_specs(args.ablation)

    if ablation_grid:
        study = run_ablation_study(args.symbols, args.strategies, config, parameter_grid=ablation_grid)
        runner = ResearchExperimentRunner()
        export_payload = {"runs": []}
        for index, run in enumerate(study["runs"]):
            suite_output_dir = os.path.join(args.output_dir, f"variant_{index + 1}")
            suite_exports = runner.export_suite(run["suite"], suite_output_dir)
            export_payload["runs"].append({
                "variant_name": run["variant_name"],
                "suite_exports": suite_exports,
            })

        if args.report:
            export_payload["report_exports"] = generate_ablation_report(study, args.output_dir)

        print(json.dumps({"summary": study["runs"], "exports": export_payload}, indent=2))
        return

    runner = ResearchExperimentRunner()
    suite = runner.run_suite(args.symbols, args.strategies, config)
    export_paths = runner.export_suite(suite, args.output_dir)
    if args.report:
        export_paths["report_exports"] = generate_suite_report(suite, args.output_dir)

    print(json.dumps({"summary": suite["results"], "exports": export_paths}, indent=2))


if __name__ == "__main__":
    main()
