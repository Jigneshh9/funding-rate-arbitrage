import json
import os
import shutil
import sys
import tempfile
import unittest
import uuid

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


class TestResearchDataset(unittest.TestCase):
    def test_align_histories_matches_nearest_blocks(self):
        from Backtesting.Research.dataset import align_histories

        synthetix_rows = [
            {"symbol": "BTC", "block_number": 100, "funding_rate": 0.0010, "price": 65000},
            {"symbol": "BTC", "block_number": 210, "funding_rate": -0.0005, "price": 65200},
        ]
        binance_rows = [
            {"symbol": "BTC", "block_number": 90, "funding_rate": 0.0002, "price": 64900},
            {"symbol": "BTC", "block_number": 205, "funding_rate": -0.0001, "price": 65150},
        ]

        observations = align_histories(synthetix_rows, binance_rows, max_block_gap=20)

        self.assertEqual(len(observations), 2)
        self.assertEqual(observations[0].binance_block_number, 90)
        self.assertAlmostEqual(observations[0].spread, 0.0008)
        self.assertEqual(observations[1].binance_block_number, 205)


class TestResearchStrategies(unittest.TestCase):
    def _build_observations(self):
        from Backtesting.Research.dataset import FundingObservation

        spreads = [0.00015, 0.00014, 0.00004, -0.00012, -0.00003]
        observations = []
        for index, spread in enumerate(spreads):
            observations.append(
                FundingObservation(
                    symbol="BTC",
                    synthetix_block_number=100 + index,
                    binance_block_number=100 + index,
                    synthetix_funding_rate=spread,
                    binance_funding_rate=0.0,
                    synthetix_price=65000.0,
                    binance_price=65000.0,
                    spread=spread,
                )
            )
        return observations

    def test_threshold_strategy_generates_convergence_and_reversal_trades(self):
        from Backtesting.Research.strategies import BacktestConfig, ThresholdConvergenceStrategy

        strategy = ThresholdConvergenceStrategy()
        config = BacktestConfig(entry_threshold=0.0001, exit_threshold=0.00005, leg_notional_usd=1000.0)

        trades = strategy.generate_trades("BTC", self._build_observations(), config)

        self.assertEqual(len(trades), 2)
        self.assertEqual(trades[0].exit_reason, "spread_converged")
        self.assertEqual(trades[1].exit_reason, "spread_converged")

    def test_fixed_horizon_strategy_respects_holding_window(self):
        from Backtesting.Research.strategies import BacktestConfig, FixedHorizonStrategy

        strategy = FixedHorizonStrategy()
        config = BacktestConfig(entry_threshold=0.0001, fixed_holding_observations=2, leg_notional_usd=1000.0)

        trades = strategy.generate_trades("BTC", self._build_observations(), config)

        self.assertEqual(len(trades), 2)
        self.assertEqual(trades[0].holding_observations, 2)


class TestResearchMetricsAndRunner(unittest.TestCase):
    def test_metrics_summary_basic_fields(self):
        from Backtesting.Research.metrics import summarize_trades
        from Backtesting.Research.strategies import TradeRecord

        trades = [
            TradeRecord("BTC", "threshold_convergence", "short_snx_long_binance", 0, 2, 100, 102, 0.0002, 0.0, 2, 30.0, 20.0, 10.0, "spread_converged"),
            TradeRecord("BTC", "threshold_convergence", "long_snx_short_binance", 3, 4, 103, 104, -0.0002, 0.0, 1, -10.0, -20.0, 10.0, "spread_reversed"),
        ]

        summary = summarize_trades(trades, initial_capital_usd=1000.0)

        self.assertEqual(summary["trade_count"], 2)
        self.assertEqual(summary["net_pnl_usd"], 0.0)
        self.assertGreaterEqual(summary["max_drawdown_usd"], 0.0)

    def test_runner_exports_json_and_csv(self):
        from Backtesting.Research.experiment_runner import ResearchExperimentRunner
        from Backtesting.Research.reporting import generate_suite_report
        from Backtesting.Research.strategies import BacktestConfig

        temp_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            f"research_test_{uuid.uuid4().hex}"
        )
        os.makedirs(temp_dir, exist_ok=True)
        try:
            data_root = os.path.join(temp_dir, "historicalDataJSON")
            os.makedirs(os.path.join(data_root, "Binance"), exist_ok=True)
            os.makedirs(os.path.join(data_root, "Synthetix"), exist_ok=True)

            with open(os.path.join(data_root, "Binance", "BTCHistorical.json"), "w", encoding="utf-8") as file:
                json.dump([
                    {"markPrice": "65000", "block_number": 100, "funding_rate": "0.0001"},
                    {"markPrice": "65100", "block_number": 101, "funding_rate": "0.0001"},
                    {"markPrice": "65200", "block_number": 102, "funding_rate": "0.0001"},
                ], file)

            with open(os.path.join(data_root, "Synthetix", "BTCHistorical.json"), "w", encoding="utf-8") as file:
                json.dump([
                    {"price": 65050, "block_number": 100, "funding_rate": 0.0003},
                    {"price": 65150, "block_number": 101, "funding_rate": 0.00025},
                    {"price": 65250, "block_number": 102, "funding_rate": 0.00001},
                ], file)

            runner = ResearchExperimentRunner(data_root=data_root)
            suite = runner.run_suite(["BTC"], ["threshold_convergence"], BacktestConfig())
            exports = runner.export_suite(suite, os.path.join(temp_dir, "results"))
            report_exports = generate_suite_report(suite, os.path.join(temp_dir, "results"))

            self.assertTrue(os.path.exists(exports["json_path"]))
            self.assertTrue(os.path.exists(exports["csv_path"]))
            self.assertTrue(os.path.exists(report_exports["summary_csv"]))
            self.assertTrue(os.path.exists(report_exports["summary_md"]))
            self.assertTrue(os.path.exists(report_exports["net_pnl_figure"]))
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)


class TestResearchAblationAndReporting(unittest.TestCase):
    def test_build_config_variants(self):
        from Backtesting.Research.ablation import build_config_variants
        from Backtesting.Research.strategies import BacktestConfig

        variants = build_config_variants(
            BacktestConfig(),
            {"entry_threshold": [0.0001, 0.0002], "fee_bps_per_leg": [5.0, 10.0]},
        )
        self.assertEqual(len(variants), 4)

    def test_run_ablation_study_and_generate_report(self):
        from Backtesting.Research.ablation import run_ablation_study
        from Backtesting.Research.reporting import generate_ablation_report
        from Backtesting.Research.strategies import BacktestConfig

        temp_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)),
            f"research_ablation_{uuid.uuid4().hex}"
        )
        os.makedirs(temp_dir, exist_ok=True)
        try:
            data_root = os.path.join(temp_dir, "historicalDataJSON")
            os.makedirs(os.path.join(data_root, "Binance"), exist_ok=True)
            os.makedirs(os.path.join(data_root, "Synthetix"), exist_ok=True)

            with open(os.path.join(data_root, "Binance", "BTCHistorical.json"), "w", encoding="utf-8") as file:
                json.dump([
                    {"markPrice": "65000", "block_number": 100, "funding_rate": "0.0001"},
                    {"markPrice": "65100", "block_number": 101, "funding_rate": "0.0001"},
                    {"markPrice": "65200", "block_number": 102, "funding_rate": "0.0001"},
                ], file)

            with open(os.path.join(data_root, "Synthetix", "BTCHistorical.json"), "w", encoding="utf-8") as file:
                json.dump([
                    {"price": 65050, "block_number": 100, "funding_rate": 0.0003},
                    {"price": 65150, "block_number": 101, "funding_rate": 0.00025},
                    {"price": 65250, "block_number": 102, "funding_rate": 0.00001},
                ], file)

            study = run_ablation_study(
                symbols=["BTC"],
                strategy_names=["threshold_convergence", "fixed_horizon_baseline"],
                base_config=BacktestConfig(),
                parameter_grid={"entry_threshold": [0.0001, 0.0002]},
                data_root=data_root,
            )
            report_exports = generate_ablation_report(study, os.path.join(temp_dir, "reports"))

            self.assertEqual(len(study["runs"]), 2)
            self.assertTrue(os.path.exists(report_exports["summary_json"]))
            self.assertTrue(os.path.exists(report_exports["summary_csv"]))
            self.assertTrue(os.path.exists(report_exports["summary_md"]))
            self.assertTrue(os.path.exists(report_exports["net_pnl_figure"]))
        finally:
            shutil.rmtree(temp_dir, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
