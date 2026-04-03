import csv
import json
import os
from datetime import UTC, datetime


def _sanitize_name(value: str) -> str:
    return "".join(char if char.isalnum() or char in ("_", "-") else "_" for char in value)


def _write_markdown_table(path: str, headers: list, rows: list):
    with open(path, "w", encoding="utf-8") as file:
        file.write("| " + " | ".join(headers) + " |\n")
        file.write("| " + " | ".join(["---"] * len(headers)) + " |\n")
        for row in rows:
            file.write("| " + " | ".join(str(value) for value in row) + " |\n")


def generate_suite_report(suite: dict, output_dir: str) -> dict:
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    summary_csv = os.path.join(output_dir, f"suite_summary_{timestamp}.csv")
    summary_md = os.path.join(output_dir, f"suite_summary_{timestamp}.md")
    figure_path = os.path.join(output_dir, f"suite_net_pnl_{timestamp}.svg")

    rows = []
    for result in suite["results"]:
        metric = result["metrics"]
        rows.append({
            "symbol": result["symbol"],
            "strategy_name": result["strategy_name"],
            "trade_count": metric["trade_count"],
            "net_pnl_usd": metric["net_pnl_usd"],
            "return_pct": metric["return_pct"],
            "win_rate": metric["win_rate"],
            "sharpe_like": metric["sharpe_like"],
            "max_drawdown_pct": metric["max_drawdown_pct"],
        })

    with open(summary_csv, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=list(rows[0].keys()) if rows else [
            "symbol", "strategy_name", "trade_count", "net_pnl_usd", "return_pct", "win_rate", "sharpe_like", "max_drawdown_pct"
        ])
        writer.writeheader()
        writer.writerows(rows)

    markdown_rows = [
        [
            row["symbol"],
            row["strategy_name"],
            row["trade_count"],
            f'{row["net_pnl_usd"]:.2f}',
            f'{row["return_pct"]:.4f}',
            f'{row["win_rate"]:.4f}',
            f'{row["sharpe_like"]:.4f}',
            f'{row["max_drawdown_pct"]:.4f}',
        ]
        for row in rows
    ]
    _write_markdown_table(
        summary_md,
        ["Symbol", "Strategy", "Trades", "Net PnL (USD)", "Return %", "Win Rate", "Sharpe-like", "Max DD %"],
        markdown_rows,
    )

    _generate_bar_chart(
        labels=[f'{row["symbol"]}\n{row["strategy_name"]}' for row in rows],
        values=[row["net_pnl_usd"] for row in rows],
        title="Net PnL by Symbol and Strategy",
        ylabel="Net PnL (USD)",
        output_path=figure_path,
    )

    return {
        "summary_csv": summary_csv,
        "summary_md": summary_md,
        "net_pnl_figure": figure_path,
    }


def generate_ablation_report(study: dict, output_dir: str) -> dict:
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now(UTC).strftime("%Y%m%d_%H%M%S")
    summary_json = os.path.join(output_dir, f"ablation_summary_{timestamp}.json")
    summary_csv = os.path.join(output_dir, f"ablation_summary_{timestamp}.csv")
    summary_md = os.path.join(output_dir, f"ablation_summary_{timestamp}.md")
    figure_path = os.path.join(output_dir, f"ablation_net_pnl_{timestamp}.svg")

    json_payload = []
    csv_rows = []
    markdown_rows = []

    for run in study["runs"]:
        variant_name = run["variant_name"]
        for strategy_name, aggregate in run["aggregated"]["by_strategy"].items():
            row = {
                "variant_name": variant_name,
                "strategy_name": strategy_name,
                "total_trade_count": aggregate["total_trade_count"],
                "total_net_pnl_usd": aggregate["total_net_pnl_usd"],
                "avg_return_pct": aggregate["avg_return_pct"],
                "avg_win_rate": aggregate["avg_win_rate"],
                "avg_sharpe_like": aggregate["avg_sharpe_like"],
                "avg_max_drawdown_pct": aggregate["avg_max_drawdown_pct"],
            }
            csv_rows.append(row)
            markdown_rows.append([
                variant_name,
                strategy_name,
                aggregate["total_trade_count"],
                f'{aggregate["total_net_pnl_usd"]:.2f}',
                f'{aggregate["avg_return_pct"]:.4f}',
                f'{aggregate["avg_win_rate"]:.4f}',
                f'{aggregate["avg_sharpe_like"]:.4f}',
                f'{aggregate["avg_max_drawdown_pct"]:.4f}',
            ])
            json_payload.append({
                "variant_name": variant_name,
                "strategy_name": strategy_name,
                "parameters": run["parameters"],
                "aggregate": aggregate,
            })

    with open(summary_json, "w", encoding="utf-8") as file:
        json.dump(json_payload, file, indent=2)

    with open(summary_csv, "w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=list(csv_rows[0].keys()) if csv_rows else [
            "variant_name", "strategy_name", "total_trade_count", "total_net_pnl_usd", "avg_return_pct", "avg_win_rate", "avg_sharpe_like", "avg_max_drawdown_pct"
        ])
        writer.writeheader()
        writer.writerows(csv_rows)

    _write_markdown_table(
        summary_md,
        ["Variant", "Strategy", "Trades", "Total Net PnL (USD)", "Avg Return %", "Avg Win Rate", "Avg Sharpe-like", "Avg Max DD %"],
        markdown_rows,
    )

    _generate_grouped_bar_chart(
        rows=csv_rows,
        category_key="variant_name",
        series_key="strategy_name",
        value_key="total_net_pnl_usd",
        title="Ablation Study: Total Net PnL by Variant and Strategy",
        ylabel="Total Net PnL (USD)",
        output_path=figure_path,
    )

    return {
        "summary_json": summary_json,
        "summary_csv": summary_csv,
        "summary_md": summary_md,
        "net_pnl_figure": figure_path,
    }


def _generate_bar_chart(labels: list, values: list, title: str, ylabel: str, output_path: str):
    width = 1200
    height = 700
    margin_left = 80
    margin_right = 40
    margin_top = 70
    margin_bottom = 180
    chart_width = width - margin_left - margin_right
    chart_height = height - margin_top - margin_bottom

    max_abs_value = max([abs(value) for value in values] + [1.0])
    zero_y = margin_top + chart_height / 2
    bar_width = chart_width / max(len(labels), 1) * 0.6
    gap = chart_width / max(len(labels), 1)

    bars = []
    label_nodes = []
    for index, (label, value) in enumerate(zip(labels, values)):
        center_x = margin_left + gap * index + gap / 2
        scaled_height = (abs(value) / max_abs_value) * (chart_height / 2 - 10)
        y = zero_y - scaled_height if value >= 0 else zero_y
        color = "#22c55e" if value >= 0 else "#ef4444"
        bars.append(
            f'<rect x="{center_x - bar_width / 2:.1f}" y="{y:.1f}" width="{bar_width:.1f}" height="{scaled_height:.1f}" fill="{color}" opacity="0.9" />'
        )
        label_nodes.append(
            f'<text x="{center_x:.1f}" y="{height - margin_bottom + 20}" font-size="11" fill="#334155" text-anchor="middle">{label}</text>'
        )

    svg = f'''<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">
  <rect width="100%" height="100%" fill="white" />
  <text x="{width / 2}" y="30" font-size="22" font-family="Arial" text-anchor="middle" fill="#0f172a">{title}</text>
  <text x="25" y="{margin_top + chart_height / 2}" font-size="14" font-family="Arial" transform="rotate(-90 25,{margin_top + chart_height / 2})" fill="#334155">{ylabel}</text>
  <line x1="{margin_left}" y1="{zero_y}" x2="{width - margin_right}" y2="{zero_y}" stroke="#94a3b8" stroke-width="1" />
  <line x1="{margin_left}" y1="{margin_top}" x2="{margin_left}" y2="{height - margin_bottom}" stroke="#94a3b8" stroke-width="1" />
  {''.join(bars)}
  {''.join(label_nodes)}
</svg>'''
    with open(output_path, "w", encoding="utf-8") as file:
        file.write(svg)


def _generate_grouped_bar_chart(rows: list, category_key: str, series_key: str, value_key: str, title: str, ylabel: str, output_path: str):
    categories = sorted({row[category_key] for row in rows})
    series_names = sorted({row[series_key] for row in rows})
    values_by_series = {series_name: [] for series_name in series_names}

    for category in categories:
        rows_for_category = {row[series_key]: row[value_key] for row in rows if row[category_key] == category}
        for series_name in series_names:
            values_by_series[series_name].append(rows_for_category.get(series_name, 0.0))

    width = 1200
    height = 700
    margin_left = 80
    margin_right = 40
    margin_top = 80
    margin_bottom = 180
    chart_width = width - margin_left - margin_right
    chart_height = height - margin_top - margin_bottom
    max_abs_value = max(
        [abs(value) for values in values_by_series.values() for value in values] + [1.0]
    )
    zero_y = margin_top + chart_height / 2
    category_gap = chart_width / max(len(categories), 1)
    group_width = category_gap * 0.75
    series_bar_width = group_width / max(len(series_names), 1)
    palette = ["#2563eb", "#22c55e", "#f59e0b", "#8b5cf6", "#ef4444"]

    bar_nodes = []
    label_nodes = []
    legend_nodes = []

    for series_index, series_name in enumerate(series_names):
        color = palette[series_index % len(palette)]
        legend_nodes.append(
            f'<rect x="{margin_left + series_index * 140}" y="45" width="18" height="10" fill="{color}" />'
            f'<text x="{margin_left + series_index * 140 + 24}" y="54" font-size="12" font-family="Arial" fill="#334155">{series_name}</text>'
        )
        for category_index, category in enumerate(categories):
            center_x = margin_left + category_gap * category_index + category_gap / 2
            start_x = center_x - group_width / 2 + series_index * series_bar_width
            value = values_by_series[series_name][category_index]
            scaled_height = (abs(value) / max_abs_value) * (chart_height / 2 - 10)
            y = zero_y - scaled_height if value >= 0 else zero_y
            bar_nodes.append(
                f'<rect x="{start_x:.1f}" y="{y:.1f}" width="{series_bar_width * 0.9:.1f}" height="{scaled_height:.1f}" fill="{color}" opacity="0.9" />'
            )

    for category_index, category in enumerate(categories):
        center_x = margin_left + category_gap * category_index + category_gap / 2
        label_nodes.append(
            f'<text x="{center_x:.1f}" y="{height - margin_bottom + 20}" font-size="11" fill="#334155" text-anchor="middle">{_sanitize_name(category)}</text>'
        )

    svg = f'''<svg xmlns="http://www.w3.org/2000/svg" width="{width}" height="{height}">
  <rect width="100%" height="100%" fill="white" />
  <text x="{width / 2}" y="30" font-size="22" font-family="Arial" text-anchor="middle" fill="#0f172a">{title}</text>
  <text x="25" y="{margin_top + chart_height / 2}" font-size="14" font-family="Arial" transform="rotate(-90 25,{margin_top + chart_height / 2})" fill="#334155">{ylabel}</text>
  <line x1="{margin_left}" y1="{zero_y}" x2="{width - margin_right}" y2="{zero_y}" stroke="#94a3b8" stroke-width="1" />
  <line x1="{margin_left}" y1="{margin_top}" x2="{margin_left}" y2="{height - margin_bottom}" stroke="#94a3b8" stroke-width="1" />
  {''.join(legend_nodes)}
  {''.join(bar_nodes)}
  {''.join(label_nodes)}
</svg>'''
    with open(output_path, "w", encoding="utf-8") as file:
        file.write(svg)
