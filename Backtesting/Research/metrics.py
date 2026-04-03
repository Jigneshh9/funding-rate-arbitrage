import math


def _equity_curve(trades: list) -> list:
    curve = []
    cumulative = 0.0
    for trade in trades:
        cumulative += trade.net_pnl_usd
        curve.append(cumulative)
    return curve


def _max_drawdown(equity_curve: list) -> tuple:
    if not equity_curve:
        return 0.0, 0.0

    peak = equity_curve[0]
    max_dd_usd = 0.0
    max_dd_pct = 0.0

    for value in equity_curve:
        peak = max(peak, value)
        drawdown = peak - value
        max_dd_usd = max(max_dd_usd, drawdown)
        if peak != 0:
            max_dd_pct = max(max_dd_pct, drawdown / abs(peak))

    return max_dd_usd, max_dd_pct


def summarize_trades(trades: list, initial_capital_usd: float = 10000.0) -> dict:
    if not trades:
        return {
            "trade_count": 0,
            "gross_pnl_usd": 0.0,
            "net_pnl_usd": 0.0,
            "total_cost_usd": 0.0,
            "win_rate": 0.0,
            "profit_factor": 0.0,
            "average_trade_pnl_usd": 0.0,
            "average_holding_observations": 0.0,
            "best_trade_usd": 0.0,
            "worst_trade_usd": 0.0,
            "expectancy_usd": 0.0,
            "max_drawdown_usd": 0.0,
            "max_drawdown_pct": 0.0,
            "ending_equity_usd": initial_capital_usd,
            "return_pct": 0.0,
            "sharpe_like": 0.0,
        }

    gross_pnl_usd = sum(trade.gross_pnl_usd for trade in trades)
    net_pnl_usd = sum(trade.net_pnl_usd for trade in trades)
    total_cost_usd = sum(trade.total_cost_usd for trade in trades)
    winners = [trade.net_pnl_usd for trade in trades if trade.net_pnl_usd > 0]
    losers = [trade.net_pnl_usd for trade in trades if trade.net_pnl_usd < 0]
    win_rate = len(winners) / len(trades)
    average_trade_pnl_usd = net_pnl_usd / len(trades)
    average_holding_observations = sum(trade.holding_observations for trade in trades) / len(trades)
    expectancy_usd = average_trade_pnl_usd
    best_trade_usd = max(trade.net_pnl_usd for trade in trades)
    worst_trade_usd = min(trade.net_pnl_usd for trade in trades)
    ending_equity_usd = initial_capital_usd + net_pnl_usd
    return_pct = (net_pnl_usd / initial_capital_usd) if initial_capital_usd else 0.0

    gross_profit = sum(winners)
    gross_loss = abs(sum(losers))
    profit_factor = gross_profit / gross_loss if gross_loss else (float("inf") if gross_profit > 0 else 0.0)

    equity_curve = _equity_curve(trades)
    max_drawdown_usd, max_drawdown_pct = _max_drawdown(equity_curve)

    trade_returns = [trade.net_pnl_usd / initial_capital_usd for trade in trades if initial_capital_usd]
    if len(trade_returns) >= 2:
        mean_return = sum(trade_returns) / len(trade_returns)
        variance = sum((value - mean_return) ** 2 for value in trade_returns) / (len(trade_returns) - 1)
        std_dev = math.sqrt(variance)
        sharpe_like = (mean_return / std_dev) * math.sqrt(len(trade_returns)) if std_dev else 0.0
    else:
        sharpe_like = 0.0

    return {
        "trade_count": len(trades),
        "gross_pnl_usd": gross_pnl_usd,
        "net_pnl_usd": net_pnl_usd,
        "total_cost_usd": total_cost_usd,
        "win_rate": win_rate,
        "profit_factor": profit_factor,
        "average_trade_pnl_usd": average_trade_pnl_usd,
        "average_holding_observations": average_holding_observations,
        "best_trade_usd": best_trade_usd,
        "worst_trade_usd": worst_trade_usd,
        "expectancy_usd": expectancy_usd,
        "max_drawdown_usd": max_drawdown_usd,
        "max_drawdown_pct": max_drawdown_pct,
        "ending_equity_usd": ending_equity_usd,
        "return_pct": return_pct,
        "sharpe_like": sharpe_like,
    }
