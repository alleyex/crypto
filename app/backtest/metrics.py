import math
from typing import Any, Dict, List, Optional


def _daily_closes(equity_curve: List[Dict]) -> List[float]:
    """Return the last equity value per calendar day (UTC)."""
    daily: Dict[str, float] = {}
    for point in equity_curve:
        date = point["timestamp"][:10]
        daily[date] = point["equity"]
    return [daily[d] for d in sorted(daily)]


def _sharpe_ratio(daily_closes: List[float], periods_per_year: int = 252) -> Optional[float]:
    if len(daily_closes) < 2:
        return None
    returns = [
        (daily_closes[i] - daily_closes[i - 1]) / daily_closes[i - 1]
        for i in range(1, len(daily_closes))
        if daily_closes[i - 1] > 0
    ]
    if len(returns) < 2:
        return None
    n = len(returns)
    mean = sum(returns) / n
    variance = sum((r - mean) ** 2 for r in returns) / (n - 1)
    if variance <= 0:
        return None
    return round((mean / math.sqrt(variance)) * math.sqrt(periods_per_year), 4)


def _max_drawdown_pct(equity_curve: List[Dict]) -> float:
    if not equity_curve:
        return 0.0
    peak = equity_curve[0]["equity"]
    max_dd = 0.0
    for point in equity_curve:
        eq = point["equity"]
        if eq > peak:
            peak = eq
        if peak > 0:
            dd = (peak - eq) / peak
            if dd > max_dd:
                max_dd = dd
    return round(max_dd * 100, 4)


def _round_trip_stats(trades: List[Dict]) -> Dict[str, Any]:
    """Compute win rate and profit factor from sequential BUY/SELL pairs."""
    buy_price: Optional[float] = None
    wins = losses = 0
    gross_profit = gross_loss = 0.0

    for trade in trades:
        if trade["side"] == "BUY":
            buy_price = float(trade["price"])
        elif trade["side"] == "SELL" and buy_price is not None:
            pnl = (float(trade["price"]) - buy_price) * float(trade["qty"])
            if pnl > 0:
                wins += 1
                gross_profit += pnl
            else:
                losses += 1
                gross_loss += abs(pnl)
            buy_price = None

    total = wins + losses
    win_rate = round(wins / total * 100, 2) if total > 0 else None
    profit_factor: Optional[float]
    if gross_loss == 0:
        profit_factor = None if gross_profit == 0 else float("inf")
    else:
        profit_factor = round(gross_profit / gross_loss, 4)

    return {
        "win_rate_pct": win_rate,
        "profit_factor": profit_factor,
        "round_trips": total,
    }


def compute_metrics(
    equity_curve: List[Dict],
    trades: List[Dict],
    initial_capital: float,
) -> Dict[str, Any]:
    if not equity_curve:
        return {}

    final_equity = equity_curve[-1]["equity"]
    total_return_pct = (
        round((final_equity - initial_capital) / initial_capital * 100, 4)
        if initial_capital > 0
        else 0.0
    )
    closes = _daily_closes(equity_curve)
    rt = _round_trip_stats(trades)

    return {
        "initial_capital": initial_capital,
        "final_equity": round(final_equity, 8),
        "total_return_pct": total_return_pct,
        "max_drawdown_pct": _max_drawdown_pct(equity_curve),
        "sharpe_ratio": _sharpe_ratio(closes),
        "win_rate_pct": rt["win_rate_pct"],
        "profit_factor": rt["profit_factor"],
        "round_trips": rt["round_trips"],
        "trade_count": len(trades),
    }
