from __future__ import annotations
import pandas as pd

def backtest_from_signal_returns(
    signal_mask: pd.Series,
    trade_returns: pd.Series,
) -> pd.DataFrame:
    """
    Assumes trade_returns is aligned to the same index as signal_mask and represents
    the return of taking the trade when signal_mask[t] is True.
    """
    if not signal_mask.index.equals(trade_returns.index):
        raise ValueError("signal_mask and trade_returns must share index")

    taken = signal_mask.astype(bool)
    r = trade_returns.where(taken, 0.0).fillna(0.0)

    equity = (1.0 + r).cumprod()
    out = pd.DataFrame({
        "trade_return": trade_returns,
        "taken": taken,
        "strategy_return": r,
        "equity": equity,
    }, index=signal_mask.index)
    return out
