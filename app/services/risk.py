"""Kill switch: caps, halt file and loss breakers. Delete the halt file to resume."""
import json
import logging
import time
from pathlib import Path
from typing import Optional, Tuple

import requests

from app.settings.settings import settings, BASE_DIR_2

logger = logging.getLogger(__name__)

_shutdown_event = None  # set by the executor so halt() can stop the app
_start_balances: Optional[Tuple[float, float]] = None  # per process, captured on first check


def set_shutdown_event(event) -> None:
    global _shutdown_event
    _shutdown_event = event


def _halt_path() -> Path:
    return BASE_DIR_2 / settings.RISK_HALT_FILE


def _state_path() -> Path:
    return _halt_path().with_name("risk_state.json")


def _load() -> dict:
    try:
        s = json.loads(_state_path().read_text())
    except (OSError, ValueError):
        s = {}
    for k, v in (("trades", []), ("open", []), ("failures", 0), ("pnl", 0.0), ("start", None)):
        s.setdefault(k, v)
    return s


def _save(s: dict) -> None:
    _state_path().write_text(json.dumps(s))


def _legs(opp, size) -> Tuple[float, float]:
    """Dollar cost of (yes leg, no leg)."""
    return float(opp.buy_yes_price) * float(size), float(opp.buy_no_price) * float(size)


def check(opportunity, size, balances: Optional[Tuple[float, float]] = None) -> Optional[str]:
    """Return a refusal reason, or None if the trade may go ahead.
    balances = (kalshi_usd, polymarket_usdc_e) live; missing balances refuse the trade."""
    global _start_balances
    if _halt_path().exists():
        return f"halt file present: {_halt_path()}"
    s = _load()
    yes, no = _legs(opportunity, size)
    if max(yes, no) > float(settings.RISK_MAX_NOTIONAL_PER_TRADE):
        return f"notional per leg ${max(yes, no):.2f} > cap {settings.RISK_MAX_NOTIONAL_PER_TRADE}"
    exposure = sum(s["open"])
    if exposure + yes + no > float(settings.RISK_MAX_OPEN_EXPOSURE):
        return f"open exposure ${exposure + yes + no:.2f} > cap {settings.RISK_MAX_OPEN_EXPOSURE}"
    now = time.time()
    if sum(t > now - 3600 for t in s["trades"]) >= settings.RISK_MAX_TRADES_PER_HOUR:
        return f"hourly trade cap {settings.RISK_MAX_TRADES_PER_HOUR} reached"
    if sum(t > now - 86400 for t in s["trades"]) >= settings.RISK_MAX_TRADES_PER_DAY:
        return f"daily trade cap {settings.RISK_MAX_TRADES_PER_DAY} reached"
    edge = float(opportunity.profit_margin)
    if not float(settings.RISK_MIN_EDGE_PER_CONTRACT) <= edge <= float(settings.RISK_MAX_EDGE_PER_CONTRACT):
        return (f"edge {edge:.4f}/contract outside band "
                f"[{settings.RISK_MIN_EDGE_PER_CONTRACT}, {settings.RISK_MAX_EDGE_PER_CONTRACT}]")
    if balances is None:
        return "live balances unavailable"
    live = (float(balances[0]), float(balances[1]))
    if _start_balances is None:
        _start_balances = live
        s["start"] = list(live)
        _save(s)
    for name, bal, start in (("kalshi", live[0], _start_balances[0]), ("polymarket", live[1], _start_balances[1])):
        if bal < start - float(settings.RISK_BALANCE_FLOOR_DROP):
            return f"{name} balance ${bal:.2f} fell more than {settings.RISK_BALANCE_FLOOR_DROP} below start ${start:.2f}"
        if bal < float(settings.SHUTDOWN_BALANCE):
            return f"{name} balance ${bal:.2f} below SHUTDOWN_BALANCE {settings.SHUTDOWN_BALANCE}"
    return None


def record_sent(opportunity, size) -> None:
    s = _load()
    s["trades"] = [t for t in s["trades"] if t > time.time() - 86400] + [time.time()]
    s["open"].append(sum(_legs(opportunity, size)))
    _save(s)


def record_leg_failure() -> None:
    s = _load()
    s["failures"] += 1
    _save(s)
    if s["failures"] >= settings.RISK_MAX_CONSECUTIVE_LEG_FAILURES:
        halt(f"{s['failures']} consecutive leg failures")


def record_leg_success() -> None:
    s = _load()
    s["failures"] = 0
    _save(s)


def record_resolution(realized_pnl: float, mismatch: bool) -> None:
    """Oldest open trade is treated as the one resolved (FIFO).
    Nothing calls this yet: wire it to whatever detects market resolution."""
    s = _load()
    if s["open"]:
        s["open"].pop(0)
    s["pnl"] += float(realized_pnl)
    _save(s)
    if mismatch:
        halt("resolution mismatch: both legs won or both lost (matching bug)")
    elif s["pnl"] <= -float(settings.RISK_MAX_REALIZED_LOSS):
        halt(f"cumulative realized pnl ${s['pnl']:.2f} hit loss floor -{settings.RISK_MAX_REALIZED_LOSS}")


def halt(reason: str) -> None:
    stamp = time.strftime("%Y-%m-%d %H:%M:%S")
    logger.critical(f"RISK HALT: {reason}")
    try:
        _halt_path().write_text(f"{stamp} {reason}\n")
    except OSError as e:
        logger.critical(f"Could not write halt file: {e}")
    if _shutdown_event is not None:
        _shutdown_event.set()
    if settings.NTFY_TOPIC:
        try:
            requests.post(f"https://ntfy.sh/{settings.NTFY_TOPIC}", data=f"arb bot halted: {reason}".encode(), timeout=5)
        except Exception:
            pass
