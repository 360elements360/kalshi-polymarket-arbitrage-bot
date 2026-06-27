import json
import logging
import os
from datetime import datetime, timedelta, timezone
from decimal import getcontext, Decimal, ROUND_CEILING
from typing import Any, Dict, List, Optional

from app.domain.events import MarketBookUpdated, ArbitrageOpportunityFound, ExecuteTrade, TradeAttemptCompleted
from app.domain.models.opportunity import ArbitrageOpportunity
from app.domain.primitives import Money, Platform, SIDES
from app.markets.manager import MarketManager
from app.markets.state import MarketState
from app.message_bus import MessageBus

# --- Module Setup ---

getcontext().prec = 6
logger = logging.getLogger(__name__)
PROFITABILITY_BUFFER = Decimal("0.01")
STALENESS_THRESHOLD = timedelta(seconds=5)

# Observation-only JSONL sink. Active when OBSERVATION_LOG_PATH env var is set.
# Writes one row per arbitrage price check with full field set + decision/reason.
# Strategy thresholds and decision logic are NOT affected by this flag.
_OBSERVATION_LOG_PATH = os.environ.get("OBSERVATION_LOG_PATH")


def _jsonable(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, Decimal):
        return str(v)
    return v


def _write_observation(row: Dict[str, Any]) -> None:
    if not _OBSERVATION_LOG_PATH:
        return
    try:
        with open(_OBSERVATION_LOG_PATH, "a", encoding="utf-8") as f:
            f.write(json.dumps({k: _jsonable(v) for k, v in row.items()}) + "\n")
    except Exception:
        logger.exception("Failed to write observation row")

# Dependencies are stored at the module level and injected once at startup.
_market_manager: MarketManager
_bus: MessageBus
_market_config_map: Dict[str, Dict[str, str]] = {}
_is_trade_in_progress: bool = False


def initialize_arbitrage_handlers(
    market_manager: MarketManager,
    bus: MessageBus,
    markets_config: List[Dict[str, str]],
):
    """Injects dependencies into the strategy handlers module."""
    global _market_manager, _bus, _market_config_map
    _market_manager = market_manager
    _bus = bus
    _market_config_map = {m["id"]: m for m in markets_config}
    logger.info("Arbitrage monitor handlers initialized.")


# --- Event and Command Handlers ---

async def handle_market_book_update(event: MarketBookUpdated):
    """
    This handler is the entry point for our strategy. It's triggered when a
    market's state changes, and it checks for an arbitrage opportunity.
    """
    global _is_trade_in_progress
    if _is_trade_in_progress:
        logger.debug("Skipping opportunity check: trade already in progress.")
        return

    logger.debug(f"Handling MarketBookUpdated for {event.market_id}")
    market_state = _market_manager.get_market_state(event.market_id)
    if not market_state:
        return

    opportunity = _check_for_buy_both_arb(market_state)

    if opportunity:
        logger.info(
            "Arbitrage opportunity detected, locking strategy until execution is complete.",
            extra={
                "opportunity_details": opportunity.model_dump(mode='json')
            }
        )
        _is_trade_in_progress = True
        await _bus.publish(ArbitrageOpportunityFound(opportunity=opportunity))
    else:
        logger.debug(
            "No arbitrage opportunity found on market update",
            extra={"market_id": event.market_id}
        )


async def handle_arbitrage_opportunity_found(event: ArbitrageOpportunityFound):
    """
    This handler consumes the event created by our own strategy. It's responsible
    for the decision to act on the opportunity by issuing a command.
    """
    logger.info(f"Handling ArbitrageOpportunityFound for {event.opportunity.market_id}. Issuing ExecuteTrade command.")
    await _bus.publish(ExecuteTrade(opportunity=event.opportunity))


async def handle_trade_attempt_completed(event: TradeAttemptCompleted):
    """Resets the trade-in-progress flag, re-enabling opportunity checks."""
    global _is_trade_in_progress
    _is_trade_in_progress = False
    logger.info("Trade attempt completed. Re-enabling arbitrage checks.")


# --- Strategy Logic ---

def _kalshi_fee(contracts: Money, price: Money, rate: Decimal = Decimal("0.07")) -> Money:
    """Calculates the Kalshi trading fee."""
    if price <= Decimal("0") or price >= Decimal("1"):
        return Money("0.00")
    raw_decimal = rate * contracts * price * (Decimal("1") - price)
    cents = raw_decimal * Decimal("100")
    rounded_cents = cents.to_integral_value(rounding=ROUND_CEILING)
    return rounded_cents / Decimal("100")


def _check_for_buy_both_arb(market_state: MarketState) -> Optional[ArbitrageOpportunity]:
    """
    Checks for a "buy both" opportunity using the MarketState domain model.
    If an opportunity is found, it returns an ArbitrageOpportunity object.
    """
    market_id = market_state.market_id
    market_config = _market_config_map.get(market_id)
    if not market_config:
        return None

    # --- Get prices by asking the domain model ---
    kalshi_yes_ask_price = market_state.get_price(Platform.KALSHI, "YES", SIDES.SELL)
    poly_no_ask_price = market_state.get_price(Platform.POLYMARKET, "NO", SIDES.SELL)
    poly_yes_ask_price = market_state.get_price(Platform.POLYMARKET, "YES", SIDES.SELL)
    kalshi_no_ask_price = market_state.get_kalshi_derived_no_ask_price()

    # --- Get available liquidity (size) ---
    kalshi_outcomes = market_state.get_outcomes_for_platform(Platform.KALSHI)
    poly_outcomes = market_state.get_outcomes_for_platform(Platform.POLYMARKET)

    kalshi_yes_tob = kalshi_outcomes.get_book("YES").get_top_of_book() if kalshi_outcomes and kalshi_outcomes.get_book("YES") else (None, None)
    poly_yes_tob = poly_outcomes.get_book("YES").get_top_of_book() if poly_outcomes and poly_outcomes.get_book("YES") else (None, None)
    poly_no_tob = poly_outcomes.get_book("NO").get_top_of_book() if poly_outcomes and poly_outcomes.get_book("NO") else (None, None)

    kalshi_yes_ask_size = kalshi_yes_tob[1][1] if kalshi_yes_tob[1] else Decimal("0")
    kalshi_yes_bid_size = kalshi_yes_tob[0][1] if kalshi_yes_tob[0] else Decimal("0")
    poly_yes_ask_size = poly_yes_tob[1][1] if poly_yes_tob[1] else Decimal("0")
    poly_no_ask_size = poly_no_tob[1][1] if poly_no_tob[1] else Decimal("0")
    # Kalshi has only a YES book — the implied "NO ask" size is the YES bid size.
    kalshi_no_ask_size = kalshi_yes_bid_size

    # +++ ADDED FOR DIAGNOSTICS +++
    logger.info(
        "--- Arbitrage Price Check ---",
        extra={
            "market_id": market_id,
            "kalshi_yes_ask_price": f"{kalshi_yes_ask_price!r}",
            "kalshi_yes_ask_size": f"{kalshi_yes_ask_size!r}",
            "poly_no_ask_price": f"{poly_no_ask_price!r}",
            "poly_no_ask_size": f"{poly_no_ask_size!r}",
        }
    )
    # +++ END DIAGNOSTICS +++

    # Track decision per opportunity for the structured observation log.
    # Strategy logic below is unchanged — these only carry "why".
    opp1_decision = "no_trade"
    opp1_reason: Optional[str] = None
    opp2_decision = "no_trade"
    opp2_reason: Optional[str] = None
    opp1_sum: Optional[Decimal] = None
    opp2_sum: Optional[Decimal] = None
    opp1_fee_buffer: Optional[Decimal] = None
    opp2_fee_buffer: Optional[Decimal] = None
    returned_opportunity: Optional[ArbitrageOpportunity] = None

    # --- Opportunity 1: Buy YES on Kalshi, Buy NO on Polymarket ---
    if kalshi_yes_ask_price is None or poly_no_ask_price is None:
        opp1_reason = "missing_price"
    else:
        opp1_sum = kalshi_yes_ask_price + poly_no_ask_price
        is_stale = False
        if kalshi_outcomes and poly_outcomes:
            kalshi_book = kalshi_outcomes.get_book("YES")
            poly_book = poly_outcomes.get_book("NO")
            if kalshi_book and poly_book and (abs(kalshi_book.last_update - poly_book.last_update) > STALENESS_THRESHOLD):
                logger.debug("Skipping opportunity 1 check for %s due to stale books.", market_id)
                is_stale = True

        if is_stale:
            opp1_reason = "stale_books"
        else:
            cost1 = opp1_sum
            trade_size1 = min(kalshi_yes_ask_size, poly_no_ask_size)
            if trade_size1 <= 0:
                opp1_reason = "zero_trade_size"
            else:
                fee_per_contract_1 = _kalshi_fee(trade_size1, kalshi_yes_ask_price) / trade_size1
                opp1_fee_buffer = fee_per_contract_1 + PROFITABILITY_BUFFER
                if (cost1 + fee_per_contract_1) < Decimal("1.0") - PROFITABILITY_BUFFER:
                    profit_margin = Decimal("1.0") - (cost1 + fee_per_contract_1)
                    returned_opportunity = ArbitrageOpportunity(
                        market_id=market_id, buy_yes_platform=Platform.KALSHI, buy_yes_price=kalshi_yes_ask_price,
                        buy_no_platform=Platform.POLYMARKET, buy_no_price=poly_no_ask_price, profit_margin=profit_margin,
                        potential_trade_size=trade_size1, kalshi_ticker=market_config["kalshi_ticker"],
                        polymarket_yes_token_id=market_config["polymarket_yes_token_id"], polymarket_no_token_id=market_config["polymarket_no_token_id"],
                        kalshi_fees=fee_per_contract_1
                    )
                    opp1_decision = "trade"
                    opp1_reason = "profitable"
                else:
                    opp1_reason = "sum_above_breakeven"

    # --- Opportunity 2: Buy YES on Polymarket, Buy NO on Kalshi ---
    # NOTE: original logic still runs even if opp1 returned, but only opp1's
    # return value is published. Keep evaluation parity for the observation log.
    if poly_yes_ask_price is None or kalshi_no_ask_price is None:
        opp2_reason = "missing_price"
    else:
        opp2_sum = poly_yes_ask_price + kalshi_no_ask_price
        is_stale = False
        if kalshi_outcomes and poly_outcomes:
            kalshi_book = kalshi_outcomes.get_book("YES")
            poly_book = poly_outcomes.get_book("YES")
            if kalshi_book and poly_book and (abs(kalshi_book.last_update - poly_book.last_update) > STALENESS_THRESHOLD):
                logger.debug("Skipping opportunity 2 check for %s due to stale books.", market_id)
                is_stale = True

        if is_stale:
            opp2_reason = "stale_books"
        else:
            cost2 = opp2_sum
            trade_size2 = min(poly_yes_ask_size, kalshi_yes_bid_size)
            if trade_size2 <= 0:
                opp2_reason = "zero_trade_size"
            else:
                fee_per_contract_2 = _kalshi_fee(trade_size2, kalshi_no_ask_price) / trade_size2
                opp2_fee_buffer = fee_per_contract_2 + PROFITABILITY_BUFFER
                if (cost2 + fee_per_contract_2) < Decimal("1.0") - PROFITABILITY_BUFFER:
                    profit_margin = Decimal("1.0") - (cost2 + fee_per_contract_2)
                    # If opp1 already produced an opportunity, the original code
                    # returned immediately and never evaluated opp2's trade. Mirror
                    # that: opp2 only fires if opp1 didn't.
                    if returned_opportunity is None:
                        returned_opportunity = ArbitrageOpportunity(
                            market_id=market_id, buy_yes_platform=Platform.POLYMARKET, buy_yes_price=poly_yes_ask_price,
                            buy_no_platform=Platform.KALSHI, buy_no_price=kalshi_no_ask_price, profit_margin=profit_margin,
                            potential_trade_size=trade_size2, kalshi_ticker=market_config["kalshi_ticker"],
                            polymarket_yes_token_id=market_config["polymarket_yes_token_id"], polymarket_no_token_id=market_config["polymarket_no_token_id"],
                            kalshi_fees=fee_per_contract_2
                        )
                    opp2_decision = "trade"
                    opp2_reason = "profitable"
                else:
                    opp2_reason = "sum_above_breakeven"

    # Aggregate decision: "trade" if either opp would fire, else "no_trade".
    overall_decision = "trade" if (opp1_decision == "trade" or opp2_decision == "trade") else "no_trade"

    # Leg-staleness observation (decision-time book ages). Observation-only —
    # does NOT change the strategy's STALENESS_THRESHOLD gate above. Reports
    # both opp1 pair (Kalshi YES vs Poly NO) and opp2 pair (Kalshi YES vs Poly YES).
    decision_ts = datetime.now(timezone.utc)
    kalshi_yes_book = kalshi_outcomes.get_book("YES") if kalshi_outcomes else None
    poly_no_book = poly_outcomes.get_book("NO") if poly_outcomes else None
    poly_yes_book = poly_outcomes.get_book("YES") if poly_outcomes else None

    def _age_ms(book) -> Optional[int]:
        if book is None:
            return None
        return int((decision_ts - book.last_update).total_seconds() * 1000)

    kalshi_yes_age_ms = _age_ms(kalshi_yes_book)
    poly_no_age_ms = _age_ms(poly_no_book)
    poly_yes_age_ms = _age_ms(poly_yes_book)

    def _skew(a, b) -> Optional[int]:
        return abs(a - b) if (a is not None and b is not None) else None

    opp1_leg_skew_ms = _skew(kalshi_yes_age_ms, poly_no_age_ms)
    opp2_leg_skew_ms = _skew(kalshi_yes_age_ms, poly_yes_age_ms)

    _write_observation({
        "ts": decision_ts.isoformat(),
        "market_pair": market_id,
        "kalshi_yes_ask_price": kalshi_yes_ask_price,
        "kalshi_yes_ask_size": kalshi_yes_ask_size,
        "kalshi_no_ask_price": kalshi_no_ask_price,
        "kalshi_no_ask_size": kalshi_no_ask_size,
        "poly_yes_ask_price": poly_yes_ask_price,
        "poly_yes_ask_size": poly_yes_ask_size,
        "poly_no_ask_price": poly_no_ask_price,
        "poly_no_ask_size": poly_no_ask_size,
        "opp1_sum_kalshi_yes_plus_poly_no": opp1_sum,
        "opp1_fee_buffer_applied": opp1_fee_buffer,
        "opp1_decision": opp1_decision,
        "opp1_reason": opp1_reason,
        "opp2_sum_poly_yes_plus_kalshi_no": opp2_sum,
        "opp2_fee_buffer_applied": opp2_fee_buffer,
        "opp2_decision": opp2_decision,
        "opp2_reason": opp2_reason,
        "decision": overall_decision,
        "profitability_buffer": PROFITABILITY_BUFFER,
        "breakeven_threshold": Decimal("1.0") - PROFITABILITY_BUFFER,
        # Leg-staleness (observation-only)
        "kalshi_age_ms": kalshi_yes_age_ms,
        "poly_no_age_ms": poly_no_age_ms,
        "poly_yes_age_ms": poly_yes_age_ms,
        "opp1_leg_skew_ms": opp1_leg_skew_ms,
        "opp2_leg_skew_ms": opp2_leg_skew_ms,
    })

    return returned_opportunity