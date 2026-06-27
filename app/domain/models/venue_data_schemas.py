from decimal import Decimal
from typing import Tuple, List, Optional

from pydantic import BaseModel, Field

# --- Kalshi Schemas (v2 wire format, dollar-denominated strings) ---
# Snapshots:  msg.yes_dollars_fp / msg.no_dollars_fp = list[[price_str, size_str]]
# Deltas:     msg.price_dollars (str)  msg.delta_fp (signed str)  msg.side
# All prices are dollars already; do NOT divide by 100 anywhere.
KalshiPriceLevel = Tuple[Decimal, Decimal]

class KalshiSnapshotData(BaseModel):
    market_ticker: str
    market_id: Optional[str] = None
    # Either side may be omitted when its half of the book is empty. Default is
    # explicitly None (not []) so an absent key is distinguishable from "empty
    # list". The WSS handler must fail loud if BOTH are absent — that signals a
    # wire-format change, not a quiet market.
    yes_dollars_fp: Optional[List[KalshiPriceLevel]] = None
    no_dollars_fp: Optional[List[KalshiPriceLevel]] = None
    class Config:
        frozen = True

class KalshiSnapshotMessage(BaseModel):
    type: str = Field(..., pattern="orderbook_snapshot")
    seq: int
    sid: Optional[int] = None
    msg: KalshiSnapshotData
    class Config:
        frozen = True

class KalshiDeltaData(BaseModel):
    market_ticker: str
    market_id: Optional[str] = None
    # price_dollars is a dollar-denominated string (e.g. "0.1400"). Decimal
    # accepts the string verbatim — no /100 anywhere downstream.
    price_dollars: Decimal
    # delta_fp is a SIGNED price-level diff (e.g. "-40.16", "24.01") applied to
    # the resting size at (price_dollars, side). Positive = liquidity added,
    # negative = pulled or filled.
    delta_fp: Decimal
    side: str  # 'yes' or 'no'
    ts: Optional[str] = None
    ts_ms: Optional[int] = None

class KalshiDeltaMessage(BaseModel):
    type: str = Field(..., pattern="orderbook_delta")
    seq: int
    sid: Optional[int] = None
    msg: KalshiDeltaData

# --- Polymarket Schemas ---
class PolyPriceLevel(BaseModel):
    price: str
    size: str

class PolyBookMessage(BaseModel):
    event_type: str = Field(..., pattern="book")
    market: str
    bids: List[PolyPriceLevel]
    asks: List[PolyPriceLevel]

class PolyChange(BaseModel):
    # Each entry in a price_change frame carries its OWN asset_id (token_id,
    # decimal). This is what tells us whether the entry updates the YES leg or
    # the NO leg of the market. The top-level frame only identifies the
    # market (by condition_id), not the leg. Required field — without it we
    # cannot route deltas safely.
    asset_id: str
    price: str
    side: str  # 'BUY' or 'SELL'
    size: str

class PolyPriceChangeMessage(BaseModel):
    event_type: str = Field(..., pattern="price_change")
    # `market` is the Polymarket condition_id (hex, 0x...). NOT a token_id.
    market: str
    # WIRE field is `price_changes`, not `changes`. The pre-2026-06-23
    # schema used `changes` and was completely non-functional in production
    # because the asset_id filter dropped every price_change frame before
    # validation ever ran. See HANDOFF / dryrun-v2 summary for the audit.
    price_changes: List[PolyChange]