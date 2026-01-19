"""Market data models."""

from datetime import datetime
from decimal import Decimal
from pydantic import BaseModel, Field
import orjson


class MarketData(BaseModel):
    """Market data tick representing a price update for a financial instrument."""
    
    symbol: str
    bid_price: Decimal
    ask_price: Decimal
    bid_size: Decimal
    ask_size: Decimal
    last_price: Decimal
    last_size: Decimal
    volume: Decimal
    timestamp: datetime
    exchange: str
    sequence_number: int
    
    model_config = {"json_encoders": {Decimal: str}}
    
    @property
    def spread(self) -> Decimal:
        """Calculate bid-ask spread."""
        return self.ask_price - self.bid_price
    
    @property
    def mid_price(self) -> Decimal:
        """Calculate mid price."""
        return (self.bid_price + self.ask_price) / 2
    
    def to_bytes(self) -> bytes:
        """Serialize to JSON bytes."""
        return orjson.dumps(self.model_dump(), default=str)
    
    @classmethod
    def from_bytes(cls, data: bytes) -> "MarketData":
        """Deserialize from JSON bytes."""
        return cls.model_validate(orjson.loads(data))


class SymbolStats:
    """Statistics aggregation per symbol."""
    
    def __init__(self, symbol: str):
        self.symbol = symbol
        self.tick_count = 0
        self.sum_price = Decimal("0")
        self.sum_volume = Decimal("0")
        self.sum_price_volume = Decimal("0")
        self.high = Decimal("0")
        self.low = Decimal("999999999")
        self.last_price: Decimal | None = None
        self.first_tick: datetime | None = None
        self.last_tick: datetime | None = None
    
    def update(self, data: MarketData) -> None:
        """Update statistics with new market data."""
        self.tick_count += 1
        price = data.last_price
        size = data.last_size
        
        self.sum_price += price
        self.sum_volume += size
        self.sum_price_volume += price * size
        
        if price > self.high:
            self.high = price
        if price < self.low:
            self.low = price
        
        self.last_price = price
        if self.first_tick is None:
            self.first_tick = data.timestamp
        self.last_tick = data.timestamp
    
    @property
    def vwap(self) -> Decimal:
        """Calculate Volume Weighted Average Price."""
        if self.sum_volume == 0:
            return Decimal("0")
        return (self.sum_price_volume / self.sum_volume).quantize(Decimal("0.0001"))
    
    @property
    def avg_price(self) -> Decimal:
        """Calculate average price."""
        if self.tick_count == 0:
            return Decimal("0")
        return (self.sum_price / self.tick_count).quantize(Decimal("0.0001"))
    
    def __repr__(self) -> str:
        return (
            f"SymbolStats(symbol={self.symbol!r}, ticks={self.tick_count}, "
            f"vwap={self.vwap}, avg={self.avg_price}, high={self.high}, "
            f"low={self.low}, last={self.last_price})"
        )
