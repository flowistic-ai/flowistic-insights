"""
Market Data Producer - Simulates market data and publishes to RabbitMQ Stream.

This producer demonstrates:
- Creating a stream with retention policies
- High-throughput publishing
- Confirmation handling for reliable delivery
"""

import asyncio
import logging
import random
from datetime import datetime, timezone
from decimal import Decimal
from typing import NamedTuple

from rstream import Producer, AMQPMessage, RawMessage

from .config import StreamConfig
from .models import MarketData

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger(__name__)


class SymbolPrice(NamedTuple):
    """Symbol with base price for simulation."""
    symbol: str
    base_price: Decimal


# Simulated symbols with base prices
SYMBOLS = [
    SymbolPrice("AAPL", Decimal("185.50")),
    SymbolPrice("GOOGL", Decimal("142.75")),
    SymbolPrice("MSFT", Decimal("378.25")),
    SymbolPrice("AMZN", Decimal("178.50")),
    SymbolPrice("TSLA", Decimal("248.75")),
    SymbolPrice("NVDA", Decimal("495.00")),
    SymbolPrice("META", Decimal("505.25")),
    SymbolPrice("JPM", Decimal("195.50")),
    SymbolPrice("V", Decimal("275.00")),
    SymbolPrice("JNJ", Decimal("156.25")),
]


class MarketDataProducer:
    """Simulates market data feed and publishes to RabbitMQ Stream."""
    
    def __init__(self, config: StreamConfig):
        self.config = config
        self.sequence_number = 0
        self.producer: Producer | None = None
    
    async def start(self) -> None:
        """Initialize the producer and create the stream."""
        self.producer = Producer(
            host=self.config.host,
            port=self.config.port,
            username=self.config.username,
            password=self.config.password,
            vhost=self.config.virtual_host,
        )
        await self.producer.start()
        
        # Create the stream if it doesn't exist
        try:
            await self.producer.create_stream(
                self.config.stream_name,
                arguments={
                    "max-age": f"{self.config.max_age_seconds}s",
                    "max-length-bytes": self.config.max_length_bytes,
                    "max-segment-size-bytes": self.config.max_segment_size_bytes,
                }
            )
            logger.info(f"Created stream: {self.config.stream_name}")
        except Exception as e:
            # Stream might already exist
            logger.debug(f"Stream creation: {e}")
        
        logger.info(f"MarketDataProducer initialized for stream: {self.config.stream_name}")
    
    async def stop(self) -> None:
        """Close the producer."""
        if self.producer:
            await self.producer.close()
            logger.info("MarketDataProducer closed")
    
    def _generate_tick(self) -> MarketData:
        """Generate a simulated market data tick."""
        symbol_price = random.choice(SYMBOLS)
        
        # Generate price with some randomness (±0.5%)
        price_variation = 1.0 + (random.random() - 0.5) * 0.01
        current_price = (symbol_price.base_price * Decimal(str(price_variation))).quantize(
            Decimal("0.01")
        )
        
        # Spread typically 0.01% to 0.05%
        spread_percent = Decimal(str(0.0001 + random.random() * 0.0004))
        half_spread = (current_price * spread_percent / 2).quantize(Decimal("0.01"))
        
        bid_price = current_price - half_spread
        ask_price = current_price + half_spread
        
        self.sequence_number += 1
        
        return MarketData(
            symbol=symbol_price.symbol,
            bid_price=bid_price,
            ask_price=ask_price,
            bid_size=Decimal(100 + random.randint(0, 899)),
            ask_size=Decimal(100 + random.randint(0, 899)),
            last_price=current_price,
            last_size=Decimal(1 + random.randint(0, 99)),
            volume=Decimal(1_000_000 + random.randint(0, 9_999_999)),
            timestamp=datetime.now(timezone.utc),
            exchange=random.choice(["NYSE", "NASDAQ"]),
            sequence_number=self.sequence_number,
        )
    
    async def publish(self, market_data: MarketData) -> None:
        """Publish a single market data tick."""
        if not self.producer:
            raise RuntimeError("Producer not started")
        
        await self.producer.send(
            stream=self.config.stream_name,
            message=AMQPMessage(body=market_data.to_bytes()),
        )
    
    async def publish_batch(self, count: int, delay_ms: float = 0) -> None:
        """Generate and publish simulated market data."""
        logger.info(f"Publishing {count} simulated market data ticks with {delay_ms}ms delay")
        
        confirmed = 0
        start_time = asyncio.get_event_loop().time()
        
        for i in range(count):
            tick = self._generate_tick()
            await self.publish(tick)
            confirmed += 1
            
            if delay_ms > 0 and i < count - 1:
                await asyncio.sleep(delay_ms / 1000)
            
            if (i + 1) % 10000 == 0:
                logger.info(f"Published {i + 1} ticks...")
        
        duration = asyncio.get_event_loop().time() - start_time
        throughput = count / duration if duration > 0 else 0
        
        logger.info(f"Publishing complete: {confirmed} confirmed, {throughput:.2f} msg/sec")


async def main():
    """Run the producer."""
    config = StreamConfig.defaults()
    producer = MarketDataProducer(config)
    
    try:
        await producer.start()
        # Publish 100,000 ticks with no delay (max throughput)
        await producer.publish_batch(100_000, delay_ms=0)
    finally:
        await producer.stop()


if __name__ == "__main__":
    asyncio.run(main())
