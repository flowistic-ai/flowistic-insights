"""
Replay Consumer - Historical market data replay.

RabbitMQ Streams excel at replay scenarios because:
- Messages are persisted on disk with configurable retention
- You can start consuming from any offset or timestamp
- Multiple consumers can read the same stream without affecting each other
- Reading is non-destructive (unlike regular queues)

Use cases:
- Backtesting trading strategies
- Reconstructing order book state
- Debugging production issues
- Training ML models on historical data
"""

import asyncio
import logging
from collections import defaultdict
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Set, Optional

from rstream import (
    Consumer,
    ConsumerOffsetSpecification,
    OffsetType,
    AMQPMessage,
    MessageContext,
    amqp_decoder,
)

from .config import StreamConfig
from .models import MarketData

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger(__name__)


class ReplayResult:
    """Result of a replay operation."""
    
    def __init__(self):
        self.data_by_symbol: Dict[str, List[MarketData]] = defaultdict(list)
        self.total_processed = 0
        self.matched_count = 0
    
    @property
    def symbol_count(self) -> int:
        return len(self.data_by_symbol)
    
    def get_data_for_symbol(self, symbol: str) -> List[MarketData]:
        return self.data_by_symbol.get(symbol, [])
    
    def print_summary(self) -> None:
        logger.info("=== Replay Summary ===")
        logger.info(f"Total messages: {self.total_processed}")
        logger.info(f"Matched messages: {self.matched_count}")
        logger.info(f"Symbols: {list(self.data_by_symbol.keys())}")
        for symbol, data in self.data_by_symbol.items():
            logger.info(f"  {symbol}: {len(data)} ticks")


class StreamStats:
    """Statistics about the stream."""
    
    def __init__(self, stream_name: str):
        self.stream_name = stream_name
        self.first_offset = -1
        self.last_offset = -1
        self.total_messages = 0
        self.ticks_by_symbol: Dict[str, int] = defaultdict(int)
        self.ticks_by_exchange: Dict[str, int] = defaultdict(int)
        self.first_timestamp: Optional[datetime] = None
        self.last_timestamp: Optional[datetime] = None
    
    def print(self) -> None:
        logger.info(f"=== Stream Statistics: {self.stream_name} ===")
        logger.info(f"Offset range: {self.first_offset} - {self.last_offset}")
        logger.info(f"Total messages: {self.total_messages}")
        
        if self.first_timestamp and self.last_timestamp:
            span = self.last_timestamp - self.first_timestamp
            logger.info(
                f"Time range: {self.first_timestamp} to {self.last_timestamp} ({span})"
            )
        
        logger.info("Ticks by symbol:")
        for symbol, count in sorted(
            self.ticks_by_symbol.items(), key=lambda x: x[1], reverse=True
        ):
            logger.info(f"  {symbol}: {count}")
        
        logger.info("Ticks by exchange:")
        for exchange, count in self.ticks_by_exchange.items():
            logger.info(f"  {exchange}: {count}")


class ReplayConsumer:
    """Consumer for replaying historical market data."""
    
    def __init__(self, config: StreamConfig):
        self.config = config
        self.consumer: Consumer | None = None
    
    async def start(self) -> None:
        """Initialize the consumer."""
        self.consumer = Consumer(
            host=self.config.host,
            port=self.config.port,
            username=self.config.username,
            password=self.config.password,
            vhost=self.config.virtual_host,
        )
        await self.consumer.start()
        logger.info(f"ReplayConsumer initialized for stream: {self.config.stream_name}")
    
    async def stop(self) -> None:
        """Close the consumer."""
        if self.consumer:
            await self.consumer.close()
        logger.info("ReplayConsumer closed")
    
    async def replay_all(self, timeout_seconds: float = 30) -> ReplayResult:
        """Replay all data from the stream and collect results."""
        return await self._replay(
            ConsumerOffsetSpecification(OffsetType.FIRST, None),
            timeout_seconds=timeout_seconds,
        )
    
    async def replay_from_time(
        self, from_time: datetime, timeout_seconds: float = 30
    ) -> ReplayResult:
        """Replay data from a specific timestamp."""
        return await self._replay(
            ConsumerOffsetSpecification(OffsetType.TIMESTAMP, int(from_time.timestamp())),
            timeout_seconds=timeout_seconds,
        )
    
    async def replay_time_range(
        self,
        from_time: datetime,
        to_time: datetime,
        timeout_seconds: float = 30,
    ) -> ReplayResult:
        """Replay data within a time range."""
        return await self._replay(
            ConsumerOffsetSpecification(OffsetType.TIMESTAMP, int(from_time.timestamp())),
            end_time=to_time,
            timeout_seconds=timeout_seconds,
        )
    
    async def replay_symbols(
        self,
        symbols: Set[str],
        timeout_seconds: float = 30,
    ) -> ReplayResult:
        """Replay data for specific symbols."""
        logger.info(f"Replaying data for symbols: {symbols}")
        return await self._replay(
            ConsumerOffsetSpecification(OffsetType.FIRST, None),
            symbol_filter=symbols,
            timeout_seconds=timeout_seconds,
        )
    
    async def _replay(
        self,
        offset_spec: ConsumerOffsetSpecification,
        end_time: Optional[datetime] = None,
        symbol_filter: Optional[Set[str]] = None,
        timeout_seconds: float = 30,
    ) -> ReplayResult:
        """Internal replay implementation."""
        if not self.consumer:
            raise RuntimeError("Consumer not started")
        
        result = ReplayResult()
        done_event = asyncio.Event()
        
        async def on_message(msg: AMQPMessage, context: MessageContext) -> None:
            try:
                data = MarketData.from_bytes(msg.body)
                
                # Check if we've passed the end time
                if end_time and data.timestamp > end_time:
                    done_event.set()
                    return
                
                result.total_processed += 1
                
                # Apply symbol filter
                if symbol_filter and data.symbol not in symbol_filter:
                    return
                
                result.matched_count += 1
                result.data_by_symbol[data.symbol].append(data)
                
            except Exception as e:
                logger.error(f"Error processing message: {e}")
        
        await self.consumer.subscribe(
            stream=self.config.stream_name,
            callback=on_message,
            offset_specification=offset_spec,
            decoder=amqp_decoder,  # Decode AMQP messages
        )
        
        # Wait for timeout or completion
        try:
            await asyncio.wait_for(done_event.wait(), timeout=timeout_seconds)
        except asyncio.TimeoutError:
            pass
        
        logger.info(f"Replay complete: {result.total_processed} total messages")
        
        return result
    
    async def get_stream_stats(self, sample_seconds: float = 10) -> StreamStats:
        """Get stream statistics without fully consuming."""
        logger.info(f"Collecting stream statistics for {sample_seconds}s...")
        
        if not self.consumer:
            raise RuntimeError("Consumer not started")
        
        stats = StreamStats(self.config.stream_name)
        timestamps: List[datetime] = []
        
        async def on_message(msg: AMQPMessage, context: MessageContext) -> None:
            try:
                data = MarketData.from_bytes(msg.body)
                
                if stats.first_offset == -1:
                    stats.first_offset = context.offset
                stats.last_offset = context.offset
                stats.total_messages += 1
                
                stats.ticks_by_symbol[data.symbol] += 1
                stats.ticks_by_exchange[data.exchange] += 1
                
                if len(timestamps) < 10000:
                    timestamps.append(data.timestamp)
                    
            except Exception as e:
                logger.error(f"Error processing message: {e}")
        
        await self.consumer.subscribe(
            stream=self.config.stream_name,
            callback=on_message,
            offset_specification=ConsumerOffsetSpecification(OffsetType.FIRST, None),
            decoder=amqp_decoder,  # Decode AMQP messages
        )
        
        await asyncio.sleep(sample_seconds)
        
        if timestamps:
            stats.first_timestamp = timestamps[0]
            stats.last_timestamp = timestamps[-1]
        
        return stats


async def main():
    """Run the replay consumer."""
    config = StreamConfig.defaults()
    
    consumer = ReplayConsumer(config)
    
    try:
        await consumer.start()
        
        # Get stream statistics
        stats = await consumer.get_stream_stats(sample_seconds=10)
        stats.print()
        
        # Need to recreate consumer after stats collection
        await consumer.stop()
        await consumer.start()
        
        # Replay data for specific symbols
        result = await consumer.replay_symbols(
            {"AAPL", "GOOGL", "MSFT"},
            timeout_seconds=30,
        )
        result.print_summary()
        
        # Access individual symbol data
        aapl_data = result.get_data_for_symbol("AAPL")
        if aapl_data:
            logger.info(f"First AAPL tick: {aapl_data[0]}")
            logger.info(f"Last AAPL tick: {aapl_data[-1]}")
            
    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())
