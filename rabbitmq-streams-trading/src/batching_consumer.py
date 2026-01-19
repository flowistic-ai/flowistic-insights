"""
Batching Consumer - High-performance micro-batching for market data.

This consumer demonstrates:
- Micro-batching for improved throughput
- Per-symbol aggregation windows
- VWAP (Volume Weighted Average Price) calculation
- Backpressure handling
"""

import asyncio
import logging
from collections import defaultdict
from datetime import datetime, timezone
from typing import Callable, List, Dict
from collections.abc import Awaitable

from rstream import (
    Consumer,
    ConsumerOffsetSpecification,
    OffsetType,
    AMQPMessage,
    MessageContext,
    amqp_decoder,
)

from .config import StreamConfig
from .models import MarketData, SymbolStats

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger(__name__)

BatchHandler = Callable[[List[MarketData]], Awaitable[None]]


class BatchingConsumer:
    """High-performance batching consumer for market data."""
    
    def __init__(
        self,
        config: StreamConfig,
        batch_size: int = 1000,
        batch_timeout_ms: float = 100,
    ):
        self.config = config
        self.batch_size = batch_size
        self.batch_timeout_ms = batch_timeout_ms
        self.consumer: Consumer | None = None
        self.message_count = 0
        self.batch_count = 0
        
        self._buffer: List[MarketData] = []
        self._buffer_lock = asyncio.Lock()
        self._running = False
        self._flush_task: asyncio.Task | None = None
    
    async def start(self, handler: BatchHandler) -> None:
        """Start consuming with batching."""
        self.consumer = Consumer(
            host=self.config.host,
            port=self.config.port,
            username=self.config.username,
            password=self.config.password,
            vhost=self.config.virtual_host,
        )
        await self.consumer.start()
        
        self._running = True
        self._handler = handler
        
        # Start the periodic flush task
        self._flush_task = asyncio.create_task(self._periodic_flush())
        
        async def on_message(msg: AMQPMessage, context: MessageContext) -> None:
            try:
                data = MarketData.from_bytes(msg.body)
                self.message_count += 1
                
                async with self._buffer_lock:
                    self._buffer.append(data)
                    
                    # Flush if batch size reached
                    if len(self._buffer) >= self.batch_size:
                        await self._flush_batch()
                        
            except Exception as e:
                logger.error(f"Error buffering message: {e}")
        
        await self.consumer.subscribe(
            stream=self.config.stream_name,
            callback=on_message,
            offset_specification=ConsumerOffsetSpecification(OffsetType.FIRST, None),
            decoder=amqp_decoder,  # Decode AMQP messages
        )
        
        logger.info(
            f"BatchingConsumer started with batch_size={self.batch_size}, "
            f"timeout={self.batch_timeout_ms}ms"
        )
    
    async def _periodic_flush(self) -> None:
        """Periodically flush the buffer."""
        while self._running:
            await asyncio.sleep(self.batch_timeout_ms / 1000)
            async with self._buffer_lock:
                if self._buffer:
                    await self._flush_batch()
    
    async def _flush_batch(self) -> None:
        """Flush the current buffer as a batch."""
        if not self._buffer:
            return
        
        batch = self._buffer[:self.batch_size]
        self._buffer = self._buffer[self.batch_size:]
        
        self.batch_count += 1
        
        try:
            await self._handler(batch)
        except Exception as e:
            logger.error(f"Error processing batch: {e}")
    
    async def stop(self) -> None:
        """Stop the consumer."""
        self._running = False
        
        if self._flush_task:
            self._flush_task.cancel()
            try:
                await self._flush_task
            except asyncio.CancelledError:
                pass
        
        # Flush remaining messages
        async with self._buffer_lock:
            if self._buffer:
                await self._flush_batch()
        
        if self.consumer:
            await self.consumer.close()
        
        logger.info(
            f"BatchingConsumer closed. Messages: {self.message_count}, "
            f"Batches: {self.batch_count}"
        )
    
    @staticmethod
    def aggregate_by_symbol(batch: List[MarketData]) -> Dict[str, SymbolStats]:
        """Get aggregated statistics by symbol."""
        stats: Dict[str, SymbolStats] = {}
        
        for data in batch:
            if data.symbol not in stats:
                stats[data.symbol] = SymbolStats(data.symbol)
            stats[data.symbol].update(data)
        
        return stats


async def main():
    """Run the batching consumer."""
    config = StreamConfig.defaults()
    
    consumer = BatchingConsumer(
        config,
        batch_size=1000,
        batch_timeout_ms=100,
    )
    
    async def handle_batch(batch: List[MarketData]) -> None:
        # Aggregate statistics by symbol
        stats = BatchingConsumer.aggregate_by_symbol(batch)
        
        logger.info(f"Batch processed: {len(batch)} ticks across {len(stats)} symbols")
        
        for symbol_stats in stats.values():
            logger.info(f"  {symbol_stats}")
    
    try:
        await consumer.start(handle_batch)
        
        # Run for 60 seconds
        await asyncio.sleep(60)
        
        logger.info(
            f"Total messages: {consumer.message_count}, "
            f"batches: {consumer.batch_count}"
        )
    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())
