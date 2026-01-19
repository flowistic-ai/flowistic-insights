"""
Market Data Consumer - Consumes market data from RabbitMQ Stream.

This consumer demonstrates:
- Multiple offset strategies (first, last, next, timestamp, offset)
- Message filtering by symbol or other criteria
- Offset tracking for replay capabilities
- High-throughput consumption
"""

import asyncio
import logging
from datetime import datetime, timezone
from typing import Callable, Set
from collections.abc import Awaitable

from rstream import (
    Consumer,
    ConsumerOffsetSpecification,
    OffsetType,
    AMQPMessage,
    MessageContext,
)

from .config import StreamConfig
from .models import MarketData

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Type alias for message handlers
MarketDataHandler = Callable[[MarketData, int], Awaitable[None]]


class MarketDataConsumer:
    """Consumes market data from RabbitMQ Stream."""
    
    def __init__(self, config: StreamConfig):
        self.config = config
        self.consumer: Consumer | None = None
        self.message_count = 0
        self.last_offset = -1
        self._running = False
    
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
        logger.info(f"MarketDataConsumer initialized for stream: {self.config.stream_name}")
    
    async def stop(self) -> None:
        """Close the consumer."""
        self._running = False
        if self.consumer:
            await self.consumer.close()
            logger.info(f"MarketDataConsumer closed. Total messages: {self.message_count}")
    
    async def consume_from_first(self, handler: MarketDataHandler) -> None:
        """Start consuming from the first message in the stream."""
        await self._consume(
            ConsumerOffsetSpecification(OffsetType.FIRST, None),
            handler,
        )
    
    async def consume_from_last(self, handler: MarketDataHandler) -> None:
        """Start consuming from the last message in the stream."""
        await self._consume(
            ConsumerOffsetSpecification(OffsetType.LAST, None),
            handler,
        )
    
    async def consume_from_next(self, handler: MarketDataHandler) -> None:
        """Start consuming from the next message (new messages only)."""
        await self._consume(
            ConsumerOffsetSpecification(OffsetType.NEXT, None),
            handler,
        )
    
    async def consume_from_offset(self, offset: int, handler: MarketDataHandler) -> None:
        """Start consuming from a specific offset."""
        await self._consume(
            ConsumerOffsetSpecification(OffsetType.OFFSET, offset),
            handler,
        )
    
    async def consume_from_timestamp(self, timestamp: datetime, handler: MarketDataHandler) -> None:
        """Start consuming from a specific timestamp."""
        await self._consume(
            ConsumerOffsetSpecification(OffsetType.TIMESTAMP, int(timestamp.timestamp())),
            handler,
        )
    
    async def consume_symbols(
        self,
        offset_spec: ConsumerOffsetSpecification,
        handler: MarketDataHandler,
        symbols: Set[str],
    ) -> None:
        """Consume messages for specific symbols only."""
        async def filtered_handler(data: MarketData, offset: int) -> None:
            if data.symbol in symbols:
                await handler(data, offset)
        
        await self._consume(offset_spec, filtered_handler)
    
    async def _consume(
        self,
        offset_spec: ConsumerOffsetSpecification,
        handler: MarketDataHandler,
        filter_fn: Callable[[MarketData], bool] | None = None,
    ) -> None:
        """Internal consume implementation."""
        if not self.consumer:
            raise RuntimeError("Consumer not started")
        
        self._running = True
        logger.info(f"Starting consumer with offset specification: {offset_spec}")
        
        async def on_message(msg: AMQPMessage, context: MessageContext) -> None:
            try:
                data = MarketData.from_bytes(msg.body)
                
                # Apply filter if provided
                if filter_fn and not filter_fn(data):
                    return
                
                self.message_count += 1
                self.last_offset = context.offset
                
                await handler(data, context.offset)
                
            except Exception as e:
                logger.error(f"Error processing message at offset {context.offset}: {e}")
        
        await self.consumer.subscribe(
            stream=self.config.stream_name,
            callback=on_message,
            offset_specification=offset_spec,
        )
        
        logger.info("Consumer started successfully")


async def main():
    """Run the consumer."""
    config = StreamConfig.defaults()
    consumer = MarketDataConsumer(config)
    
    async def handle_message(data: MarketData, offset: int) -> None:
        logger.info(
            f"[{offset}] {data.symbol} @ {data.exchange} "
            f"bid={data.bid_price} ask={data.ask_price} spread={data.spread}"
        )
    
    try:
        await consumer.start()
        
        # Consume all messages from the beginning
        await consumer.consume_from_first(handle_message)
        
        # Run for 30 seconds then exit
        await asyncio.sleep(30)
        
        logger.info(
            f"Consumed {consumer.message_count} messages, "
            f"last offset: {consumer.last_offset}"
        )
    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())
