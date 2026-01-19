"""
Single Active Consumer - Ordered processing with automatic failover.

Single Active Consumer ensures that only one consumer in a group 
processes messages at a time. This is crucial for:
- Maintaining message ordering across consumers
- Avoiding duplicate processing during failover
- Implementing active-passive consumer patterns

If the active consumer fails, another consumer in the group 
automatically takes over from the last committed offset.
"""

import asyncio
import logging
import sys
from typing import Callable
from collections.abc import Awaitable

from rstream import (
    Consumer,
    ConsumerOffsetSpecification,
    OffsetType,
    AMQPMessage,
    MessageContext,
    SuperStreamConsumer,
    amqp_decoder,
)

from .config import StreamConfig
from .models import MarketData

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s"
)
logger = logging.getLogger(__name__)

MarketDataHandler = Callable[[MarketData, int], Awaitable[None]]


class SingleActiveConsumer:
    """
    Demonstrates Single Active Consumer pattern for RabbitMQ Streams.
    
    Uses consumer name for offset tracking - if this consumer restarts,
    it will resume from the last stored offset.
    """
    
    def __init__(self, config: StreamConfig, consumer_name: str):
        self.config = config
        self.consumer_name = consumer_name
        self.consumer: Consumer | None = None
        self.message_count = 0
        self.last_offset = -1
        self.is_active = False
        self._running = False
    
    async def start(self, handler: MarketDataHandler) -> None:
        """Start the consumer with Single Active Consumer semantics."""
        self.consumer = Consumer(
            host=self.config.host,
            port=self.config.port,
            username=self.config.username,
            password=self.config.password,
            vhost=self.config.virtual_host,
        )
        await self.consumer.start()
        
        logger.info(f"Starting Single Active Consumer: {self.consumer_name}")
        self._running = True
        self.is_active = True
        
        async def on_message(msg: AMQPMessage, context: MessageContext) -> None:
            if not self.is_active:
                return
            
            try:
                data = MarketData.from_bytes(msg.body)
                self.message_count += 1
                self.last_offset = context.offset
                
                await handler(data, context.offset)
                
                # Store offset periodically for recovery
                if self.message_count % 1000 == 0:
                    await context.consumer.store_offset(
                        stream=self.config.stream_name,
                        offset=context.offset,
                        subscriber_name=self.consumer_name,
                    )
                
            except Exception as e:
                logger.error(f"Error processing message: {e}")
        
        # Subscribe with automatic offset tracking
        await self.consumer.subscribe(
            stream=self.config.stream_name,
            callback=on_message,
            offset_specification=ConsumerOffsetSpecification(OffsetType.FIRST, None),
            subscriber_name=self.consumer_name,  # Enables offset tracking
            decoder=amqp_decoder,  # Decode AMQP messages
        )
        
        logger.info(f"Consumer '{self.consumer_name}' started and active")
    
    async def stop(self) -> None:
        """Stop the consumer."""
        self._running = False
        self.is_active = False
        
        if self.consumer:
            # Store final offset before closing
            if self.last_offset >= 0:
                await self.consumer.store_offset(
                    stream=self.config.stream_name,
                    offset=self.last_offset,
                    subscriber_name=self.consumer_name,
                )
            await self.consumer.close()
        
        logger.info(
            f"SingleActiveConsumer '{self.consumer_name}' closed. "
            f"Messages processed: {self.message_count}"
        )


async def main():
    """Demo: Run a consumer that tracks its offset."""
    config = StreamConfig.defaults()
    
    # Get consumer ID from command line
    consumer_id = sys.argv[1] if len(sys.argv) > 1 else "consumer-1"
    
    consumer = SingleActiveConsumer(config, consumer_id)
    
    async def handle_message(data: MarketData, offset: int) -> None:
        logger.info(
            f"[{offset}] Processing {data.symbol} - {data.last_price} @ {data.timestamp}"
        )
    
    try:
        await consumer.start(handle_message)
        
        logger.info(f"Consumer {consumer_id} running. Press Ctrl+C to stop.")
        
        # Keep running until interrupted
        while True:
            await asyncio.sleep(1)
            
    except KeyboardInterrupt:
        logger.info("Interrupted, shutting down...")
    finally:
        await consumer.stop()


if __name__ == "__main__":
    asyncio.run(main())
