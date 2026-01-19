package com.flowistic.trading.consumer;

import com.flowistic.trading.config.StreamConfig;
import com.flowistic.trading.model.MarketData;
import com.flowistic.trading.serialization.MarketDataCodec;
import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.MessageHandler;
import com.rabbitmq.stream.OffsetSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

/**
 * Consumes market data from RabbitMQ Stream.
 * 
 * This consumer demonstrates:
 * - Multiple offset strategies (first, last, next, timestamp, offset)
 * - Message filtering by symbol or other criteria
 * - Offset tracking for replay capabilities
 * - High-throughput consumption
 */
public class MarketDataConsumer implements AutoCloseable {
    
    private static final Logger logger = LoggerFactory.getLogger(MarketDataConsumer.class);
    
    private final StreamConfig config;
    private final MarketDataCodec codec;
    private final Environment environment;
    private Consumer consumer;
    private final AtomicLong messageCount;
    private final AtomicLong lastOffset;
    
    public MarketDataConsumer(StreamConfig config) {
        this.config = config;
        this.codec = new MarketDataCodec();
        this.messageCount = new AtomicLong(0);
        this.lastOffset = new AtomicLong(-1);
        
        this.environment = Environment.builder()
            .host(config.host())
            .port(config.port())
            .username(config.username())
            .password(config.password())
            .virtualHost(config.virtualHost())
            .build();
        
        logger.info("MarketDataConsumer initialized for stream: {}", config.streamName());
    }
    
    /**
     * Start consuming from the first message in the stream.
     * Useful for replaying all historical data.
     */
    public void consumeFromFirst(MarketDataHandler handler) {
        consume(OffsetSpecification.first(), handler, null);
    }
    
    /**
     * Start consuming from the last message in the stream.
     * Useful for getting only the most recent data.
     */
    public void consumeFromLast(MarketDataHandler handler) {
        consume(OffsetSpecification.last(), handler, null);
    }
    
    /**
     * Start consuming from the next message (new messages only).
     * Useful for real-time streaming without historical replay.
     */
    public void consumeFromNext(MarketDataHandler handler) {
        consume(OffsetSpecification.next(), handler, null);
    }
    
    /**
     * Start consuming from a specific offset.
     * Useful for resuming from a known position.
     */
    public void consumeFromOffset(long offset, MarketDataHandler handler) {
        consume(OffsetSpecification.offset(offset), handler, null);
    }
    
    /**
     * Start consuming from a specific timestamp.
     * Useful for replaying data from a point in time.
     */
    public void consumeFromTimestamp(Instant timestamp, MarketDataHandler handler) {
        consume(OffsetSpecification.timestamp(timestamp.toEpochMilli()), handler, null);
    }
    
    /**
     * Consume with a filter predicate.
     * Note: Filtering happens client-side; for server-side filtering, use Super Streams.
     */
    public void consumeWithFilter(
            OffsetSpecification offsetSpec, 
            MarketDataHandler handler,
            Predicate<MarketData> filter) {
        consume(offsetSpec, handler, filter);
    }
    
    /**
     * Consume messages for specific symbols only.
     */
    public void consumeSymbols(
            OffsetSpecification offsetSpec,
            MarketDataHandler handler,
            String... symbols) {
        
        var symbolSet = java.util.Set.of(symbols);
        consume(offsetSpec, handler, data -> symbolSet.contains(data.symbol()));
    }
    
    private void consume(
            OffsetSpecification offsetSpec, 
            MarketDataHandler handler,
            Predicate<MarketData> filter) {
        
        logger.info("Starting consumer with offset specification: {}", offsetSpec);
        
        this.consumer = environment.consumerBuilder()
            .stream(config.streamName())
            .offset(offsetSpec)
            .messageHandler((context, message) -> {
                try {
                    MarketData marketData = codec.decode(message);
                    
                    // Apply filter if provided
                    if (filter != null && !filter.test(marketData)) {
                        return;
                    }
                    
                    messageCount.incrementAndGet();
                    lastOffset.set(context.offset());
                    
                    // Call the user's handler
                    handler.handle(marketData, context.offset());
                    
                } catch (Exception e) {
                    logger.error("Error processing message at offset {}: {}", 
                        context.offset(), e.getMessage());
                }
            })
            .build();
        
        logger.info("Consumer started successfully");
    }
    
    /**
     * Get the current message count.
     */
    public long getMessageCount() {
        return messageCount.get();
    }
    
    /**
     * Get the last processed offset.
     */
    public long getLastOffset() {
        return lastOffset.get();
    }
    
    @Override
    public void close() {
        if (consumer != null) {
            consumer.close();
        }
        if (environment != null) {
            environment.close();
        }
        logger.info("MarketDataConsumer closed. Total messages processed: {}", messageCount.get());
    }
    
    /**
     * Functional interface for handling market data.
     */
    @FunctionalInterface
    public interface MarketDataHandler {
        void handle(MarketData marketData, long offset);
    }
    
    public static void main(String[] args) throws Exception {
        StreamConfig config = StreamConfig.defaults();
        CountDownLatch latch = new CountDownLatch(1);
        
        try (MarketDataConsumer consumer = new MarketDataConsumer(config)) {
            
            // Example 1: Consume all messages from the beginning
            consumer.consumeFromFirst((data, offset) -> {
                logger.info("[{}] {} @ {} bid={} ask={} spread={}", 
                    offset,
                    data.symbol(),
                    data.exchange(),
                    data.bidPrice(),
                    data.askPrice(),
                    data.spread());
            });
            
            // Run for 30 seconds then exit
            Thread.sleep(Duration.ofSeconds(30).toMillis());
            
            logger.info("Consumed {} messages, last offset: {}", 
                consumer.getMessageCount(), consumer.getLastOffset());
        }
    }
}
