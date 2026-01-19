package com.flowistic.trading.consumer;

import com.flowistic.trading.config.StreamConfig;
import com.flowistic.trading.model.MarketData;
import com.flowistic.trading.serialization.MarketDataCodec;
import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.ConsumerBuilder;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.OffsetSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Demonstrates Single Active Consumer pattern for RabbitMQ Streams.
 * 
 * Single Active Consumer ensures that only one consumer in a group 
 * processes messages at a time. This is crucial for:
 * - Maintaining message ordering across consumers
 * - Avoiding duplicate processing during failover
 * - Implementing active-passive consumer patterns
 * 
 * If the active consumer fails, another consumer in the group 
 * automatically takes over from the last committed offset.
 */
public class SingleActiveConsumer implements AutoCloseable {
    
    private static final Logger logger = LoggerFactory.getLogger(SingleActiveConsumer.class);
    
    private final StreamConfig config;
    private final String consumerName;
    private final String consumerGroup;
    private final MarketDataCodec codec;
    private final Environment environment;
    private Consumer consumer;
    private final AtomicBoolean isActive;
    private final AtomicLong messageCount;
    private final AtomicLong lastOffset;
    
    public SingleActiveConsumer(StreamConfig config, String consumerName, String consumerGroup) {
        this.config = config;
        this.consumerName = consumerName;
        this.consumerGroup = consumerGroup;
        this.codec = new MarketDataCodec();
        this.isActive = new AtomicBoolean(false);
        this.messageCount = new AtomicLong(0);
        this.lastOffset = new AtomicLong(-1);
        
        this.environment = Environment.builder()
            .host(config.host())
            .port(config.port())
            .username(config.username())
            .password(config.password())
            .virtualHost(config.virtualHost())
            .build();
        
        logger.info("SingleActiveConsumer '{}' initialized in group '{}'", consumerName, consumerGroup);
    }
    
    /**
     * Start the consumer with Single Active Consumer semantics.
     */
    public void start(MarketDataConsumer.MarketDataHandler handler) {
        logger.info("Starting Single Active Consumer: {}", consumerName);
        
        this.consumer = environment.consumerBuilder()
            .stream(config.streamName())
            .name(consumerName)
            .singleActiveConsumer()
            .consumerUpdateListener(context -> {
                if (context.isActive()) {
                    isActive.set(true);
                    logger.info("Consumer '{}' is now ACTIVE", consumerName);
                } else {
                    isActive.set(false);
                    logger.info("Consumer '{}' is now PASSIVE", consumerName);
                }
                // Return the offset to start from when becoming active
                // Using stored offset allows seamless failover
                return OffsetSpecification.next();
            })
            .autoTrackingStrategy()              // Automatically track offsets
                .flushInterval(Duration.ofSeconds(5))  // Flush every 5 seconds
                .messageCountBeforeStorage(1000)       // Or every 1000 messages
            .builder()
            .messageHandler((context, message) -> {
                if (!isActive.get()) {
                    // Shouldn't happen with SAC, but defensive check
                    return;
                }
                
                try {
                    MarketData marketData = codec.decode(message);
                    messageCount.incrementAndGet();
                    lastOffset.set(context.offset());
                    
                    handler.handle(marketData, context.offset());
                    
                } catch (Exception e) {
                    logger.error("Error processing message: {}", e.getMessage());
                }
            })
            .build();
        
        logger.info("Consumer '{}' started, waiting for activation...", consumerName);
    }
    
    /**
     * Check if this consumer is currently active.
     */
    public boolean isActive() {
        return isActive.get();
    }
    
    /**
     * Get the message count processed by this consumer.
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
        logger.info("SingleActiveConsumer '{}' closed. Messages processed: {}", 
            consumerName, messageCount.get());
    }
    
    /**
     * Demo: Run two consumers in the same group to demonstrate failover.
     */
    public static void main(String[] args) throws Exception {
        StreamConfig config = StreamConfig.defaults();
        String consumerGroup = "market-data-processors";
        
        // Simulate which consumer instance this is (from command line arg)
        String consumerId = args.length > 0 ? args[0] : "consumer-1";
        
        try (SingleActiveConsumer consumer = 
                new SingleActiveConsumer(config, consumerId, consumerGroup)) {
            
            consumer.start((data, offset) -> {
                logger.info("[{}] Processing {} - {} @ {}", 
                    offset, data.symbol(), data.lastPrice(), data.timestamp());
            });
            
            // Keep running
            logger.info("Consumer {} running. Press Ctrl+C to stop.", consumerId);
            Thread.currentThread().join();
        }
    }
}
