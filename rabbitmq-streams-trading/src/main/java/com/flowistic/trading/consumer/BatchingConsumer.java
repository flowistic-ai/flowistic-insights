package com.flowistic.trading.consumer;

import com.flowistic.trading.config.StreamConfig;
import com.flowistic.trading.model.MarketData;
import com.flowistic.trading.serialization.MarketDataCodec;
import com.rabbitmq.stream.Consumer;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.OffsetSpecification;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

/**
 * High-performance batching consumer for market data.
 * 
 * This consumer demonstrates:
 * - Micro-batching for improved throughput
 * - Per-symbol aggregation windows
 * - VWAP (Volume Weighted Average Price) calculation
 * - Backpressure handling
 */
public class BatchingConsumer implements AutoCloseable {
    
    private static final Logger logger = LoggerFactory.getLogger(BatchingConsumer.class);
    
    private final StreamConfig config;
    private final MarketDataCodec codec;
    private final Environment environment;
    private Consumer consumer;
    
    private final BlockingQueue<MarketData> buffer;
    private final ScheduledExecutorService scheduler;
    private final ExecutorService processor;
    private final AtomicLong messageCount;
    private final AtomicLong batchCount;
    
    private final int batchSize;
    private final Duration batchTimeout;
    
    public BatchingConsumer(StreamConfig config, int batchSize, Duration batchTimeout) {
        this.config = config;
        this.batchSize = batchSize;
        this.batchTimeout = batchTimeout;
        this.codec = new MarketDataCodec();
        this.messageCount = new AtomicLong(0);
        this.batchCount = new AtomicLong(0);
        
        // Bounded buffer for backpressure
        this.buffer = new LinkedBlockingQueue<>(batchSize * 10);
        this.scheduler = Executors.newSingleThreadScheduledExecutor();
        this.processor = Executors.newFixedThreadPool(
            Runtime.getRuntime().availableProcessors()
        );
        
        this.environment = Environment.builder()
            .host(config.host())
            .port(config.port())
            .username(config.username())
            .password(config.password())
            .virtualHost(config.virtualHost())
            .build();
        
        logger.info("BatchingConsumer initialized with batchSize={}, timeout={}",
            batchSize, batchTimeout);
    }
    
    /**
     * Start consuming with batching.
     */
    public void start(BatchHandler batchHandler) {
        // Start the batch flushing timer
        scheduler.scheduleAtFixedRate(
            () -> flushBatch(batchHandler),
            batchTimeout.toMillis(),
            batchTimeout.toMillis(),
            TimeUnit.MILLISECONDS
        );
        
        // Start the consumer
        this.consumer = environment.consumerBuilder()
            .stream(config.streamName())
            .offset(OffsetSpecification.first())
            .messageHandler((context, message) -> {
                try {
                    MarketData data = codec.decode(message);
                    messageCount.incrementAndGet();
                    
                    // Try to add to buffer, apply backpressure if full
                    if (!buffer.offer(data, 100, TimeUnit.MILLISECONDS)) {
                        logger.warn("Buffer full, applying backpressure");
                    }
                    
                    // Flush if batch size reached
                    if (buffer.size() >= batchSize) {
                        flushBatch(batchHandler);
                    }
                    
                } catch (Exception e) {
                    logger.error("Error buffering message: {}", e.getMessage());
                }
            })
            .build();
        
        logger.info("BatchingConsumer started");
    }
    
    private synchronized void flushBatch(BatchHandler handler) {
        if (buffer.isEmpty()) {
            return;
        }
        
        List<MarketData> batch = new ArrayList<>(batchSize);
        buffer.drainTo(batch, batchSize);
        
        if (!batch.isEmpty()) {
            batchCount.incrementAndGet();
            processor.submit(() -> {
                try {
                    handler.handleBatch(batch);
                } catch (Exception e) {
                    logger.error("Error processing batch: {}", e.getMessage());
                }
            });
        }
    }
    
    /**
     * Get aggregated statistics by symbol.
     */
    public static Map<String, SymbolStats> aggregateBySymbol(List<MarketData> batch) {
        Map<String, SymbolStats> stats = new HashMap<>();
        
        for (MarketData data : batch) {
            stats.computeIfAbsent(data.symbol(), SymbolStats::new)
                 .update(data);
        }
        
        return stats;
    }
    
    public long getMessageCount() {
        return messageCount.get();
    }
    
    public long getBatchCount() {
        return batchCount.get();
    }
    
    @Override
    public void close() {
        scheduler.shutdown();
        processor.shutdown();
        try {
            scheduler.awaitTermination(5, TimeUnit.SECONDS);
            processor.awaitTermination(5, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        if (consumer != null) {
            consumer.close();
        }
        if (environment != null) {
            environment.close();
        }
        
        logger.info("BatchingConsumer closed. Messages: {}, Batches: {}", 
            messageCount.get(), batchCount.get());
    }
    
    /**
     * Handler for processing batches of market data.
     */
    @FunctionalInterface
    public interface BatchHandler {
        void handleBatch(List<MarketData> batch);
    }
    
    /**
     * Statistics aggregation per symbol.
     */
    public static class SymbolStats {
        private final String symbol;
        private int tickCount;
        private java.math.BigDecimal sumPrice = java.math.BigDecimal.ZERO;
        private java.math.BigDecimal sumVolume = java.math.BigDecimal.ZERO;
        private java.math.BigDecimal sumPriceVolume = java.math.BigDecimal.ZERO;
        private java.math.BigDecimal high = java.math.BigDecimal.ZERO;
        private java.math.BigDecimal low = new java.math.BigDecimal("999999999");
        private java.math.BigDecimal lastPrice;
        private Instant firstTick;
        private Instant lastTick;
        
        public SymbolStats(String symbol) {
            this.symbol = symbol;
        }
        
        public void update(MarketData data) {
            tickCount++;
            var price = data.lastPrice();
            var size = data.lastSize();
            
            sumPrice = sumPrice.add(price);
            sumVolume = sumVolume.add(size);
            sumPriceVolume = sumPriceVolume.add(price.multiply(size));
            
            if (price.compareTo(high) > 0) high = price;
            if (price.compareTo(low) < 0) low = price;
            
            lastPrice = price;
            if (firstTick == null) firstTick = data.timestamp();
            lastTick = data.timestamp();
        }
        
        public java.math.BigDecimal getVwap() {
            if (sumVolume.compareTo(java.math.BigDecimal.ZERO) == 0) {
                return java.math.BigDecimal.ZERO;
            }
            return sumPriceVolume.divide(sumVolume, 4, java.math.RoundingMode.HALF_UP);
        }
        
        public java.math.BigDecimal getAvgPrice() {
            if (tickCount == 0) return java.math.BigDecimal.ZERO;
            return sumPrice.divide(java.math.BigDecimal.valueOf(tickCount), 
                4, java.math.RoundingMode.HALF_UP);
        }
        
        @Override
        public String toString() {
            return String.format(
                "SymbolStats{symbol='%s', ticks=%d, vwap=%s, avg=%s, high=%s, low=%s, last=%s}",
                symbol, tickCount, getVwap(), getAvgPrice(), high, low, lastPrice);
        }
    }
    
    public static void main(String[] args) throws Exception {
        StreamConfig config = StreamConfig.defaults();
        
        try (BatchingConsumer consumer = new BatchingConsumer(
                config, 
                1000,                           // Batch size
                Duration.ofMillis(100))) {      // Flush interval
            
            consumer.start(batch -> {
                // Aggregate statistics by symbol
                Map<String, SymbolStats> stats = aggregateBySymbol(batch);
                
                logger.info("Batch processed: {} ticks across {} symbols",
                    batch.size(), stats.size());
                
                stats.values().forEach(s -> logger.info("  {}", s));
            });
            
            // Run for 60 seconds
            Thread.sleep(Duration.ofSeconds(60).toMillis());
            
            logger.info("Total messages: {}, batches: {}", 
                consumer.getMessageCount(), consumer.getBatchCount());
        }
    }
}
