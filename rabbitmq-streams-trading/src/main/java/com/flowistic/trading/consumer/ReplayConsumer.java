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
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Consumer for replaying historical market data.
 * 
 * RabbitMQ Streams excel at replay scenarios because:
 * - Messages are persisted on disk with configurable retention
 * - You can start consuming from any offset or timestamp
 * - Multiple consumers can read the same stream without affecting each other
 * - Reading is non-destructive (unlike regular queues)
 * 
 * Use cases:
 * - Backtesting trading strategies
 * - Reconstructing order book state
 * - Debugging production issues
 * - Training ML models on historical data
 */
public class ReplayConsumer implements AutoCloseable {
    
    private static final Logger logger = LoggerFactory.getLogger(ReplayConsumer.class);
    private static final DateTimeFormatter FORMATTER = 
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    
    private final StreamConfig config;
    private final MarketDataCodec codec;
    private final Environment environment;
    private Consumer consumer;
    private final AtomicLong messageCount;
    private final AtomicBoolean completed;
    
    public ReplayConsumer(StreamConfig config) {
        this.config = config;
        this.codec = new MarketDataCodec();
        this.messageCount = new AtomicLong(0);
        this.completed = new AtomicBoolean(false);
        
        this.environment = Environment.builder()
            .host(config.host())
            .port(config.port())
            .username(config.username())
            .password(config.password())
            .virtualHost(config.virtualHost())
            .build();
        
        logger.info("ReplayConsumer initialized for stream: {}", config.streamName());
    }
    
    /**
     * Replay all data from the stream and collect results.
     */
    public ReplayResult replayAll(Duration timeout) {
        return replay(OffsetSpecification.first(), null, timeout);
    }
    
    /**
     * Replay data from a specific timestamp.
     */
    public ReplayResult replayFromTime(Instant fromTime, Duration timeout) {
        return replay(OffsetSpecification.timestamp(fromTime.toEpochMilli()), null, timeout);
    }
    
    /**
     * Replay data within a time range.
     */
    public ReplayResult replayTimeRange(Instant fromTime, Instant toTime, Duration timeout) {
        return replay(
            OffsetSpecification.timestamp(fromTime.toEpochMilli()),
            toTime,
            timeout
        );
    }
    
    /**
     * Replay data for specific symbols.
     */
    public ReplayResult replaySymbols(Set<String> symbols, Duration timeout) {
        logger.info("Replaying data for symbols: {}", symbols);
        
        Map<String, List<MarketData>> dataBySymbol = new ConcurrentHashMap<>();
        symbols.forEach(s -> dataBySymbol.put(s, Collections.synchronizedList(new ArrayList<>())));
        
        AtomicLong totalProcessed = new AtomicLong(0);
        AtomicLong matched = new AtomicLong(0);
        CountDownLatch latch = new CountDownLatch(1);
        
        this.consumer = environment.consumerBuilder()
            .stream(config.streamName())
            .offset(OffsetSpecification.first())
            .messageHandler((context, message) -> {
                try {
                    MarketData data = codec.decode(message);
                    totalProcessed.incrementAndGet();
                    
                    if (symbols.contains(data.symbol())) {
                        matched.incrementAndGet();
                        dataBySymbol.get(data.symbol()).add(data);
                    }
                } catch (Exception e) {
                    logger.error("Error processing message: {}", e.getMessage());
                }
            })
            .build();
        
        try {
            latch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        consumer.close();
        
        logger.info("Replay complete: {} total messages, {} matched symbols", 
            totalProcessed.get(), matched.get());
        
        return new ReplayResult(dataBySymbol, totalProcessed.get(), matched.get());
    }
    
    private ReplayResult replay(OffsetSpecification offsetSpec, Instant endTime, Duration timeout) {
        Map<String, List<MarketData>> dataBySymbol = new ConcurrentHashMap<>();
        AtomicLong totalProcessed = new AtomicLong(0);
        CountDownLatch latch = new CountDownLatch(1);
        
        this.consumer = environment.consumerBuilder()
            .stream(config.streamName())
            .offset(offsetSpec)
            .messageHandler((context, message) -> {
                try {
                    MarketData data = codec.decode(message);
                    
                    // Check if we've passed the end time
                    if (endTime != null && data.timestamp().isAfter(endTime)) {
                        latch.countDown();
                        return;
                    }
                    
                    totalProcessed.incrementAndGet();
                    dataBySymbol.computeIfAbsent(
                        data.symbol(), 
                        k -> Collections.synchronizedList(new ArrayList<>())
                    ).add(data);
                    
                } catch (Exception e) {
                    logger.error("Error processing message: {}", e.getMessage());
                }
            })
            .build();
        
        try {
            latch.await(timeout.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        consumer.close();
        
        logger.info("Replay complete: {} total messages", totalProcessed.get());
        
        return new ReplayResult(dataBySymbol, totalProcessed.get(), totalProcessed.get());
    }
    
    /**
     * Get stream statistics without consuming.
     */
    public StreamStats getStreamStats(Duration sampleDuration) {
        logger.info("Collecting stream statistics for {} ...", sampleDuration);
        
        Map<String, Integer> tickCountBySymbol = new ConcurrentHashMap<>();
        Map<String, Integer> tickCountByExchange = new ConcurrentHashMap<>();
        AtomicLong totalTicks = new AtomicLong(0);
        AtomicLong firstOffset = new AtomicLong(-1);
        AtomicLong lastOffset = new AtomicLong(-1);
        
        List<Instant> timestamps = Collections.synchronizedList(new ArrayList<>());
        
        CountDownLatch latch = new CountDownLatch(1);
        
        this.consumer = environment.consumerBuilder()
            .stream(config.streamName())
            .offset(OffsetSpecification.first())
            .messageHandler((context, message) -> {
                try {
                    MarketData data = codec.decode(message);
                    
                    firstOffset.compareAndSet(-1, context.offset());
                    lastOffset.set(context.offset());
                    totalTicks.incrementAndGet();
                    
                    tickCountBySymbol.merge(data.symbol(), 1, Integer::sum);
                    tickCountByExchange.merge(data.exchange(), 1, Integer::sum);
                    
                    if (timestamps.size() < 10000) {
                        timestamps.add(data.timestamp());
                    }
                    
                } catch (Exception e) {
                    logger.error("Error processing message: {}", e.getMessage());
                }
            })
            .build();
        
        try {
            latch.await(sampleDuration.toMillis(), TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        
        consumer.close();
        
        return new StreamStats(
            config.streamName(),
            firstOffset.get(),
            lastOffset.get(),
            totalTicks.get(),
            tickCountBySymbol,
            tickCountByExchange,
            timestamps.isEmpty() ? null : timestamps.get(0),
            timestamps.isEmpty() ? null : timestamps.get(timestamps.size() - 1)
        );
    }
    
    @Override
    public void close() {
        if (consumer != null) {
            consumer.close();
        }
        if (environment != null) {
            environment.close();
        }
        logger.info("ReplayConsumer closed");
    }
    
    /**
     * Result of a replay operation.
     */
    public record ReplayResult(
        Map<String, List<MarketData>> dataBySymbol,
        long totalProcessed,
        long matchedCount
    ) {
        public int symbolCount() {
            return dataBySymbol.size();
        }
        
        public List<MarketData> getDataForSymbol(String symbol) {
            return dataBySymbol.getOrDefault(symbol, List.of());
        }
        
        public void printSummary() {
            logger.info("=== Replay Summary ===");
            logger.info("Total messages: {}", totalProcessed);
            logger.info("Matched messages: {}", matchedCount);
            logger.info("Symbols: {}", dataBySymbol.keySet());
            dataBySymbol.forEach((symbol, data) -> {
                logger.info("  {}: {} ticks", symbol, data.size());
            });
        }
    }
    
    /**
     * Statistics about the stream.
     */
    public record StreamStats(
        String streamName,
        long firstOffset,
        long lastOffset,
        long totalMessages,
        Map<String, Integer> ticksBySymbol,
        Map<String, Integer> ticksByExchange,
        Instant firstTimestamp,
        Instant lastTimestamp
    ) {
        public void print() {
            logger.info("=== Stream Statistics: {} ===", streamName);
            logger.info("Offset range: {} - {}", firstOffset, lastOffset);
            logger.info("Total messages: {}", totalMessages);
            
            if (firstTimestamp != null && lastTimestamp != null) {
                Duration span = Duration.between(firstTimestamp, lastTimestamp);
                logger.info("Time range: {} to {} ({})", 
                    LocalDateTime.ofInstant(firstTimestamp, ZoneId.systemDefault()).format(FORMATTER),
                    LocalDateTime.ofInstant(lastTimestamp, ZoneId.systemDefault()).format(FORMATTER),
                    span);
            }
            
            logger.info("Ticks by symbol:");
            ticksBySymbol.entrySet().stream()
                .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
                .forEach(e -> logger.info("  {}: {}", e.getKey(), e.getValue()));
            
            logger.info("Ticks by exchange:");
            ticksByExchange.forEach((k, v) -> logger.info("  {}: {}", k, v));
        }
    }
    
    public static void main(String[] args) throws Exception {
        StreamConfig config = StreamConfig.defaults();
        
        try (ReplayConsumer consumer = new ReplayConsumer(config)) {
            
            // Get stream statistics
            StreamStats stats = consumer.getStreamStats(Duration.ofSeconds(10));
            stats.print();
            
            // Replay data for specific symbols
            ReplayResult result = consumer.replaySymbols(
                Set.of("AAPL", "GOOGL", "MSFT"),
                Duration.ofSeconds(30)
            );
            result.printSummary();
            
            // Access individual symbol data
            List<MarketData> aaplData = result.getDataForSymbol("AAPL");
            if (!aaplData.isEmpty()) {
                logger.info("First AAPL tick: {}", aaplData.get(0));
                logger.info("Last AAPL tick: {}", aaplData.get(aaplData.size() - 1));
            }
        }
    }
}
