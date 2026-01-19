package com.flowistic.trading.producer;

import com.flowistic.trading.config.StreamConfig;
import com.flowistic.trading.model.MarketData;
import com.flowistic.trading.serialization.MarketDataCodec;
import com.rabbitmq.stream.ByteCapacity;
import com.rabbitmq.stream.Environment;
import com.rabbitmq.stream.Producer;
import com.rabbitmq.stream.ConfirmationHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Instant;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Simulates market data feed and publishes to RabbitMQ Stream.
 * 
 * This producer demonstrates:
 * - Creating a stream with retention policies
 * - High-throughput publishing with sub-entry batching
 * - Confirmation handling for reliable delivery
 */
public class MarketDataProducer implements AutoCloseable {
    
    private static final Logger logger = LoggerFactory.getLogger(MarketDataProducer.class);
    
    private final StreamConfig config;
    private final MarketDataCodec codec;
    private final Environment environment;
    private final Producer producer;
    private final AtomicLong sequenceNumber;
    private final Random random;
    
    // Simulated symbols with base prices
    private static final List<SymbolPrice> SYMBOLS = List.of(
        new SymbolPrice("AAPL", new BigDecimal("185.50")),
        new SymbolPrice("GOOGL", new BigDecimal("142.75")),
        new SymbolPrice("MSFT", new BigDecimal("378.25")),
        new SymbolPrice("AMZN", new BigDecimal("178.50")),
        new SymbolPrice("TSLA", new BigDecimal("248.75")),
        new SymbolPrice("NVDA", new BigDecimal("495.00")),
        new SymbolPrice("META", new BigDecimal("505.25")),
        new SymbolPrice("JPM", new BigDecimal("195.50")),
        new SymbolPrice("V", new BigDecimal("275.00")),
        new SymbolPrice("JNJ", new BigDecimal("156.25"))
    );
    
    private record SymbolPrice(String symbol, BigDecimal basePrice) {}
    
    public MarketDataProducer(StreamConfig config) {
        this.config = config;
        this.codec = new MarketDataCodec();
        this.sequenceNumber = new AtomicLong(0);
        this.random = new Random();
        
        // Create the stream environment
        this.environment = Environment.builder()
            .host(config.host())
            .port(config.port())
            .username(config.username())
            .password(config.password())
            .virtualHost(config.virtualHost())
            .build();
        
        // Create the stream if it doesn't exist
        createStream();
        
        // Create the producer with sub-entry batching for high throughput
        this.producer = environment.producerBuilder()
            .stream(config.streamName())
            .subEntrySize(100)              // Batch 100 messages per sub-entry
            .batchSize(1000)                // Batch up to 1000 sub-entries
            .build();
        
        logger.info("MarketDataProducer initialized for stream: {}", config.streamName());
    }
    
    private void createStream() {
        try {
            environment.streamCreator()
                .stream(config.streamName())
                .maxAge(config.maxAge())
                .maxLengthBytes(ByteCapacity.from(String.valueOf(config.maxLengthBytes())))
                .maxSegmentSizeBytes(ByteCapacity.from(String.valueOf(config.maxSegmentSizeBytes())))
                .create();
            logger.info("Created stream: {}", config.streamName());
        } catch (Exception e) {
            // Stream might already exist, which is fine
            logger.debug("Stream already exists or creation failed: {}", e.getMessage());
        }
    }
    
    /**
     * Publish a single market data tick.
     */
    public void publish(MarketData marketData, ConfirmationHandler confirmationHandler) {
        producer.send(codec.encode(marketData), confirmationHandler);
    }
    
    /**
     * Generate and publish simulated market data.
     */
    public void publishSimulated(int count, int delayMs) throws InterruptedException {
        logger.info("Publishing {} simulated market data ticks with {}ms delay", count, delayMs);
        
        CountDownLatch latch = new CountDownLatch(count);
        AtomicLong confirmed = new AtomicLong(0);
        AtomicLong errored = new AtomicLong(0);
        
        long startTime = System.currentTimeMillis();
        
        for (int i = 0; i < count; i++) {
            MarketData tick = generateTick();
            
            publish(tick, confirmationStatus -> {
                if (confirmationStatus.isConfirmed()) {
                    confirmed.incrementAndGet();
                } else {
                    errored.incrementAndGet();
                    logger.warn("Message not confirmed: {}", confirmationStatus.getMessage());
                }
                latch.countDown();
            });
            
            if (delayMs > 0 && i < count - 1) {
                Thread.sleep(delayMs);
            }
            
            if ((i + 1) % 10000 == 0) {
                logger.info("Published {} ticks...", i + 1);
            }
        }
        
        latch.await();
        
        long duration = System.currentTimeMillis() - startTime;
        double throughput = (count * 1000.0) / duration;
        
        logger.info("Publishing complete: {} confirmed, {} errors, {:.2f} msg/sec", 
            confirmed.get(), errored.get(), throughput);
    }
    
    /**
     * Generate a simulated market data tick.
     */
    private MarketData generateTick() {
        SymbolPrice symbolPrice = SYMBOLS.get(random.nextInt(SYMBOLS.size()));
        
        // Generate price with some randomness (±0.5%)
        double priceVariation = 1.0 + (random.nextDouble() - 0.5) * 0.01;
        BigDecimal currentPrice = symbolPrice.basePrice()
            .multiply(BigDecimal.valueOf(priceVariation))
            .setScale(2, RoundingMode.HALF_UP);
        
        // Spread typically 0.01% to 0.05%
        BigDecimal spreadPercent = BigDecimal.valueOf(0.0001 + random.nextDouble() * 0.0004);
        BigDecimal halfSpread = currentPrice.multiply(spreadPercent)
            .divide(BigDecimal.valueOf(2), 2, RoundingMode.HALF_UP);
        
        BigDecimal bidPrice = currentPrice.subtract(halfSpread);
        BigDecimal askPrice = currentPrice.add(halfSpread);
        
        return MarketData.builder()
            .symbol(symbolPrice.symbol())
            .bidPrice(bidPrice)
            .askPrice(askPrice)
            .bidSize(BigDecimal.valueOf(100 + random.nextInt(900)))
            .askSize(BigDecimal.valueOf(100 + random.nextInt(900)))
            .lastPrice(currentPrice)
            .lastSize(BigDecimal.valueOf(1 + random.nextInt(100)))
            .volume(BigDecimal.valueOf(1_000_000 + random.nextInt(10_000_000)))
            .timestamp(Instant.now())
            .exchange(random.nextBoolean() ? "NYSE" : "NASDAQ")
            .sequenceNumber(sequenceNumber.incrementAndGet())
            .build();
    }
    
    @Override
    public void close() {
        if (producer != null) {
            producer.close();
        }
        if (environment != null) {
            environment.close();
        }
        logger.info("MarketDataProducer closed");
    }
    
    public static void main(String[] args) throws Exception {
        StreamConfig config = StreamConfig.defaults();
        
        try (MarketDataProducer producer = new MarketDataProducer(config)) {
            // Publish 100,000 ticks with no delay (max throughput)
            producer.publishSimulated(100_000, 0);
        }
    }
}
