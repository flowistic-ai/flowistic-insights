# RabbitMQ Streams for Market Data

This project demonstrates how to use RabbitMQ Streams for high-throughput, low-latency market data consumption. RabbitMQ Streams provide a persistent, append-only log that's perfect for financial market data scenarios.

## Why RabbitMQ Streams for Market Data?

| Feature | Benefit for Market Data |
|---------|------------------------|
| **Persistent Log** | Replay historical data for backtesting strategies |
| **High Throughput** | Handle millions of ticks per second |
| **Time-based Consumption** | Start consuming from any point in time |
| **Offset Tracking** | Resume from last position after failures |
| **Single Active Consumer** | Maintain ordering with automatic failover |
| **Non-destructive Reads** | Multiple consumers read the same data |

## Project Structure

```
rabbitmq-streams-trading/
├── docker-compose.yml              # RabbitMQ with Streams enabled
├── pom.xml                         # Maven dependencies
├── src/main/java/com/flowistic/trading/
│   ├── config/
│   │   └── StreamConfig.java       # Configuration for streams
│   ├── model/
│   │   └── MarketData.java         # Market data tick model
│   ├── serialization/
│   │   └── MarketDataCodec.java    # JSON serialization
│   ├── producer/
│   │   └── MarketDataProducer.java # Simulated market data publisher
│   └── consumer/
│       ├── MarketDataConsumer.java      # Basic consumer with offset options
│       ├── SingleActiveConsumer.java    # Ordered processing with failover
│       ├── BatchingConsumer.java        # High-throughput micro-batching
│       └── ReplayConsumer.java          # Historical data replay
└── src/main/resources/
    └── logback.xml                 # Logging configuration
```

## Quick Start

### 1. Start RabbitMQ with Streams

```bash
cd rabbitmq-streams-trading
docker-compose up -d
```

Wait for RabbitMQ to be healthy:
```bash
docker-compose ps
```

Access the management UI at http://localhost:15672 (guest/guest)

### 2. Build the Project

```bash
mvn clean package
```

### 3. Run the Producer

Generate simulated market data:

```bash
mvn exec:java -Dexec.mainClass="com.flowistic.trading.producer.MarketDataProducer"
```

### 4. Run a Consumer

**Basic Consumer** - consume from the beginning:
```bash
mvn exec:java -Dexec.mainClass="com.flowistic.trading.consumer.MarketDataConsumer"
```

**Batching Consumer** - high-throughput with aggregation:
```bash
mvn exec:java -Dexec.mainClass="com.flowistic.trading.consumer.BatchingConsumer"
```

**Replay Consumer** - replay historical data:
```bash
mvn exec:java -Dexec.mainClass="com.flowistic.trading.consumer.ReplayConsumer"
```

**Single Active Consumer** - for ordered processing:
```bash
# Terminal 1
mvn exec:java -Dexec.mainClass="com.flowistic.trading.consumer.SingleActiveConsumer" -Dexec.args="consumer-1"

# Terminal 2 (standby, will activate if consumer-1 fails)
mvn exec:java -Dexec.mainClass="com.flowistic.trading.consumer.SingleActiveConsumer" -Dexec.args="consumer-2"
```

## Consumer Patterns

### 1. Basic Consumer

The `MarketDataConsumer` demonstrates various offset specifications:

```java
// Start from the beginning (replay all)
consumer.consumeFromFirst(handler);

// Start from the last message
consumer.consumeFromLast(handler);

// Only new messages
consumer.consumeFromNext(handler);

// Resume from a specific offset
consumer.consumeFromOffset(12345L, handler);

// Start from a specific time
consumer.consumeFromTimestamp(Instant.parse("2024-01-15T09:30:00Z"), handler);

// Filter by symbols
consumer.consumeSymbols(OffsetSpecification.first(), handler, "AAPL", "GOOGL");
```

### 2. Single Active Consumer

Ensures ordered processing with automatic failover:

```java
SingleActiveConsumer consumer = new SingleActiveConsumer(config, "processor-1", "trading-group");

consumer.start((data, offset) -> {
    // Only the active consumer processes messages
    processMarketData(data);
});

// Check if this instance is active
if (consumer.isActive()) {
    logger.info("I am the active consumer");
}
```

### 3. Batching Consumer

For high-throughput scenarios with aggregation:

```java
BatchingConsumer consumer = new BatchingConsumer(
    config,
    1000,                      // batch size
    Duration.ofMillis(100)     // flush interval
);

consumer.start(batch -> {
    // Process batch of market data
    Map<String, SymbolStats> stats = BatchingConsumer.aggregateBySymbol(batch);
    
    // Calculate VWAP per symbol
    stats.forEach((symbol, stat) -> {
        logger.info("{}: VWAP={}, High={}, Low={}", 
            symbol, stat.getVwap(), stat.getHigh(), stat.getLow());
    });
});
```

### 4. Replay Consumer

For backtesting and historical analysis:

```java
ReplayConsumer consumer = new ReplayConsumer(config);

// Replay all data
ReplayResult result = consumer.replayAll(Duration.ofMinutes(5));

// Replay specific time range
ReplayResult result = consumer.replayTimeRange(
    Instant.parse("2024-01-15T09:30:00Z"),
    Instant.parse("2024-01-15T10:00:00Z"),
    Duration.ofMinutes(5)
);

// Replay specific symbols
ReplayResult result = consumer.replaySymbols(
    Set.of("AAPL", "GOOGL", "MSFT"),
    Duration.ofMinutes(5)
);

// Access results
List<MarketData> aaplData = result.getDataForSymbol("AAPL");
```

## Configuration

### Stream Settings

```java
StreamConfig config = StreamConfig.builder()
    .host("localhost")
    .port(5552)
    .username("guest")
    .password("guest")
    .streamName("market-data")
    .maxAge(Duration.ofHours(24))           // Retain data for 24 hours
    .maxLengthBytes(10_000_000_000L)        // Max 10 GB
    .maxSegmentSizeBytes(500_000_000)       // 500 MB per segment
    .build();
```

### RabbitMQ Stream Settings

In `rabbitmq.conf`:
```
# Stream port
listeners.stream.default = 5552

# Credits for flow control
stream.initial_credits = 50000
stream.credits_required_for_unblocking = 25000
```

## Performance Tips

1. **Use Sub-Entry Batching**: The producer batches messages into sub-entries for higher throughput
   ```java
   .subEntrySize(100)      // 100 messages per sub-entry
   .batchSize(1000)        // 1000 sub-entries per batch
   ```

2. **Tune Flow Control**: Adjust credits based on consumer processing speed
   
3. **Use Bounded Buffers**: Apply backpressure when consumers can't keep up
   
4. **Parallel Processing**: Use multiple threads to process batches

5. **Client-side Filtering**: Filter symbols client-side or use Super Streams for server-side partitioning

## Monitoring

Access RabbitMQ Management UI at http://localhost:15672

The Streams tab shows:
- Stream size and message count
- Publisher and consumer connections
- Message rates

## Cleanup

```bash
docker-compose down -v
```

## Next Steps

- **Super Streams**: For partitioned streams with server-side routing
- **Deduplication**: Add deduplication using message IDs
- **Compression**: Enable compression for lower network usage
- **TLS**: Configure TLS for production deployments
