# RabbitMQ Streams for Market Data (Python)

This project demonstrates how to use RabbitMQ Streams for high-throughput, low-latency market data consumption using Python and the `rstream` library.

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
├── pyproject.toml                  # Project config & dependencies (uv)
├── src/
│   ├── __init__.py
│   ├── config.py                   # Stream configuration
│   ├── models.py                   # MarketData model with Pydantic
│   ├── producer.py                 # Simulated market data publisher
│   ├── consumer.py                 # Basic consumer with offset options
│   ├── single_active_consumer.py   # Ordered processing with failover
│   ├── batching_consumer.py        # High-throughput micro-batching
│   ├── replay_consumer.py          # Historical data replay
│   └── dashboard.py                # Streamlit visualization dashboard
└── README.md
```

## Quick Start

### 1. Start RabbitMQ with Streams

```bash
cd rabbitmq-streams-trading
docker compose up -d
```

Wait for RabbitMQ to be healthy:
```bash
docker compose ps
```

Access the management UI at http://localhost:15672 (guest/guest)

### 2. Install Dependencies with uv

```bash
uv sync
```

### 3. Run the Producer

Generate simulated market data:

```bash
uv run python -m src.producer
```

### 4. Run a Consumer

**Basic Consumer** - consume from the beginning:
```bash
uv run python -m src.consumer
```

**Batching Consumer** - high-throughput with aggregation:
```bash
uv run python -m src.batching_consumer
```

**Replay Consumer** - replay historical data:
```bash
uv run python -m src.replay_consumer
```

**Single Active Consumer** - for ordered processing:
```bash
# Terminal 1
uv run python -m src.single_active_consumer consumer-1

# Terminal 2 (standby, will activate if consumer-1 fails)
uv run python -m src.single_active_consumer consumer-2
```

### 5. Run the Streamlit Dashboard

Visualize real-time production and consumption:

```bash
uv run streamlit run src/dashboard.py
```

The dashboard shows:
- Live producer/consumer metrics
- Multiple consumer status and throughput
- Symbol statistics with VWAP
- Real-time price charts

## Multiple Consumers

RabbitMQ Streams supports multiple concurrent consumers reading from the same stream:

```python
# Each consumer reads independently - non-destructive reads
consumer1 = MarketDataConsumer(config)
consumer2 = MarketDataConsumer(config)
consumer3 = MarketDataConsumer(config)

# All three can consume from the same stream simultaneously
# Each tracks its own offset independently
await consumer1.consume_from_first(handler1)  # Reads all from beginning
await consumer2.consume_from_last(handler2)   # Reads only latest
await consumer3.consume_from_offset(5000, handler3)  # Reads from offset 5000
```

Key benefits:
- **Non-destructive reads**: Unlike queues, messages aren't removed after consumption
- **Independent offsets**: Each consumer tracks its own position
- **Parallel processing**: Multiple consumers can process concurrently
- **Replay capability**: New consumers can read historical data

## Consumer Patterns

### 1. Basic Consumer

The `MarketDataConsumer` demonstrates various offset specifications:

```python
from src.consumer import MarketDataConsumer
from src.config import StreamConfig

config = StreamConfig.defaults()
consumer = MarketDataConsumer(config)

await consumer.start()

# Start from the beginning (replay all)
await consumer.consume_from_first(handler)

# Start from the last message
await consumer.consume_from_last(handler)

# Only new messages
await consumer.consume_from_next(handler)

# Resume from a specific offset
await consumer.consume_from_offset(12345, handler)

# Start from a specific time
from datetime import datetime
await consumer.consume_from_timestamp(
    datetime(2024, 1, 15, 9, 30), 
    handler
)

# Filter by symbols
await consumer.consume_symbols(
    offset_spec, 
    handler, 
    {"AAPL", "GOOGL"}
)
```

### 2. Single Active Consumer

Ensures ordered processing with automatic failover:

```python
from src.single_active_consumer import SingleActiveConsumer

consumer = SingleActiveConsumer(config, "processor-1")

async def handle_message(data: MarketData, offset: int):
    # Only the active consumer processes messages
    await process_market_data(data)

await consumer.start(handle_message)

# Check if this instance is active
if consumer.is_active:
    print("I am the active consumer")
```

### 3. Batching Consumer

For high-throughput scenarios with aggregation:

```python
from src.batching_consumer import BatchingConsumer

consumer = BatchingConsumer(
    config,
    batch_size=1000,
    batch_timeout_ms=100,
)

async def handle_batch(batch: list[MarketData]):
    # Process batch of market data
    stats = BatchingConsumer.aggregate_by_symbol(batch)
    
    # Calculate VWAP per symbol
    for symbol, stat in stats.items():
        print(f"{symbol}: VWAP={stat.vwap}, High={stat.high}, Low={stat.low}")

await consumer.start(handle_batch)
```

### 4. Replay Consumer

For backtesting and historical analysis:

```python
from src.replay_consumer import ReplayConsumer
from datetime import datetime

consumer = ReplayConsumer(config)
await consumer.start()

# Replay all data
result = await consumer.replay_all(timeout_seconds=60)

# Replay specific time range
result = await consumer.replay_time_range(
    from_time=datetime(2024, 1, 15, 9, 30),
    to_time=datetime(2024, 1, 15, 10, 0),
    timeout_seconds=60,
)

# Replay specific symbols
result = await consumer.replay_symbols(
    {"AAPL", "GOOGL", "MSFT"},
    timeout_seconds=60,
)

# Access results
aapl_data = result.get_data_for_symbol("AAPL")
result.print_summary()
```

## Configuration

### Stream Settings

```python
from src.config import StreamConfig

config = StreamConfig(
    host="localhost",
    port=5552,
    username="guest",
    password="guest",
    stream_name="market-data",
    max_age_seconds=86400,           # Retain data for 24 hours
    max_length_bytes=10_000_000_000, # Max 10 GB
    max_segment_size_bytes=500_000_000,  # 500 MB per segment
)
```

## Market Data Model

```python
from src.models import MarketData

tick = MarketData(
    symbol="AAPL",
    bid_price=Decimal("185.50"),
    ask_price=Decimal("185.52"),
    bid_size=Decimal("500"),
    ask_size=Decimal("300"),
    last_price=Decimal("185.51"),
    last_size=Decimal("100"),
    volume=Decimal("5000000"),
    timestamp=datetime.now(timezone.utc),
    exchange="NASDAQ",
    sequence_number=12345,
)

# Computed properties
print(tick.spread)     # 0.02
print(tick.mid_price)  # 185.51

# Serialization
bytes_data = tick.to_bytes()
restored = MarketData.from_bytes(bytes_data)
```

## Performance Tips

1. **Use Batching**: The `BatchingConsumer` aggregates messages for better throughput

2. **Tune Buffer Sizes**: Adjust `batch_size` and `batch_timeout_ms` based on your latency requirements

3. **Parallel Processing**: Use `asyncio.gather()` to process batches concurrently

4. **Client-side Filtering**: Filter symbols client-side or use Super Streams for server-side partitioning

5. **Offset Tracking**: Use `subscriber_name` for automatic offset recovery after restarts

## Monitoring

Access RabbitMQ Management UI at http://localhost:15672

The Streams tab shows:
- Stream size and message count
- Publisher and consumer connections
- Message rates

## Cleanup

```bash
docker compose down -v
```

## Dependencies

Managed via [uv](https://docs.astral.sh/uv/):

- **rstream**: Official RabbitMQ Stream Python client
- **pydantic**: Data validation and serialization
- **orjson**: Fast JSON serialization

Install uv if you haven't already:
```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

## Next Steps

- **Super Streams**: For partitioned streams with server-side routing
- **Deduplication**: Add deduplication using message IDs
- **Compression**: Enable compression for lower network usage
- **TLS**: Configure TLS for production deployments
