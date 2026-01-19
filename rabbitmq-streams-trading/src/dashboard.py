"""
Streamlit Dashboard for RabbitMQ Streams Market Data

Visualizes real-time production and consumption of market data messages.
"""

import asyncio
import threading
import time
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from queue import Queue, Empty
from typing import Dict, List, Optional

import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import streamlit as st

from src.config import StreamConfig
from src.models import MarketData, SymbolStats


@dataclass
class ConsumerState:
    """State for a single consumer."""
    name: str
    message_count: int = 0
    messages_per_second: float = 0.0
    last_offset: int = -1
    is_active: bool = False
    recent_symbols: Dict[str, int] = field(default_factory=lambda: defaultdict(int))
    start_time: Optional[datetime] = None


@dataclass
class ProducerState:
    """State for the producer."""
    message_count: int = 0
    messages_per_second: float = 0.0
    is_running: bool = False
    start_time: Optional[datetime] = None


class StreamingState:
    """Shared state between async tasks and Streamlit."""
    
    def __init__(self):
        self.producer = ProducerState()
        self.consumers: Dict[str, ConsumerState] = {}
        self.message_queue: Queue = Queue(maxsize=1000)
        self.stats_by_symbol: Dict[str, SymbolStats] = {}
        self.price_history: Dict[str, List[tuple]] = defaultdict(list)
        self.throughput_history: List[tuple] = []
        self.lock = threading.Lock()
    
    def add_consumer(self, name: str) -> ConsumerState:
        with self.lock:
            if name not in self.consumers:
                self.consumers[name] = ConsumerState(name=name)
            return self.consumers[name]
    
    def remove_consumer(self, name: str):
        with self.lock:
            if name in self.consumers:
                del self.consumers[name]
    
    def update_producer(self, count: int, rate: float, running: bool):
        with self.lock:
            self.producer.message_count = count
            self.producer.messages_per_second = rate
            self.producer.is_running = running
            if running and self.producer.start_time is None:
                self.producer.start_time = datetime.now(timezone.utc)
    
    def update_consumer(self, name: str, count: int, rate: float, offset: int):
        with self.lock:
            if name in self.consumers:
                self.consumers[name].message_count = count
                self.consumers[name].messages_per_second = rate
                self.consumers[name].last_offset = offset
                self.consumers[name].is_active = True
    
    def record_message(self, data: MarketData, consumer_name: str):
        with self.lock:
            # Update symbol stats
            if data.symbol not in self.stats_by_symbol:
                self.stats_by_symbol[data.symbol] = SymbolStats(data.symbol)
            self.stats_by_symbol[data.symbol].update(data)
            
            # Update consumer's recent symbols
            if consumer_name in self.consumers:
                self.consumers[consumer_name].recent_symbols[data.symbol] += 1
            
            # Record price history (keep last 100 per symbol)
            self.price_history[data.symbol].append((
                data.timestamp,
                float(data.last_price),
                float(data.bid_price),
                float(data.ask_price),
            ))
            if len(self.price_history[data.symbol]) > 100:
                self.price_history[data.symbol] = self.price_history[data.symbol][-100:]
    
    def record_throughput(self, producer_rate: float, consumer_rates: Dict[str, float]):
        with self.lock:
            self.throughput_history.append((
                datetime.now(timezone.utc),
                producer_rate,
                consumer_rates.copy(),
            ))
            if len(self.throughput_history) > 60:
                self.throughput_history = self.throughput_history[-60:]


# Initialize session state
if "state" not in st.session_state:
    st.session_state.state = StreamingState()
if "producer_thread" not in st.session_state:
    st.session_state.producer_thread = None
if "consumer_threads" not in st.session_state:
    st.session_state.consumer_threads = {}
if "stop_flags" not in st.session_state:
    st.session_state.stop_flags = {}


def run_producer_async(state: StreamingState, count: int, delay_ms: float, stop_flag: threading.Event):
    """Run producer in a separate thread with its own event loop."""
    async def produce():
        from src.producer import MarketDataProducer
        config = StreamConfig.defaults()
        producer = MarketDataProducer(config)
        
        try:
            await producer.start()
            state.update_producer(0, 0, True)
            
            start_time = time.time()
            message_count = 0
            
            for i in range(count):
                if stop_flag.is_set():
                    break
                
                tick = producer._generate_tick()
                await producer.publish(tick)
                message_count += 1
                
                elapsed = time.time() - start_time
                rate = message_count / elapsed if elapsed > 0 else 0
                state.update_producer(message_count, rate, True)
                
                if delay_ms > 0:
                    await asyncio.sleep(delay_ms / 1000)
            
            state.update_producer(message_count, 0, False)
            
        finally:
            await producer.stop()
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(produce())
    finally:
        loop.close()


def run_consumer_async(state: StreamingState, consumer_name: str, stop_flag: threading.Event, from_first: bool = False):
    """Run consumer in a separate thread with its own event loop."""
    async def consume():
        from rstream import Consumer, ConsumerOffsetSpecification, OffsetType, AMQPMessage, MessageContext, amqp_decoder
        from src.models import MarketData
        
        config = StreamConfig.defaults()
        consumer_state = state.add_consumer(consumer_name)
        consumer_state.start_time = datetime.now(timezone.utc)
        consumer_state.is_active = True
        
        # Choose offset based on user selection
        offset_type = OffsetType.FIRST if from_first else OffsetType.NEXT
        
        consumer = Consumer(
            host=config.host,
            port=config.port,
            username=config.username,
            password=config.password,
            vhost=config.virtual_host,
        )
        
        try:
            await consumer.start()
            
            message_count = 0
            start_time = time.time()
            
            async def on_message(msg, context: MessageContext):
                nonlocal message_count
                if stop_flag.is_set():
                    return
                
                try:
                    # msg.body contains our JSON data (already decoded from AMQP)
                    data = MarketData.from_bytes(msg.body)
                    message_count += 1
                    
                    elapsed = time.time() - start_time
                    rate = message_count / elapsed if elapsed > 0 else 0
                    
                    state.update_consumer(consumer_name, message_count, rate, context.offset)
                    state.record_message(data, consumer_name)
                    
                except Exception as e:
                    pass
            
            await consumer.subscribe(
                stream=config.stream_name,
                callback=on_message,
                offset_specification=ConsumerOffsetSpecification(offset_type, None),
                decoder=amqp_decoder,  # Decode AMQP messages
            )
            
            # Keep running until stopped
            while not stop_flag.is_set():
                await asyncio.sleep(0.1)
            
        finally:
            await consumer.close()
            state.consumers[consumer_name].is_active = False
    
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(consume())
    finally:
        loop.close()


def start_producer(count: int, delay_ms: float):
    """Start the producer in a background thread."""
    if st.session_state.producer_thread is not None and st.session_state.producer_thread.is_alive():
        return
    
    stop_flag = threading.Event()
    st.session_state.stop_flags["producer"] = stop_flag
    
    thread = threading.Thread(
        target=run_producer_async,
        args=(st.session_state.state, count, delay_ms, stop_flag),
        daemon=True,
    )
    thread.start()
    st.session_state.producer_thread = thread


def stop_producer():
    """Stop the producer."""
    if "producer" in st.session_state.stop_flags:
        st.session_state.stop_flags["producer"].set()


def start_consumer(name: str, from_first: bool = False):
    """Start a consumer in a background thread."""
    if name in st.session_state.consumer_threads:
        if st.session_state.consumer_threads[name].is_alive():
            return
    
    stop_flag = threading.Event()
    st.session_state.stop_flags[f"consumer_{name}"] = stop_flag
    
    thread = threading.Thread(
        target=run_consumer_async,
        args=(st.session_state.state, name, stop_flag, from_first),
        daemon=True,
    )
    thread.start()
    st.session_state.consumer_threads[name] = thread


def stop_consumer(name: str):
    """Stop a consumer."""
    flag_key = f"consumer_{name}"
    if flag_key in st.session_state.stop_flags:
        st.session_state.stop_flags[flag_key].set()


def stop_all():
    """Stop all producers and consumers."""
    for flag in st.session_state.stop_flags.values():
        flag.set()


# Streamlit UI
st.set_page_config(
    page_title="RabbitMQ Streams Market Data",
    page_icon="📈",
    layout="wide",
)

st.title("📈 RabbitMQ Streams Market Data Dashboard")
st.markdown("Real-time visualization of market data streaming with multiple consumers")

# Sidebar controls
with st.sidebar:
    st.header("🎛️ Controls")
    
    # RabbitMQ Management link
    st.markdown("🐰 [RabbitMQ Management UI](http://localhost:15672) (guest/guest)")
    
    st.divider()
    
    st.subheader("Producer")
    producer_count = st.number_input("Messages to produce", min_value=100, max_value=1_000_000, value=10_000, step=1000)
    producer_delay = st.slider("Delay between messages (ms)", min_value=0, max_value=100, value=1)
    
    col1, col2 = st.columns(2)
    with col1:
        if st.button("▶️ Start Producer", width='stretch'):
            start_producer(producer_count, producer_delay)
    with col2:
        if st.button("⏹️ Stop Producer", width='stretch'):
            stop_producer()
    
    st.divider()
    
    st.subheader("Consumers")
    num_consumers = st.number_input("Number of consumers", min_value=1, max_value=5, value=2)
    
    # Offset choice
    offset_choice = st.radio(
        "Start consuming from",
        ["New messages only", "All messages (replay)"],
        index=0,
        help="'New messages only' starts from NEXT offset. 'All messages' replays from the beginning."
    )
    consume_from_first = offset_choice == "All messages (replay)"
    
    col1, col2 = st.columns(2)
    with col1:
        if st.button("▶️ Start All", width='stretch'):
            for i in range(num_consumers):
                start_consumer(f"Consumer-{i+1}", consume_from_first)
    with col2:
        if st.button("⏹️ Stop All", width='stretch'):
            stop_all()
    
    st.divider()
    
    # Individual consumer controls
    for i in range(num_consumers):
        name = f"Consumer-{i+1}"
        col1, col2 = st.columns(2)
        with col1:
            if st.button(f"▶️ {name}", key=f"start_{name}", width='stretch'):
                start_consumer(name, consume_from_first)
        with col2:
            if st.button(f"⏹️", key=f"stop_{name}", width='stretch'):
                stop_consumer(name)

# Main content
state = st.session_state.state

# Metrics row
st.header("📊 Live Metrics")
col1, col2, col3, col4, col5 = st.columns(5)

with col1:
    st.metric(
        "Producer Messages",
        f"{state.producer.message_count:,}",
        f"{state.producer.messages_per_second:.0f} msg/s" if state.producer.is_running else "Stopped",
    )

with col2:
    # Stream total = max offset + 1 from any consumer (offsets are 0-indexed)
    max_offset = max((c.last_offset for c in state.consumers.values()), default=-1)
    stream_total = max_offset + 1 if max_offset >= 0 else 0
    st.metric("Stream Total", f"{stream_total:,}", help="Total messages in the stream")

with col3:
    total_consumed = sum(c.message_count for c in state.consumers.values())
    st.metric("Total Consumed", f"{total_consumed:,}")

with col4:
    active_consumers = sum(1 for c in state.consumers.values() if c.is_active)
    st.metric("Active Consumers", active_consumers)

with col5:
    total_rate = sum(c.messages_per_second for c in state.consumers.values())
    st.metric("Total Consume Rate", f"{total_rate:.0f} msg/s")

# Consumer status
st.header("🔄 Consumer Status")
if state.consumers:
    consumer_data = []
    for name, consumer in state.consumers.items():
        consumer_data.append({
            "Consumer": name,
            "Status": "🟢 Active" if consumer.is_active else "🔴 Stopped",
            "Messages": consumer.message_count,
            "Rate (msg/s)": f"{consumer.messages_per_second:.0f}",
            "Last Offset": consumer.last_offset,
            "Top Symbols": ", ".join(
                f"{k}({v})" for k, v in sorted(
                    consumer.recent_symbols.items(),
                    key=lambda x: x[1],
                    reverse=True
                )[:3]
            ) if consumer.recent_symbols else "-",
        })
    
    df = pd.DataFrame(consumer_data)
    st.dataframe(df, width='stretch', hide_index=True)
else:
    st.info("No consumers started yet. Use the sidebar to start consumers.")

# Charts
col1, col2 = st.columns(2)

with col1:
    st.subheader("📈 Symbol Statistics")
    if state.stats_by_symbol:
        symbol_data = []
        for symbol, stats in state.stats_by_symbol.items():
            symbol_data.append({
                "Symbol": symbol,
                "Ticks": stats.tick_count,
                "VWAP": float(stats.vwap),
                "High": float(stats.high),
                "Low": float(stats.low),
                "Last": float(stats.last_price) if stats.last_price else 0,
            })
        
        df = pd.DataFrame(symbol_data).sort_values("Ticks", ascending=False)
        
        fig = px.bar(
            df,
            x="Symbol",
            y="Ticks",
            color="VWAP",
            title="Ticks per Symbol",
            color_continuous_scale="Viridis",
        )
        st.plotly_chart(fig, width='stretch')
    else:
        st.info("No data yet. Start the producer and consumers.")

with col2:
    st.subheader("💹 Price Trends")
    if state.price_history:
        # Get top 3 symbols by tick count
        top_symbols = sorted(
            state.stats_by_symbol.items(),
            key=lambda x: x[1].tick_count,
            reverse=True
        )[:3]
        
        fig = go.Figure()
        
        for symbol, _ in top_symbols:
            if symbol in state.price_history:
                history = state.price_history[symbol]
                times = [h[0] for h in history]
                prices = [h[1] for h in history]
                
                fig.add_trace(go.Scatter(
                    x=times,
                    y=prices,
                    mode="lines",
                    name=symbol,
                ))
        
        fig.update_layout(
            title="Recent Price Movements (Top 3 Symbols)",
            xaxis_title="Time",
            yaxis_title="Price",
            height=400,
        )
        st.plotly_chart(fig, width='stretch')
    else:
        st.info("No price data yet.")

# Consumer comparison chart
st.subheader("⚡ Consumer Throughput Comparison")
if state.consumers:
    consumer_rates = {
        name: consumer.messages_per_second
        for name, consumer in state.consumers.items()
    }
    
    fig = px.bar(
        x=list(consumer_rates.keys()),
        y=list(consumer_rates.values()),
        labels={"x": "Consumer", "y": "Messages/Second"},
        title="Current Throughput by Consumer",
        color=list(consumer_rates.values()),
        color_continuous_scale="Blues",
    )
    st.plotly_chart(fig, width='stretch')

# Auto-refresh
st.markdown("---")
st.caption("Dashboard auto-refreshes every second")

# Auto-refresh using a placeholder
placeholder = st.empty()
time.sleep(1)
st.rerun()
