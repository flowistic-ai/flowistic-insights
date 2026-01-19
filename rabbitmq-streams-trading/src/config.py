"""Configuration for RabbitMQ Stream connections."""

from dataclasses import dataclass, field


@dataclass
class StreamConfig:
    """Configuration for RabbitMQ Stream connections."""
    
    host: str = "localhost"
    port: int = 5552
    username: str = "guest"
    password: str = "guest"
    virtual_host: str = "/"
    stream_name: str = "market-data"
    max_age_seconds: int = 86400  # 24 hours
    max_length_bytes: int = 10_000_000_000  # 10 GB
    max_segment_size_bytes: int = 500_000_000  # 500 MB
    
    @classmethod
    def defaults(cls) -> "StreamConfig":
        """Create default configuration."""
        return cls()
