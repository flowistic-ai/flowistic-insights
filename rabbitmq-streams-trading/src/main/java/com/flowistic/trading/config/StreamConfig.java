package com.flowistic.trading.config;

import java.time.Duration;

/**
 * Configuration for RabbitMQ Stream connections.
 */
public record StreamConfig(
    String host,
    int port,
    String username,
    String password,
    String virtualHost,
    String streamName,
    Duration maxAge,
    long maxLengthBytes,
    int maxSegmentSizeBytes
) {
    
    public static final String DEFAULT_STREAM_NAME = "market-data";
    
    public static StreamConfig defaults() {
        return new StreamConfig(
            "localhost",
            5552,
            "guest",
            "guest",
            "/",
            DEFAULT_STREAM_NAME,
            Duration.ofHours(24),           // Keep data for 24 hours
            10_000_000_000L,                // 10 GB max
            500_000_000                     // 500 MB segments
        );
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    public static class Builder {
        private String host = "localhost";
        private int port = 5552;
        private String username = "guest";
        private String password = "guest";
        private String virtualHost = "/";
        private String streamName = DEFAULT_STREAM_NAME;
        private Duration maxAge = Duration.ofHours(24);
        private long maxLengthBytes = 10_000_000_000L;
        private int maxSegmentSizeBytes = 500_000_000;
        
        public Builder host(String host) {
            this.host = host;
            return this;
        }
        
        public Builder port(int port) {
            this.port = port;
            return this;
        }
        
        public Builder username(String username) {
            this.username = username;
            return this;
        }
        
        public Builder password(String password) {
            this.password = password;
            return this;
        }
        
        public Builder virtualHost(String virtualHost) {
            this.virtualHost = virtualHost;
            return this;
        }
        
        public Builder streamName(String streamName) {
            this.streamName = streamName;
            return this;
        }
        
        public Builder maxAge(Duration maxAge) {
            this.maxAge = maxAge;
            return this;
        }
        
        public Builder maxLengthBytes(long maxLengthBytes) {
            this.maxLengthBytes = maxLengthBytes;
            return this;
        }
        
        public Builder maxSegmentSizeBytes(int maxSegmentSizeBytes) {
            this.maxSegmentSizeBytes = maxSegmentSizeBytes;
            return this;
        }
        
        public StreamConfig build() {
            return new StreamConfig(
                host, port, username, password, virtualHost,
                streamName, maxAge, maxLengthBytes, maxSegmentSizeBytes
            );
        }
    }
}
