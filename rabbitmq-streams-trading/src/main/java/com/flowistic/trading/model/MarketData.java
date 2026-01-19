package com.flowistic.trading.model;

import java.math.BigDecimal;
import java.time.Instant;

/**
 * Market data tick representing a price update for a financial instrument.
 */
public record MarketData(
    String symbol,
    BigDecimal bidPrice,
    BigDecimal askPrice,
    BigDecimal bidSize,
    BigDecimal askSize,
    BigDecimal lastPrice,
    BigDecimal lastSize,
    BigDecimal volume,
    Instant timestamp,
    String exchange,
    long sequenceNumber
) {
    
    public BigDecimal spread() {
        return askPrice.subtract(bidPrice);
    }
    
    public BigDecimal midPrice() {
        return bidPrice.add(askPrice).divide(BigDecimal.valueOf(2));
    }
    
    public static Builder builder() {
        return new Builder();
    }
    
    public static class Builder {
        private String symbol;
        private BigDecimal bidPrice;
        private BigDecimal askPrice;
        private BigDecimal bidSize;
        private BigDecimal askSize;
        private BigDecimal lastPrice;
        private BigDecimal lastSize;
        private BigDecimal volume;
        private Instant timestamp;
        private String exchange;
        private long sequenceNumber;
        
        public Builder symbol(String symbol) {
            this.symbol = symbol;
            return this;
        }
        
        public Builder bidPrice(BigDecimal bidPrice) {
            this.bidPrice = bidPrice;
            return this;
        }
        
        public Builder askPrice(BigDecimal askPrice) {
            this.askPrice = askPrice;
            return this;
        }
        
        public Builder bidSize(BigDecimal bidSize) {
            this.bidSize = bidSize;
            return this;
        }
        
        public Builder askSize(BigDecimal askSize) {
            this.askSize = askSize;
            return this;
        }
        
        public Builder lastPrice(BigDecimal lastPrice) {
            this.lastPrice = lastPrice;
            return this;
        }
        
        public Builder lastSize(BigDecimal lastSize) {
            this.lastSize = lastSize;
            return this;
        }
        
        public Builder volume(BigDecimal volume) {
            this.volume = volume;
            return this;
        }
        
        public Builder timestamp(Instant timestamp) {
            this.timestamp = timestamp;
            return this;
        }
        
        public Builder exchange(String exchange) {
            this.exchange = exchange;
            return this;
        }
        
        public Builder sequenceNumber(long sequenceNumber) {
            this.sequenceNumber = sequenceNumber;
            return this;
        }
        
        public MarketData build() {
            return new MarketData(
                symbol, bidPrice, askPrice, bidSize, askSize,
                lastPrice, lastSize, volume, timestamp, exchange, sequenceNumber
            );
        }
    }
}
