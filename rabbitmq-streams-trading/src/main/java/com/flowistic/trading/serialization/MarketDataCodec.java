package com.flowistic.trading.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.flowistic.trading.model.MarketData;
import com.rabbitmq.stream.Codec;
import com.rabbitmq.stream.Message;
import com.rabbitmq.stream.MessageBuilder;
import com.rabbitmq.stream.codecs.SimpleCodec;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Codec for serializing/deserializing MarketData to/from RabbitMQ Stream messages.
 */
public class MarketDataCodec {
    
    private static final ObjectMapper objectMapper = new ObjectMapper()
        .registerModule(new JavaTimeModule())
        .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
    
    private final Codec codec;
    
    public MarketDataCodec() {
        this.codec = new SimpleCodec();
    }
    
    public Codec getCodec() {
        return codec;
    }
    
    /**
     * Serialize MarketData to a RabbitMQ Stream message.
     */
    public Message encode(MarketData marketData) {
        try {
            byte[] jsonBytes = objectMapper.writeValueAsBytes(marketData);
            return codec.messageBuilder()
                .addData(jsonBytes)
                .properties()
                    .messageId(marketData.sequenceNumber())
                    .correlationId(marketData.symbol())
                    .contentType("application/json")
                    .creationTime(marketData.timestamp().toEpochMilli())
                .messageBuilder()
                .applicationProperties()
                    .entry("symbol", marketData.symbol())
                    .entry("exchange", marketData.exchange())
                .messageBuilder()
                .build();
        } catch (IOException e) {
            throw new RuntimeException("Failed to serialize MarketData", e);
        }
    }
    
    /**
     * Deserialize a RabbitMQ Stream message to MarketData.
     */
    public MarketData decode(Message message) {
        try {
            byte[] data = message.getBodyAsBinary();
            return objectMapper.readValue(data, MarketData.class);
        } catch (IOException e) {
            throw new RuntimeException("Failed to deserialize MarketData", e);
        }
    }
    
    /**
     * Get the ObjectMapper for external use.
     */
    public static ObjectMapper getObjectMapper() {
        return objectMapper;
    }
}
