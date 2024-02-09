package com.arcadedb.server.kafka;

import com.arcadedb.log.LogManager;
import com.raft.arcadedb.cdc.Message;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.serialization.StringSerializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import java.util.Properties;
import java.util.UUID;
import java.util.logging.Level;


public class Producer implements Callback {
    KafkaProducer<String, Message> kafkaProducer;
    String topicName;

    public Producer(String topicName) {
        this.topicName = topicName;
        this.kafkaProducer = createKafkaProducer();
    }

    public void send(Message message) {
        kafkaProducer.send(new ProducerRecord<>(this.topicName, null, message), this);
    }

    private KafkaProducer<String, Message> createKafkaProducer() {
        // Create properties for the Kafka producer
        Properties props = KafkaClientConfiguration.getKafkaClientConfiguration();
        
        // Set a unique client ID for tracking
        props.put(ProducerConfig.CLIENT_ID_CONFIG, "producer-arcadedb-" + UUID.randomUUID());
        
        // Configure serializers for keys and values
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class);

        return new KafkaProducer<>(props);
    }

    private boolean retriable(Exception exception) {
        return exception instanceof RetriableException;
    }

    @Override
    public void onCompletion(RecordMetadata metadata, Exception exception) {
        if (exception != null) {
            // If an exception occurred while sending the record
            LogManager.instance().log(this, Level.SEVERE, "Failed to send message to topic: %s", metadata.topic());

            if (!retriable(exception)) {
                exception.printStackTrace();
            }
        } else {
            LogManager.instance().log(this, Level.FINE, "Record sent to %s-%d with offset %d",
                    metadata.topic(), metadata.partition(), metadata.offset());
        }
    }

    public void flush() {
        LogManager.instance().log(this, Level.INFO, "Flushing messages to topic: %s",  this.topicName);
        this.kafkaProducer.flush();
    }
}
