package org.appledog.base.kafka.services;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.LongSerializer;
import org.appledog.utils.PropertiesFileReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Properties;

public class SparkKafkaProducer implements Serializable {
    private static final Logger logger = LoggerFactory.getLogger(SparkKafkaProducer.class.getSimpleName());
    private transient KafkaProducer<Long, byte[]> producer = null;

    public KafkaProducer<Long, byte[]> init(String configPath) {
        Properties configFile = PropertiesFileReader.readConfig(configPath);
        logger.info("starting create kafka producer");
        String kafkaServer = configFile.getProperty("kafka.server");
        Properties kafkaProperties = new Properties();
        kafkaProperties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        logger.info("kafka bootstrap server {}", kafkaServer);
        kafkaProperties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, LongSerializer.class.getName());
        kafkaProperties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
        kafkaProperties.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        Runtime.getRuntime().addShutdownHook(new Thread(this::close));
        return new KafkaProducer<>(kafkaProperties);
    }

    public void close() {
        producer.close();
    }
}
