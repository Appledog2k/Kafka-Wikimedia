package org.appledog.base.http.services;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EventHandlerServices implements EventHandler {
    private static final Logger log = LoggerFactory.getLogger(EventHandlerServices.class);
    KafkaProducer<Long, byte[]> producer;
    String topic;

    public EventHandlerServices(KafkaProducer<Long, byte[]> producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }
    @Override
    public void onOpen() throws Exception {

    }

    @Override
    public void onClosed() throws Exception {

    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) throws Exception {
        log.info(messageEvent.getData());
        producer.send(new ProducerRecord<>(topic, System.currentTimeMillis(), messageEvent.getData().getBytes()));
    }

    @Override
    public void onComment(String comment) throws Exception {

    }

    @Override
    public void onError(Throwable t) {
        log.error(t.getMessage());
    }
}
