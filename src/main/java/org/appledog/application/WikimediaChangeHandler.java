package org.appledog.application;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.appledog.base.http.services.EventHandlerServices;
import org.appledog.base.kafka.services.SparkKafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.concurrent.TimeUnit;

public class WikimediaChangeHandler {

    private static final Logger logger = LoggerFactory.getLogger(WikimediaChangeHandler.class.getSimpleName());
    public static void main(String[] args) throws InterruptedException {
        String configPath = args.length == 0 ? System.getProperty("user.dir") + "/config/application.properties" : args[0];
        KafkaProducer<Long, byte[]> producer = new SparkKafkaProducer().init(configPath);

        String topic = "wikimedia.recentchange";
        EventHandler eventHandler = new EventHandlerServices(producer, topic);
        String url = "https://stream.wikimedia.org/v2/stream/recentchange";
        EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(url));
        EventSource eventSource = builder.build();
        eventSource.start();

        TimeUnit.MINUTES.sleep(1);
    }
}
