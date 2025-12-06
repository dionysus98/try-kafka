package io.avy.demos.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.launchdarkly.eventsource.MessageEvent;
import com.launchdarkly.eventsource.background.BackgroundEventHandler;

public class wikimediaChangesHandler implements BackgroundEventHandler {
    public final Logger log = LoggerFactory.getLogger(wikimediaChangesHandler.class.getSimpleName());

    KafkaProducer<String, String> producer;
    String topic;

    public wikimediaChangesHandler(KafkaProducer<String, String> producer, String topic) {
        this.producer = producer;
        this.topic = topic;
    }

    @Override
    public void onOpen() {
        // nothing
    }

    @Override
    public void onClosed() {
        producer.close();

    }

    @Override
    public void onMessage(String event, MessageEvent messageEvent) {
        log.info(messageEvent.getData());

        // async
        producer.send(new ProducerRecord<String, String>(topic, messageEvent.getData()));

    }

    @Override
    public void onComment(String comment) {

    }

    @Override
    public void onError(Throwable t) {
        log.error("Error while reading stream", t);
    }

}
