package io.avy.demos.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemo {

    public static final Logger log = LoggerFactory.getLogger(ProducerDemo.class.getSimpleName());

    public static void main(String[] args) throws Exception {
        log.info("Hello!");

        // create producer properties
        Properties props = new Properties();
        String bootstrapServer = System.getenv("KAFKA_BROKER_HOST");
        if (bootstrapServer == null) {
            log.error("Set `KAFKA_BROKER_HOST` env", new Exception());
        }

        // basic localhost setup.
        props.setProperty("bootstrap.servers", bootstrapServer);
        props.setProperty("key.serializer", StringSerializer.class.getName());
        props.setProperty("value.serializer", StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        // create producer record
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "Hello world");

        // send data
        producer.send(producerRecord);

        // flush & close producer
        producer.flush(); // send all data and block until done (sync)
        producer.close(); // would call flush anyway

    }
}
