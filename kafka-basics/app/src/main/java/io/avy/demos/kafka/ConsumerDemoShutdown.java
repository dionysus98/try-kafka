package io.avy.demos.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerDemoShutdown {

    public static final Logger log = LoggerFactory.getLogger(ConsumerDemoShutdown.class.getSimpleName());

    public static void main(String[] args) throws Exception {
        log.info("Consumer!");

        // create producer properties
        Properties props = new Properties();
        String bootstrapServer = System.getenv("KAFKA_BROKER_HOST");
        if (bootstrapServer == null) {
            log.error("Set `KAFKA_BROKER_HOST` env", new Exception());
        }

        String groupId = "my-java-app";
        String topic = "demo_java";

        // basic localhost setup.
        props.setProperty("bootstrap.servers", bootstrapServer);
        props.setProperty("key.deserializer", StringDeserializer.class.getName());
        props.setProperty("value.deserializer", StringDeserializer.class.getName());
        props.setProperty("group.id", groupId);
        props.setProperty("auto.offset.reset", "earliest"); // none, earliest, latest

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // get a ref to main thread;
        final Thread mainThread = Thread.currentThread();

        // add shutdown hook.
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                log.info("Detected shutdown, exit by consumer.wakeup()..");
                consumer.wakeup();

                // join the main thread.
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        });

        try {
            // subscrive to a topic
            consumer.subscribe(Arrays.asList(topic));
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key: " + record.key() + ", value: " + record.value());
                    log.info("partition: " + record.partition() + ", offset: " + record.offset());
                }
            }

        } catch (WakeupException e) {
            log.info("Consumer is shutting down..");
        } catch (Exception e) {
            log.error("Unexpected exception", e);
        } finally {
            consumer.close(); // commits the offsets
        }

    }
}
