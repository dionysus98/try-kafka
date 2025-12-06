package io.avy.demos.kafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
// import org.apache.kafka.clients.producer.RoundRobinPartitioner;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemoKeys {

    public static final Logger log = LoggerFactory.getLogger(ProducerDemoKeys.class.getSimpleName());

    public static void main(String[] args) throws Exception {

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
        props.setProperty("batch.size", "400");
        // props.setProperty("partitioner.class",
        // RoundRobinPartitioner.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        for (int j = 0; j < 10; j++) {
            for (int i = 0; i < 10; i++) {
                String topic = "demo_java";
                String key = "id_" + i;
                String value = "Hello world " + i;

                // create producer record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topic, key, value);

                // send data
                producer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception e) {
                        if (e == null) {
                            log.info("\nreceived new metadata" +
                                    "\nTopic: " + metadata.topic() +
                                    "\nkey: " + key +
                                    "\nPartition: " + metadata.partition()
                            // +
                            // "\nOffset: " + metadata.offset()
                            // "\nTimestamp: " + metadata.timestamp()
                            );
                        } else {
                            log.error("Error while producing", e);
                        }
                    }

                });
            }

            Thread.sleep(500);
        }
        // flush & close producer
        producer.flush(); // send all data and block until done (sync)
        producer.close(); // would call flush anyway

    }
}
