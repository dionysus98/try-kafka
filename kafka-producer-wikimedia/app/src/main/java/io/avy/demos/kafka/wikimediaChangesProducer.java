package io.avy.demos.kafka;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.launchdarkly.eventsource.EventSource;
import com.launchdarkly.eventsource.HttpConnectStrategy;
import com.launchdarkly.eventsource.StreamException;
import com.launchdarkly.eventsource.background.BackgroundEventSource;

public class wikimediaChangesProducer {
    public static final Logger log = LoggerFactory.getLogger(wikimediaChangesProducer.class.getSimpleName());

    public static void main(String[] args) throws StreamException, InterruptedException {
        log.info("In Wikimedia Producer");

        String bootstrapServer = System.getenv("KAFKA_BROKER_HOST");
        if (bootstrapServer == null) {
            log.error("Set `KAFKA_BROKER_HOST` env", new Exception());
        }

        String wikimedia_url = System.getenv("STREAM_WIKIMEDIA_URL");
        if (bootstrapServer == null) {
            log.error("Set `STREAM_WIKIMEDIA_URL` env", new Exception());
        }

        log.info("wikimedia url: " + wikimedia_url);

        Properties props = new Properties();
        props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
        props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // high throughput producer config.
        props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG,
                Integer.toString(32 * 1024));
        props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

        // create producer:
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        String topic = "wikimedia-recentchanges";

        wikimediaChangesHandler eventhandler = new wikimediaChangesHandler(producer, topic);

        BackgroundEventSource.Builder builder = new BackgroundEventSource.Builder(eventhandler, new EventSource.Builder(
                HttpConnectStrategy.http(URI.create(wikimedia_url)).header("User-Agent",
                        "my-kafka-producer/1.0(contact: admin@ourcompany.com)")));

        BackgroundEventSource eventSource = builder.build();

        // start producer in a different thread;
        eventSource.start();

        // produce for 10min and block
        TimeUnit.MINUTES.sleep(10);

    }
}
