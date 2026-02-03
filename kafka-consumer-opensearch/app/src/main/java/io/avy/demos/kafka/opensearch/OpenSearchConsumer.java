package io.avy.demos.kafka.opensearch;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.impl.DefaultConnectionKeepAliveStrategy;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.core5.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import com.google.gson.JsonParser;
import org.opensearch.client.indices.CreateIndexRequest;
import org.opensearch.client.indices.GetIndexRequest;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class OpenSearchConsumer {

    public static RestHighLevelClient createOpenSearchClient(String connString) {

        // we build a URI from the connection string
        RestHighLevelClient restHighLevelClient;
        URI connUri = URI.create(connString);
        // extract login information if it exists
        String userInfo = connUri.getUserInfo();

        if (userInfo == null) {
            // REST client without security
            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getHost(), connUri.getPort())));

        } else {
            // REST client with security
            String[] auth = userInfo.split(":");

            BasicCredentialsProvider cp = new BasicCredentialsProvider();
            cp.setCredentials(new AuthScope(null, -1), new UsernamePasswordCredentials(auth[0], auth[1].toCharArray()));

            restHighLevelClient = new RestHighLevelClient(
                    RestClient.builder(new HttpHost(connUri.getScheme(), connUri.getHost(), connUri.getPort()))
                            .setHttpClientConfigCallback(
                                    httpAsyncClientBuilder -> httpAsyncClientBuilder.setDefaultCredentialsProvider(cp)
                                            .setKeepAliveStrategy(new DefaultConnectionKeepAliveStrategy())));

        }

        return restHighLevelClient;
    }

    private static KafkaConsumer<String, String> createkafkaConsumer(String bootstrapServer) {
        Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getName());
        log.info("Consumer!");

        // create producer properties
        Properties props = new Properties();
        String groupId = "my-opensearch-demo";
        // basic localhost setup.
        props.setProperty("bootstrap.servers", bootstrapServer);
        props.setProperty("key.deserializer", StringDeserializer.class.getName());
        props.setProperty("value.deserializer", StringDeserializer.class.getName());
        props.setProperty("group.id", groupId);
        props.setProperty("auto.offset.reset", "latest"); // none, earliest, latest

        return new KafkaConsumer<>(props);

    }

    private static String extractRecordId(String recordJson) {
        return JsonParser.parseString(recordJson)
                .getAsJsonObject()
                .get("meta")
                .getAsJsonObject()
                .get("id")
                .getAsString();
    }

    public static void main(String[] args) throws IOException {

        Logger log = LoggerFactory.getLogger(OpenSearchConsumer.class.getName());

        String connString = System.getenv("OPENSEARCH_URL");
        if (connString == null) {
            log.error("Set `OPENSEARCH_URL` env", new Exception());
        }

        String bootstrapServer = System.getenv("KAFKA_BROKER_HOST");
        if (bootstrapServer == null) {
            log.error("Set `KAFKA_BROKER_HOST` env", new Exception());
        }

        String topic = "wikimedia-recentchanges";

        // create opensearch client
        RestHighLevelClient openSearchClient = createOpenSearchClient(connString);

        // create kafka client
        KafkaConsumer<String, String> consumer = createkafkaConsumer(bootstrapServer);

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

        // create index on osh client
        try (openSearchClient; consumer) {
            boolean indexExists = openSearchClient.indices().exists(new GetIndexRequest("wikimedia"),
                    RequestOptions.DEFAULT);

            if (!indexExists) {
                CreateIndexRequest createidxReq = new CreateIndexRequest("wikimedia");
                openSearchClient.indices().create(createidxReq, RequestOptions.DEFAULT);
                log.info("Wikiemdia Index has been created!");
            } else {
                log.info("Wikiemdia Index already exists!");

            }

            // subscribe to a poic
            consumer.subscribe(Collections.singleton(topic));

            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(3000));

                int recordCount = records.count();
                log.info("Received " + recordCount + " records");

                for (ConsumerRecord<String, String> record : records) {
                    // send record to opensearch
                    try {
                        String id = extractRecordId(record.value());
                        IndexRequest idxReq = new IndexRequest("wikimedia")
                                .source(record.value(), XContentType.JSON)
                                .id(id);
                        IndexResponse resp = openSearchClient.index(idxReq, RequestOptions.DEFAULT);
                        log.info("resp: " + resp.getId());
                    } catch (Exception e) {
                    }
                }
            }
        } catch (WakeupException e) {
            log.info("Consumer is shutting down..");
        } catch (Exception e) {
            log.error("Unexpected exception", e);
        } finally {
            consumer.close(); // commits the offsets
        }

        // close
        // openSearchClient.close();

    }

}
