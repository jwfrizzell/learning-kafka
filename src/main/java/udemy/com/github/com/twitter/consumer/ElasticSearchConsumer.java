package udemy.com.github.com.twitter.consumer;

import jdk.nashorn.internal.parser.JSONParser;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.nio.client.HttpAsyncClientBuilder;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

import com.google.gson.Gson;

public class ElasticSearchConsumer {
    private static final Logger log = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

    /***
     * Main entry point.
     * Consumer Kafka topic and store information in the ElasticSearch.
     * @param args
     */
    public static void main(String[] args) throws IOException {
        RestHighLevelClient client = createClient();

        KafkaConsumer<String,String> consumer = createConsumer("twitter_tweets");

        boolean run = true;
        while(run) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            if (records.count() >0) {
                BulkRequest bulk = new BulkRequest();

                log.info(String.format("Record Count: %s", records.count()));
                for (ConsumerRecord<String, String> record : records) {

                    String id = extractIDFromTweet(record.value());
                    if (id == "0"){
                        log.info("Unable to get a unique id for current tweet");
                        continue;
                    }
                    IndexRequest request = new IndexRequest("twitter")
                            .id(id)
                            .source(record.value(), XContentType.JSON);

                    bulk.add(request);
                }
                BulkResponse response = client.bulk(bulk, RequestOptions.DEFAULT);

                //Commit reads.
                log.info("Committing Records...");
                consumer.commitSync();
                log.info("Offsets have been committed...");
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    log.error(String.format("InterruptedException Error: %s", e.getMessage()));
                    run = false;
                    break;
                }
            }
        }

        //Close client
        client.close();
    }

    private static String extractIDFromTweet(String record){
        try {
            Map<String, String> data = new Gson().fromJson(record, Map.class);
            return data.get("id_str");
        }catch (Exception e){
            return "0";
        }

    }

    private static KafkaConsumer<String, String> createConsumer(String _topic){
        String bootstrapServer = "localhost:9092";
        String groupID = "kafka-demo-elasticsearch";
        String offset = "earliest";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupID);
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,offset);
        props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"false");
        props.setProperty(ConsumerConfig.MAX_POLL_RECORDS_CONFIG,"25");

        KafkaConsumer consumer =   new KafkaConsumer<String, String>(props);
        consumer.subscribe(Arrays.asList(_topic));

        return consumer;
    }

    public static RestHighLevelClient createClient() {
        String hostname = "kafka-course-7442174698.us-east-1.bonsaisearch.net";
        String username = System.getenv("BONSAI_KAFKA_ACCESS_KEY");
        String password = System.getenv("BONSAI_KAFKA_ACCESS_SECRET");

        // Done do this if you run a local elastic search
        final CredentialsProvider credProvider = new BasicCredentialsProvider();
        credProvider.setCredentials(AuthScope.ANY, new UsernamePasswordCredentials(username, password));

        RestClientBuilder builder = RestClient.builder(
                new HttpHost(hostname, 443, "https"))
                .setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
                    @Override
                    public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
                        return httpAsyncClientBuilder.setDefaultCredentialsProvider(credProvider);
                    }
                });
        return new RestHighLevelClient(builder);
    }
}


/*
Command to reset offsets to earliest time for topic twitter_tweets
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group kafka-demo-elasticsearch --reset-offsets --to-earliest --topic twitter_tweets

Kafka Connect Confluent
https://www.confluent.io/hub/

Bonsaid URL:
https://app.bonsai.io/clusters/kafka-course-7442174698/console


 */

