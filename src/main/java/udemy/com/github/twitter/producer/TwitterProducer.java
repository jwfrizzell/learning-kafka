package udemy.com.github.twitter.producer;

import com.github.kafka.twitter.TwitterClient;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class TwitterProducer {
    private final Logger log = LoggerFactory.getLogger(TwitterProducer.class);

    public  TwitterProducer(){
    }

    public static void main(String[] args) {
        new TwitterProducer().run();
    }

    public  void run() {
        log.info("executing run method...");

        //Create twitter client
        log.info("creating twitter client...");
        BlockingQueue<String> msgQueue = new LinkedBlockingQueue<String>(10000);
        Client client = createTwitterClient(msgQueue);
        client.connect();

        //Create twitter producer
        // using cli run the below command.
        // kafka-topics.sh --bootstrap-server localhost:2181 --create --topic twitter_tweets_1 --partitions 6 --replication-factor 1
        // This created my topic with 6 partitions and 1 replication-factor.
        log.info("creating kafka producer...");
        String topic = "twitter_status_connect";

        org.apache.kafka.clients.producer.KafkaProducer<String,String> producer =
                new  org.apache.kafka.clients.producer.KafkaProducer<String,String>(getKafkaProperties());


        // add shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("stopping application...");
            log.info("stopping client...");
            log.info("closing kafka producer...");
            client.stop();
            producer.close();
            log.info("shutdown complete!");
        }));

        //loop to send tweets.
        log.info("iterate over tweets...");
        try {
            int key = 1;
            while (!client.isDone()) {
                String msg = (String) msgQueue.take();
                if(msg!=null){
                    producer.send(new ProducerRecord<>(topic, Integer.toString(key), msg), new Callback() {
                        @Override
                        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                            if(e != null){
                                log.error(String.format("Producer Tweet Error: %s", e.getMessage()));
                            } else {
                                log.info(String.format("Offset: %s", recordMetadata.offset()));
                                log.info(String.format("Topic: %s",recordMetadata.topic()));
                                log.info(String.format("Partition: %s",recordMetadata.partition()));
                            }
                        }
                    });
                }
                key++;
            }
        }
        catch (InterruptedException e){
            log.error(String.format("run() InterruptedException: %s", e.getMessage()));
        }
        finally {
            client.stop();
        }

    }

    private  Client createTwitterClient(BlockingQueue<String> queue){
        String key = System.getenv("TWITTER_CONSUMER_KEY");
        String secret = System.getenv("TWITTER_CONSUMER_SECRET");
        String token = System.getenv("TWITTER_TOKEN");
        String tokenSecret = System.getenv("TWITTER_TOKEN_SECRET");

        //Declare host to connect to, the endpoint, and authentication.
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        endpoint.trackTerms(Arrays.asList("bitcoin","BTX","ETH", "ethereum", "vitalik", "satoshi","president"));

        Authentication auth = new OAuth1(key, secret, token, tokenSecret);

        Client client = new ClientBuilder()
                .name("twitter_client")
                .hosts(Constants.STREAM_HOST)
                .authentication(auth)
                .endpoint(endpoint)
                .processor(new StringDelimitedProcessor(queue))
                .build();

       return client;
    }

    private Properties getKafkaProperties() {
        String serializerName = StringSerializer.class.getName();
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, serializerName);
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,serializerName);

        //Data consistency settings
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5");

        //Batch settings for high throughput at the expense of latency and cpu usage.
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"10");

        return properties;
    }
}
