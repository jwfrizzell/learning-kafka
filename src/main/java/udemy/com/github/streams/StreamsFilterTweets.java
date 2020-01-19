package udemy.com.github.streams;

import com.google.gson.Gson;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class StreamsFilterTweets {
    private static final Logger log = LoggerFactory.getLogger(StreamsFilterTweets.class.getName());

    public static void main(String[] args) {
        // create properties
        String server = "localhost:9092";
        String application = "demo-kafka-streams";
        String serde = Serdes.StringSerde.class.getName();
        String topic = "twitter_status_connect";

        Properties props = new Properties();
        props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,server);
        props.setProperty(StreamsConfig.APPLICATION_ID_CONFIG,application);
        props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,serde);
        props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,serde);

        // create topology
        StreamsBuilder builder = new StreamsBuilder();

        // input topic
        KStream<String, String> inputTopic = builder.stream(topic);
        KStream<String, String> filteredStream= inputTopic.filter((k,tweets) -> getFollowersCount(tweets) > 10000);
        //Create another topic for filtered data.
        filteredStream.to("popular_tweets");

        // build topology
        KafkaStreams stream = new KafkaStreams(builder.build(),props);

        // start our streams application
        stream.start();

    }

    private static Double getFollowersCount(String tweets){
        try{
            Map<String, String> data = new Gson().fromJson(tweets,Map.class);
            String userJson = new Gson().toJson(data.get("user"));

            Map<String,String> userData = new Gson().fromJson(userJson,Map.class);
            Object followersCount = userData.get("followers_count");
            return Double.parseDouble(followersCount.toString());
        }catch (Exception e){
            log.error(String.format("getFollowersCount() Error: %s", e.getMessage()));
            return 0.0;
        }
    }
}
