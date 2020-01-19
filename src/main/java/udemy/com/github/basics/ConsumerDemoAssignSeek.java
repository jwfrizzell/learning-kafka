package udemy.com.github.basics;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoAssignSeek {



    public static void main(String[] args) {
        Logger log = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class.getName());
        String bootstrapServer = "localhost:9092";
        String offset = "earliest";
        String topic = "first_topic";

        Properties props = new Properties();
        props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, offset);

        // create consumer;
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);

        // assign and seek are mostly used to replay data or fetch a specific message.

        // assign
        TopicPartition partitionToReadFrom = new TopicPartition(topic,0);
        long offsetToReadFrom = 15L;
        consumer.assign(Arrays.asList(partitionToReadFrom));

        // seek
        consumer.seek(partitionToReadFrom,offsetToReadFrom);

        int numOfMessagesToRead = 5;
        boolean keepOnReading = true;
        int numberMessagesReadSoFar = 0;

        // get data
        while(keepOnReading){
            ConsumerRecords<String, String> records =  consumer.poll(Duration.ofMillis(100));
            for(ConsumerRecord<String,String> record: records){
                numberMessagesReadSoFar += 1;
                log.info(String.format("Key: %s", record.key()));
                log.info(String.format("Value: %s", record.value()));
                log.info(String.format("Partition: %s", record.partition()));
                log.info(String.format("Offset: %s", record.offset()));
                log.info(String.format("Timestamp: %s\n\n", record.timestamp()));
                if(numberMessagesReadSoFar >= numOfMessagesToRead){
                   keepOnReading = false;
                   break;
                }
            }
        }

        log.info("Exiting the application");
    }
}
