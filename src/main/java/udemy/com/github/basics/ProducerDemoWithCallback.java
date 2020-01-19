package udemy.com.github.basics;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    public static void main(String[] args){
        final Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        String bootstrapServer = "localhost:9092";
        // create Producer properties
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,bootstrapServer);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
                StringSerializer.class.getName());

        // create Producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for(int i = 0; i < 50; i++) {


            // create a producer record.
            final ProducerRecord<String, String> record =
                    new ProducerRecord<String, String>("first_topic", String.format("hello world %s",  Integer.toString(i)));

            // send data - asynchronous
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    // executes after record is sent or error is thrown.
                    if (e == null) {
                        // record sent
                        logger.info("Received Meta Data => ");
                        logger.info(String.format("Topic: %s", recordMetadata.topic()));
                        logger.info(String.format("Partition: %s", recordMetadata.partition()));
                        logger.info(String.format("Offset: %s", recordMetadata.offset()));
                        logger.info(String.format("Timestamp: %s\n\n", recordMetadata.timestamp()));

                    } else {
                        logger.error("Error: %s", e.getMessage());
                    }
                }
            });
        }

        // flush and close producer.
        producer.close();
    }
}
