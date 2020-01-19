package udemy.com.github.basics;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class);

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

        for(int i = 0; i < 10; i++) {
            String topic = "first_topic";
            String value = String.format("hello world %s",  Integer.toString(i));
            String key = String.format("id_%s", Integer.toString(i));

            // create a producer record.
            final ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);

            logger.info(String.format("Key: %s",key));
            // id_0 Partition 1
            // id_1 Partition 0
            // id_2 Partition 2
            // id_3 Partition 0
            // id_4 Partition 2
            // id_5 Partition 2
            // id_6 Partition 0
            // id_7 Partition 2
            // id_8 Partition 1
            // id_9 Partition 2

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
                        logger.error("Error: "+ e.getMessage());
                    }
                }
            }).get(); // block send to make it synchronous. This is demo. Never do in prod.
        }

        // flush and close producer.
        producer.close();
    }
}
