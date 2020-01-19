package com.github.kafka.twitter;

import com.google.common.base.Strings;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;

//import log4j
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class KafkaProducer {
    final Logger log = LoggerFactory.getLogger(KafkaProducer.class);
    private Properties properties = null;
    private org.apache.kafka.clients.producer.KafkaProducer<String,String> producer = null;

    private static String BOOTSTRAP_SERVER = "localhost:9092";
    /***
     * Initialize producer properties.
     * Create kafka producer
     */
    public KafkaProducer(){
        log.info("Initializing KafkaProducer...");
        String className = StringSerializer.class.getName();

        properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVER);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, className);
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,className);
        //Data consistency settings
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG,"all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION,"5");

        //Batch settings for high throughput at the expense of latency and cpu usage.
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG,"snappy");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG,"20");

        producer = new org.apache.kafka.clients.producer.KafkaProducer<String,String>(properties);

    }

    public void run(String _topic, String _key, String _value){
        //Create producer record.
        ProducerRecord<String,String> record = new ProducerRecord<>(_topic, _key, _value);
        log.info(String.format("Topic: %s", _topic));
        log.info(String.format("Key: %s",_key));

        this.producer.send(record, new Callback() {
            @Override
            public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                if (e == null) {
                    // record sent
                    log.info("Received Meta Data => ");
                    log.info(String.format("Topic: %s", recordMetadata.topic()));
                    log.info(String.format("Partition: %s", recordMetadata.partition()));
                    log.info(String.format("Offset: %s", recordMetadata.offset()));
                    log.info(String.format("Timestamp: %s\n\n", recordMetadata.timestamp()));

                } else {
                    log.error("Error: "+ e.getMessage());
                }
            }
        });

    }

    /***
     * Close producer.
     */
    public void close(){
        if(producer != null)
            this.producer.close();
    }


}
