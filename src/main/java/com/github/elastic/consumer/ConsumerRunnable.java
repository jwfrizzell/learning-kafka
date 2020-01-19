package com.github.elastic.consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerRunnable implements Runnable{
    private CountDownLatch latch;
    private KafkaConsumer<String,String> consumer;
    private Logger log = LoggerFactory.getLogger(ConsumerRunnable.class.getName());
    private ConsumerRestClient client = null;

    /***
     * Set the consumer and subscribe to topic.
     * @param _server Bootstrap Server to connect to.
     * @param _topic Topic being subscribed to.
     * @param _group Group to grab data for.
     * @param _offset Where to start reading from.
     * @param _latch Set latch for blocking.
     */
    public ConsumerRunnable(String _server, String _topic,
                            String _group, String _offset, CountDownLatch _latch){
        this.latch= _latch;
        String name = StringDeserializer.class.getName();

        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,_server);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,name);
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,name);
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,_group);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,_offset);

        this.consumer = new KafkaConsumer<String, String>(properties);
        this.consumer.subscribe(Arrays.asList(_topic));

        this.client = new ConsumerRestClient();
    }

    @Override
    public void run() {
        try{
            log.info("running consumer...");
            while(true){
                ConsumerRecords<String, String> records = this.consumer.poll(Duration.ofMillis(100));
                for(ConsumerRecord<String, String> record: records){
                    log.info(String.format("Key: %s", record.key()));
                    log.info(String.format("Value: %s", record.value()));
                    log.info(String.format("Partition: %s", record.partition()));
                    log.info(String.format("Offset: %s", record.offset()));
                    log.info(String.format("Timestamp: %s\n\n", record.timestamp()));
                    this.client.send(record.value(), record.key());

                }
            }

        }catch (WakeupException e){
            log.error(String.format("run WakeupException: %s", e.getMessage()));
        } catch (Exception e) {
            log.error("run Exception: %s", e.getMessage());
        } finally {
            if(this.consumer != null)
                this.consumer.close();
            try {
                if (this.client != null)
                    this.client.close();
            }catch(IOException  e){
                log.error(String.format("ConsumerRestClient Error: %s", e.getMessage()));
            }

            this.latch.countDown();
        }
    }

    public void shutdown(){
        this.consumer.wakeup();
    }
}
