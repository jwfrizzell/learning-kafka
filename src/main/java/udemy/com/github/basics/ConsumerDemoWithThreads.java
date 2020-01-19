package udemy.com.github.basics;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreads {



    public static void main(String[] args) {
        ConsumerDemoWithThreads consumer = new ConsumerDemoWithThreads();
        consumer.run();
    }

    private ConsumerDemoWithThreads(){ }

    private void run(){
        Logger log = LoggerFactory.getLogger(ConsumerDemoWithThreads.class.getName());
        String bootstrapServer = "localhost:9092";
        String groupID = "my-sixth-application";
        String offset = "earliest";
        String topic = "first_topic";

        // latch for dealing with multiple threads.
        CountDownLatch latch = new CountDownLatch(1);

        log.info("Creating the consumer thread");
        // create runnable consumer
        Runnable runnableConsumer = new ConsumerRunnable(topic,bootstrapServer,groupID,offset,latch);

        // start thread
        Thread thread = new Thread(runnableConsumer);
        thread.start();

        // add a shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("Caught shutdown hook");
            ((ConsumerRunnable) runnableConsumer).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            log.info("application has exited...");
        }));

        try {
            latch.await();
        }catch(InterruptedException e){
            log.error(e.getMessage());
        }
        finally{
            log.info("Application is closing");
        }
    }

    public class ConsumerRunnable implements Runnable {

        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;
        private Logger log = LoggerFactory.getLogger(ConsumerDemoWithThreads.class.getName());

        public ConsumerRunnable(String _topic,
                              String _server,
                              String _groupID,
                              String _offset,
                              CountDownLatch _latch) {
            this.latch = _latch;

            Properties props = new Properties();
            props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, _server);
            props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName());
            props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                    StringDeserializer.class.getName());
            props.setProperty(ConsumerConfig.GROUP_ID_CONFIG, _groupID);
            props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, _offset);

            // create consumer;
            this.consumer = new KafkaConsumer<String, String>(props);

            // subscribe consumer
            // subscribe consumer
            this.consumer.subscribe(Arrays.asList(_topic));


        }

        @Override
        public void run() {
            // get data
            try {
                while (true) {
                    ConsumerRecords<String, String> records = this.consumer.poll(Duration.ofMillis(100));
                    for (ConsumerRecord<String, String> record : records) {
                        log.info(String.format("Key: %s", record.key()));
                        log.info(String.format("Value: %s", record.value()));
                        log.info(String.format("Partition: %s", record.partition()));
                        log.info(String.format("Offset: %s", record.offset()));
                        log.info(String.format("Timestamp: %s\n\n", record.timestamp()));
                    }
                }
            } catch (WakeupException ex) {
                log.info("Received shutdown signal");
            }
            finally {
                this.consumer.close();

                // notify the completion of the consumer.
                this.latch.countDown();
            }
        }

        // shutdown consumer
        public void shutdown(){
            // the wakeup() method is a special method to interrupt consumer.poll()
            // it will throw the exception WakeUpException
            this.consumer.wakeup();
        }
    }
}
