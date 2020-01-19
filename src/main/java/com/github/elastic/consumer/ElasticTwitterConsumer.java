package com.github.elastic.consumer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.concurrent.CountDownLatch;

public class ElasticTwitterConsumer {
    private static Logger log = LoggerFactory.getLogger(ElasticTwitterConsumer.class);

    public static void main(String[] args) {
        log.info("running elastic twitter consumer");
        String server = "localhost:9092";
        String offset = "earliest";
        String topic = "misc_topic_twitter_1";
        String group = "twitter_consumer_group";

        run(server,topic,offset,group);

    }

    private static void run(String _server, String _topic, String _offset, String _group){

        CountDownLatch latch = new CountDownLatch(1);

        //Creating consumer thread.
        Runnable runnable = new ConsumerRunnable(_server,_topic,_group,_offset,latch);

        //Start thread
        Thread thread = new Thread(runnable);
        thread.start();

        // add shutdown hook.
        Runtime.getRuntime().addShutdownHook(
                new Thread(() -> {
                    log.info("caught shutdown hook");
                    ((ConsumerRunnable) runnable).shutdown();
                    try{
                        latch.await();
                    }catch(InterruptedException e){
                        log.error("Interrupt Exception: %s",e);
                    }
                    log.info("application has exited...");
                })
        );

    }
}
