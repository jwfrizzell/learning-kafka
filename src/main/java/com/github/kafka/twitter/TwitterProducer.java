package com.github.kafka.twitter;

// kafka imports

import com.google.common.collect.Lists;
import com.google.gson.Gson;

// twitter imports

// log 4j
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.hbc.core.Client;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.concurrent.BlockingQueue;
import java.util.Map;
import java.util.List;
public class TwitterProducer {
    final Logger log = LoggerFactory.getLogger(TwitterProducer.class);

    /***
     * Main entry point.
     * @param args
     */
    public static void main(String[] args) {

        TwitterClient twitter = null;
        KafkaProducer producer = null;
        String topic = "misc_topic_twitter_1";

        try {
            List<String> list = new ArrayList();
            list.addAll(Arrays.asList("#trump","#potus","trump","potus"));

            //Instantiate twitter client.
            twitter = new TwitterClient(list);
            //Create blocking queue
            BlockingQueue queue = twitter.getBlockingQueue();
            //Connect client.
            Client client = twitter.getClient();
            client.connect();

            producer  = new KafkaProducer();
            while (!client.isDone()) {
                String msg = (String) queue.take();
                Map<String, String> data = new Gson().fromJson(msg, Map.class);

                if (data.get("lang") != null) {
                    producer.run(
                            topic,
                            data.get("lang"),
                            data.get("text"));
                }
            }
        }
        catch(InterruptedException ex){
            System.out.println(ex.getMessage());
        }
        finally{
            twitter.stop();
            producer.close();
        }

    }
}
