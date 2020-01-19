package com.github.kafka.twitter;

import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterClient  {
    final Logger log = LoggerFactory.getLogger(TwitterClient.class);
    private static String CONSUMER_KEY = System.getenv("TWITTER_CONSUMER_KEY");
    private static  String CONSUMER_SECRET = System.getenv("TWITTER_CONSUMER_SECRET");
    private static String TOKEN = System.getenv("TWITTER_TOKEN");
    private static String TOKEN_SECRET = System.getenv("TWITTER_TOKEN_SECRET");
    private Client client = null;
    BlockingQueue<String> queue =  null;

    /***
     * Constructor to initialize twitter client.
     */
    public TwitterClient(List<String> list){
        queue = new LinkedBlockingQueue<String>(1000);
        StatusesFilterEndpoint endpoint = new StatusesFilterEndpoint();
        endpoint.trackTerms(list);

        Authentication auth = new OAuth1(CONSUMER_KEY, CONSUMER_SECRET, TOKEN, TOKEN_SECRET);

        // create a client
        client = new ClientBuilder()
                .hosts(Constants.STREAM_HOST)
                .endpoint(endpoint)
                .authentication(auth)
                .processor(new StringDelimitedProcessor(queue))
                .build();
    }

    /***
     * Return the queue.
     * @return BlockingQueue<String>
     */
    public BlockingQueue<String> getBlockingQueue(){
        return this.queue;
    }

    public Client getClient(){
        return this.client;
    }
    /***
     * stop the client connection.
     */
    public  void stop(){
        if (client!=null) {
            log.info("stopping client...");
            client.stop();
            log.info("client has been stopped...");
        }
    }

}