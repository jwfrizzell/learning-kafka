package com.github.elastic.consumer;


import org.apache.http.HttpHost;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.action.support.WriteRequest;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
public class ConsumerRestClient {
    final Logger log = LoggerFactory.getLogger(ConsumerRestClient.class);

    private String host = "localhost";
    private int port1 = 9200;
    private String schema = "http";
    private RestHighLevelClient client = null;

    // Elastic search credentials
    private String user = "elastic";

    /***
     * Default constructor.
     */
    public ConsumerRestClient(){
        log.info("initializing ConsumerRestClient client...");
        this.client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(this.host,this.port1,this.schema)
                )
        );
        log.info("client has been successfully initialized");
    }

    /***
     * Send data to elastic search server.
     * @param post The message being sent to elastic search.
     * @param id The unique id to send.
     * @throws Exception
     */
    public void send(String post, String id) throws Exception {
        log.info("begin sending request...");
        Map<String ,Object> jsonMap = new HashMap();
        jsonMap.put("user",this.user);
        jsonMap.put("postDate", new Date());
        jsonMap.put("message",post);
        jsonMap.put("key", id);

        //Build request and set options.
        IndexRequest request = new IndexRequest("tweets", "doc", id)
                .source(jsonMap);
        request.timeout(TimeValue.timeValueSeconds(1));
        request.setRefreshPolicy(WriteRequest.RefreshPolicy.WAIT_UNTIL);

        try{
            IndexResponse response = this.client.index(request, RequestOptions.DEFAULT);
            log.info(String.format("ID: %s",response.getId()));
            log.info(String.format("Index: %s",response.getIndex()));
            log.info(String.format("Sequence: %s",response.getSeqNo()));
            log.info(String.format("Shard ID: %s",response.getShardId()));
            log.info(String.format("Shard Info: %s",response.getShardInfo()));
            log.info(String.format("Type: %s",response.getType()));

        }catch(ElasticsearchException e){
            throw new Exception(String.format("Elastic Error: %s", e.getMessage()));
        }catch(IOException e){
            throw new Exception(String.format("Elastic Error: %s", e.getMessage()));
        }catch(Exception e){
            throw new Exception(String.format("Exception: %s",e.getMessage()));
        }
    }

    /***
     * Close client
     */
    public  void close() throws IOException {
        if(this.client != null) {
            log.info("client is being closed");
            this.client.close();
        }
    }

}