package com.robert.elastic;
import java.io.File;
import java.io.FileReader;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentType;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.*;


/**
 * Java high level rest api authority parser
 *
 * Running this will POST the parsed contents of all JSON files in specified directory
 * into local ES cluster
 *
 * Future modifications:
 * -Test this against more verbose examples
 *
 */
public class App
{
    //target directory
    static final File folder = new File("C:/Users/robert.voit/Documents/Programming/Elastic/online resourse json/sample");
    //number of total threads allowed
    static final int thread_count = 2;

    /**Get array of filenames*/
    private static LinkedList<String> listFilesForFolder(final File folder)
    {
        LinkedList<String> files= new LinkedList<String>();

        for (final File fileEntry : folder.listFiles()) {
            files.add(folder + "/"+ fileEntry.getName());
            //System.out.println(fileEntry.getName());
        }
        return files;
    }

    /**Parse each JSON file in directory, post the contents of each file to ES with bulk API*/
    public static void main( String[] args ) throws Exception
    {
        //tracking runtime of program
        long startTime = System.currentTimeMillis();

        System.out.println("Loading files from: " + folder);
        System.out.println("Starting ElasticSearch indexing process...");

        //create queue of all files in selected directory
        final Queue<String> files_queue = listFilesForFolder(folder);

        //create Elasticsearch client with resthighlevelclient
        final RestHighLevelClient es_client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")));

        //executor service for handling execution of threads. Only allows a specific number of threads to run at once
        ExecutorService pool = Executors.newFixedThreadPool(thread_count);

        //for loop to create specified number of threads
        for(int i = 1; i <=thread_count; i++)
        {
            //create new thread and define its runnable method
            Runnable running_thread = new Runnable(){
                public void run() {
                    try {
                        String current_file_path;
                        //remove files from the queue and run a new thread on each file until the queue is empty
                        while ((current_file_path = files_queue.poll()) != null) {

                            System.out.println("Thread " + Thread.currentThread().getId() + " is running on file: " + current_file_path);

                            // parsing file into json objects
                            JSONObject jo_input = (JSONObject) new JSONParser().parse(new FileReader(current_file_path));

                            //object for parsed json objects
                            JSONObject jo_output = new JSONObject();

                            //iterate over the json authority array
                            JSONArray auth = (JSONArray) jo_input.get("Authorities");
                            Iterator authIterator = auth.iterator();

                            //Create bulk api request for ES, timeout if it takes too long
                            BulkRequest request = new BulkRequest();
                            request.timeout(TimeValue.timeValueMinutes(1));

                            //loop to iterate over each authority in the authority array
                            while (authIterator.hasNext()) {

                                //iterator for current authority
                                Iterator<Map.Entry> itr1 = ((Map) authIterator.next()).entrySet().iterator();

                                //parse the current authority into a JSON object
                                while (itr1.hasNext()) {
                                    Map.Entry pair = itr1.next();
                                    if (pair.getKey().equals("AuthorityType")) {
                                        continue;
                                    } else if (pair.getKey().equals("AuthorityTypeText")) {
                                        jo_output.put("AuthorityType", pair.getValue());
                                    } else {
                                        jo_output.put(pair.getKey(), pair.getValue());
                                    }
                                }
                                //System.out.println ("Reading: "+current_file_path);
                                //System.out.println("Thread " + Thread.currentThread().getId() + " is running");
                                //System.out.println(jo_output.toString());

                                //create bulk api request by adding the current JSON object one at a time
                                request.add(new IndexRequest("authority_index_test", "_doc")
                                        .source(jo_output, XContentType.JSON));

                                /*
                                //Index parsed json object to elastic search (no bulk API)
                                IndexRequest request = new IndexRequest("authority_index_test", "_doc");
                                request.source(jo_output, XContentType.JSON);
                                es_client.index(request);
                                */
                            }

                            //Process Bulk API request after the file has been fully parsed
                            BulkResponse bulkResponse = es_client.bulk(request);
                            System.out.println("Thread " + Thread.currentThread().getId() + " has sent final bulk request.");
                        }
                    }
                    catch (Exception e) {
                        // Create better exception handling later
                        System.out.println("Exception in thread" + Thread.currentThread().getId() + " is caught: " + e);
                    }
                }
            };
            //execute thread pool
            pool.execute(running_thread);
        }

        //shutdown executor pool
        pool.shutdown();

        //System.out.println("Waiting for responses to finish...");
        //wait for all threads to finish or force timeout
        pool.awaitTermination(15,TimeUnit.MINUTES);

        //close es_client connection after threads are done executing
        es_client.close();
        System.out.println("Client Closed.");

        long stopTime = System.currentTimeMillis();
        long elapsedTime = (stopTime - startTime)/1000;

        //print total elapsed time
        System.out.println("Finished in: "+ elapsedTime +" seconds.");

    }
}
