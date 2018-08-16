package com.robert.elastic;
import java.io.File;
import java.io.FileReader;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

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
 * Java high level rest api test
 *
 *
 * Not the most efficient at the moment (triple nested loop)
 *
 * Future modifications:
 * -see if I can add multiple threads, 1 file processed per thread (not needed for local machine)
 * -Consider removing innermost loop to process JSON differently
 * -Test this against more verbose examples
 */
public class App
{

    static final File folder = new File("C:/Users/robert.voit/Documents/Programming/Elastic/collections/bulk json/multiParse");

    /**Get array of filenames*/
    public static LinkedList<String> listFilesForFolder(final File folder)
    {
        LinkedList<String> files= new LinkedList<String>();

        for (final File fileEntry : folder.listFiles()) {
            files.add(fileEntry.getName());
        }
        return files;
    }

    /**Parse each JSON file in directory, post the contents of each file to ES with bulk API*/
    public static void main( String[] args ) throws Exception
    {

        //create Elasticsearch client with resthighlevelclient
        RestHighLevelClient es_client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")));

        LinkedList<String> files = listFilesForFolder(folder);

        //for each file in directory
        for(String current_file: files) {

            // parsing file "with json objects"
            Object obj = new JSONParser().parse(new FileReader(folder+ "/" +current_file));

            // typecasting obj to JSONObject
            JSONObject jo_input = (JSONObject) obj;

            //object for parsed json objects
            JSONObject jo_output = new JSONObject();

            //iterate over the json authority array
            JSONArray auth = (JSONArray) jo_input.get("Authorities");
            Iterator authIterator = auth.iterator();


            //Create bulk api request for ES, timeout if it takes too long
            BulkRequest request = new BulkRequest();
            request.timeout(TimeValue.timeValueMinutes(1));

            while (authIterator.hasNext()) {
                Iterator<Map.Entry> itr1 = ((Map) authIterator.next()).entrySet().iterator();

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

                //System.out.println(jo_output.toString());


                //build bulk api request one object at a time
                request.add(new IndexRequest("authority_index_test", "_doc")
                    .source(jo_output, XContentType.JSON));


                /*
                //Index parsed json object to elastic search (no bulk API)
                IndexRequest request = new IndexRequest("authority_index_test", "_doc");
                request.source(jo_output, XContentType.JSON);
                es_client.index(request);
                */

            }

            //Process Bulk API indexing
            BulkResponse bulkResponse = es_client.bulk(request);

        }
        //close es_client connection
        es_client.close();

    }
}
