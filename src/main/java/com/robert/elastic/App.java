package com.robert.elastic;
import java.io.FileReader;
import java.util.Iterator;
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
 */
public class App 
{
    public static void main( String[] args ) throws Exception
    {

        //create Elasticsearch client with resthighlevelclient
        RestHighLevelClient es_client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")));

        // parsing file "with json objects"
        Object obj = new JSONParser().parse(new FileReader("C:/Users/robert.voit/Documents/Programming/Elastic/collections/bulk json/ingest_doj_short.json"));

        // typecasting obj to JSONObject
        JSONObject jo_input = (JSONObject) obj;

        //object for parsed json objects
        JSONObject jo_output = new JSONObject();
        JSONArray jo_out_array = new JSONArray();

        //iterate over the json authority array
        JSONArray auth = (JSONArray) jo_input.get("Authorities");
        Iterator authIterator = auth.iterator();


        BulkRequest request = new BulkRequest();
        request.timeout(TimeValue.timeValueMinutes(1));

        while (authIterator.hasNext())
        {
            Iterator<Map.Entry> itr1 = ((Map) authIterator.next()).entrySet().iterator();

            while (itr1.hasNext()) {
                Map.Entry pair = itr1.next();
                if(pair.getKey().equals("AuthorityType")){
                    continue;
                }
                else if(pair.getKey().equals("AuthorityTypeText")){
                    jo_output.put("AuthorityType", pair.getValue());
                }
                else {
                    jo_output.put(pair.getKey(), pair.getValue());
                }
            }

            request.add(new IndexRequest("authority_index_test", "_doc")
                    .source(jo_output, XContentType.JSON));

                /*
                //Index parsed json object to elastic search
                IndexRequest request = new IndexRequest("authority_index_test", "_doc");
                request.source(jo_output, XContentType.JSON);
                es_client.index(request);
                */

        }


        BulkResponse bulkResponse = es_client.bulk(request);

        //close es_client connection
        es_client.close();



    }
}
