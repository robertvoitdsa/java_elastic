package com.robert.elastic;

import com.sun.jersey.api.client.Client;
import com.sun.jersey.api.client.ClientResponse;
import com.sun.jersey.api.client.WebResource;
import org.apache.http.HttpHost;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;

import java.util.Iterator;
import java.util.Map;


public class das_app {

    //uri for das api search
    private static final String DAS_URI_SEARCH = "http://api.uat.nara.ppc-cloud.com/das.api/api/Authority/Search";
    private static final String DAS_URI_LOGIN = "http://api.uat.nara.ppc-cloud.com/das.api/api/Authentication/Login";
    private static final String DAS_URI_LOGOUT = "http://api.uat.nara.ppc-cloud.com/das.api/api/Authentication/Logout";

    //login json
    private static final String json_login = "{"
            +" \"Username\": \"describer1\","
            +" \"Password\": \"pa55word\""
            +"}";

    //sample JSON query for authority search
    private static final String json_input = "{"
            +"        \"PageNumber\": \"1\","
            +"        \"Search\": {"
            +"    \"BooleanOperator\": \"And\","
            +"        \"Criterias\": ["
            +"     {"
            +"        \"FieldName\": \"Auth_Type\","
            +"            \"SearchOperator\": \"Equals\","
            +"            \"SearchValue1\": \"Color\""
            +"    },"
            +"  {"
            +"     \"FieldName\": \"NaId\","
            +"            \"SearchOperator\": \"Between\","
            +"            \"SearchValue1\": \"10040000\","
            +"            \"SearchValue2\": \"10050000\""
            +"    }"
            +"    ]"
            +"},"
            +" \"FilterFields\": [\"naId\", \"termName\""
            +"]}";

    /**Use DAS api to logout of token created*/
    private static void logoutDasApi(String login_token) {
        try {
            //create DAS client with jersey library
            Client das_client = Client.create();

            //create webresource
            WebResource das_resource = das_client.resource(DAS_URI_LOGOUT);

            //request response via get
            ClientResponse das_response = das_resource.accept("application/json")
                    .header("Accept", "application/json")
                    .header("Content-Type", "application/json")
                    .header("Authorization", "token " + login_token)
                    .get(ClientResponse.class);

            //if response is incorrect, throw exception
            if (das_response.getStatus() != 200) {
                throw new RuntimeException("Failed logout : HTTP error code : "
                        + das_response.getStatus());
            }

            System.out.println("Logged out...");
        }
        catch (Exception e) {

            e.printStackTrace();

        }
    }


    /**Use DAS api to receive authentication token for login.*/
    private static String getLoginToken() {
        try {
            //create DAS client with jersey library
            Client das_client = Client.create();

            //create webresource
            WebResource das_resource = das_client.resource(DAS_URI_LOGIN);

            //request response via get
            ClientResponse das_response = das_resource
                    .header("Accept", "application/json")
                    .header("Content-Type", "application/json")
                    .post(ClientResponse.class,json_login);

            //if response is incorrect, throw exception
            if (das_response.getStatus() != 200) {
                throw new RuntimeException("Failed login : HTTP error code : "
                        + das_response.getStatus());
            }

            System.out.println("Logged on...");

            //return the token string and remove extra quotation marks
            String token = das_response.getEntity(String.class);
            return token.replaceAll("[\"]","");
        }
        catch (Exception e) {

            e.printStackTrace();
            return null;

        }
    }

    /**Get JSON object from hardcoded authority query*/
    private static JSONObject getJsonInput() {

        //get login token
        String login_token = getLoginToken();

        try {
            //create DAS client with jersey library
            Client das_client = Client.create();

            //create webresource
            WebResource das_resource = das_client.resource(DAS_URI_SEARCH);

            //request response via post method, be sure to log in with postman and fix token
            ClientResponse das_response = das_resource
                    .header("Accept", "application/json")
                    .header("Content-Type", "application/json")
                    .header("Authorization", "token " + login_token)
                    .post(ClientResponse.class, json_input);

            //if response is incorrect, throw exception
            if (das_response.getStatus() != 200) {
                throw new RuntimeException("Failed search : HTTP error code : "
                        + das_response.getStatus());
            }

            System.out.println("DAS Search complete.");
            //logout of DAS
            logoutDasApi(login_token);

            //parse the response to JSON object and return
            String output = das_response.getEntity(String.class);
            return (JSONObject) new JSONParser().parse(output);
        }
        catch (Exception e) {

            e.printStackTrace();
            return null;
        }
    }

    /**Creates ES client and populates the authority index with documents gathered
     * directly from DAS API*/
    public static void main(String[] args) {

            try {

                //create Elasticsearch client with resthighlevelclient
                RestHighLevelClient es_client = new RestHighLevelClient(
                        RestClient.builder(
                                new HttpHost("localhost", 9200, "http")));

                // get JSONObject for input
                JSONObject jo_input = getJsonInput();

                //object for parsed json objects
                JSONObject jo_output = new JSONObject();

                //iterate over the json authority array
                JSONArray auth = (JSONArray) jo_input.get("Authorities");
                Iterator authIterator = auth.iterator();

                //iterate over JSON input, parse values, post each to ES
                while (authIterator.hasNext()) {
                    Iterator<Map.Entry> itr1 = ((Map) authIterator.next()).entrySet().iterator();

                    //Map m = new LinkedHashMap();
                    while (itr1.hasNext()) {
                        Map.Entry pair = itr1.next();
                        if (pair.getKey().equals("AuthorityType")) {
                            continue;
                        } else if (pair.getKey().equals("AuthorityTypeText")) {
                            jo_output.put("authorityType", pair.getValue());
                        } else {
                            jo_output.put(pair.getKey(), pair.getValue());
                        }
                    }

                    System.out.println(jo_output.toString());
                /*
                //Index parsed json object to elastic search
                IndexRequest request = new IndexRequest("authority_index_test", "_doc");
                request.source(jo_output, XContentType.JSON);
                es_client.index(request);
                */

                }

                //close es_client connection
                es_client.close();
            }
            catch (Exception e) {

                e.printStackTrace();

            }
        }
}
