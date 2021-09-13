package com.tecnotree.lifecycle;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.http.HttpHost;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import com.jcraft.jsch.JSchException;
import com.opencsv.CSVWriter;

public class LogElasticSearch {
	
	final private static String[] FETCH_FIELDS = { "msisdn", "mainId", "creationDate", "rechargeValue"};

	final private static String MATCH_FIELD = "workFlowId";
	final private static String MATCH_FIELD2 = "serviceName";
	
	final private static String[] MUST_MATCH = { "lifecycle_expire_frozen_balance" };
	final private static String[] MUST_MATCH2 = { "mysql-lifecycle-service" };
	final private static String[] MUST_NOT_MATCH = { "21.211.33.63" };

	final private static String TIME_FIELD = "@timestamp";
	final private static String START_TIME = "2021-07-23T00:00:00";
	final private static String END_TIME = "2021-07-23T23:59:59";

	final private static String INDEX = "appolo_lifecycle*"; // accepts * as wildcard, .e.g log*
	
    public static void main( String[] args ) throws IOException, NumberFormatException, JSchException
    {
    	System.out.println("EMPEZANDO LA CONEXION");
    	RestHighLevelClient client = new RestHighLevelClient(
				RestClient.builder(new HttpHost("10.43.220.240", 9200, "http")));
    	System.out.println("CONEXION LISTA");
    	
    	
	    
    	/*SearchRequest searchRequest = new SearchRequest();
    	searchRequest.indices(INDEX);
    	
    	SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder();
        BoolQueryBuilder boolQueryBuilder2 = new BoolQueryBuilder();
        
        boolQueryBuilder.must(QueryBuilders.termQuery("serviceName", "mysql-lifecycle-service"));
        searchSourceBuilder.query(boolQueryBuilder);
        System.out.println("searchSourceBuilder.toString():" + searchSourceBuilder.toString());
        searchRequest.source(searchSourceBuilder);
        
        SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
        searchResponse.getHits().forEach(documentFields -> {
            System.out.println("documentFields.getSourceAsMap():" + documentFields.getSourceAsMap());
        });
        System.out.println("\n=================\n");*/
		
    	SearchRequest searchRequest = new SearchRequest();

		//SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		
		/*QueryBuilder qb= QueryBuilders
				  .matchQuery("workFlowId", "lifecycle_expire_frozen_balance");
		*/
		/*BoolQueryBuilder qb = QueryBuilders.boolQuery();

		if (MUST_MATCH.length > 0) {
			for (String match : MUST_MATCH) {
				qb.must(QueryBuilders.matchQuery(MATCH_FIELD, match));
			}
		}

		if (MUST_MATCH2.length > 0) {
			for (String match : MUST_MATCH2) {
				qb.must(QueryBuilders.matchQuery(MATCH_FIELD2, match));
			}
		}
		

		qb.must(QueryBuilders.rangeQuery(TIME_FIELD).gte(START_TIME));
		qb.must(QueryBuilders.rangeQuery(TIME_FIELD).lte(END_TIME));

		searchSourceBuilder.query(qb).fetchSource(FETCH_FIELDS, null);*/
		
		/*BoolQueryBuilder qb = QueryBuilders.boolQuery();
				qb
		 .must(QueryBuilders.matchQuery("workFlowId", "lifecycle_expire_frozen_balance"))
		 .must(QueryBuilders.matchQuery("serviceName", "mysql-lifecycle-service"))
		 .must(QueryBuilders.rangeQuery(TIME_FIELD).gte(START_TIME))
		 .must(QueryBuilders.rangeQuery(TIME_FIELD).lte(END_TIME));*/
		
		//String queryString = "{\"query\": { \"match\": {\"serviceName\": {\"query\": \"mysql-lifecycle-service\"  } }, \"match\": {\"workFlowId\": {\"query\": \"lifecycle_expire_frozen_balance\" } } }}";

		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(QueryBuilders.rangeQuery(TIME_FIELD).from(START_TIME).to(END_TIME));
		//searchSourceBuilder.query(qb).fetchSource(FETCH_FIELDS, null);

		System.out.println("searchSourceBuilder.toString():" + searchSourceBuilder.toString());
        
		searchRequest.indices(INDEX);
		searchRequest.source(searchSourceBuilder.size(10000));
		
		SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
		
		if (searchResponse.getHits().getTotalHits() > 0) {
			
			//CREO EL ARCHIVO CSV
	    	String savePath = "/home/operador/Lifecycle/output/";
			File file = new File(savePath);
			if (!file.exists()) {
				file.mkdir();
			}
			String filePath = savePath + "FCG-PRUEBA.csv";
			OutputStreamWriter out = new OutputStreamWriter(new FileOutputStream(filePath), "UTF-8");
		    //CSVWriter writer = new CSVWriter(out);
			CSVWriter writer = new CSVWriter(out,
				    CSVWriter.DEFAULT_SEPARATOR,
				    CSVWriter.NO_QUOTE_CHARACTER,
				    CSVWriter.DEFAULT_ESCAPE_CHARACTER,
				    CSVWriter.RFC4180_LINE_END);
					    
			System.out.println("getTotalHits(): " + searchResponse.getHits().getTotalHits());
			
			SearchHits hits = searchResponse.getHits();
			SearchHit hit[] = hits.getHits();
			
			List<String[]> allElements = new ArrayList<String[]>();
			
			System.out.println("hit.length: " +  hit.length);
			
			
			for (int i = 0; i < hit.length; i++) {
				/*SearchHit searchH = hit[i];
				System.out.println("hit.getSourceAsMap():" + searchH.getSourceAsMap());
				System.out.println("get(fetchField):" + searchH.getSourceAsMap().get("request"));
				System.out.println("get(\"request\").getClass():" + searchH.getSourceAsMap().get("request").getClass());
				
				@SuppressWarnings("rawtypes")
				HashMap request = (HashMap) searchH.getSourceAsMap().get("request");
				
				System.out.println("request.get(\"Request\"):" + request.get("Request"));
				System.out.println("request.get(\"Request\").getClass():" + request.get("Request").getClass());
				
				@SuppressWarnings("rawtypes")
				HashMap Request = (HashMap) request.get("Request");
				System.out.println("Request.get(\"msisdn\"):" + Request.get("msisdn"));
				*/
				
				System.out.println(" - @timestamp " + hit[i].getSourceAsMap().get("@timestamp"));
				
				// contenido del documento
								
				String[] content = new String[]{
							getDataRequest(hit[i],"msisdn"),//1
							getDataRequest(hit[i],"mainId"),//2
							getDataRequest(hit[i],"creationDate"),//3
							getDataRequest(hit[i],"rechargeValue"),//4
							getDataRequest(hit[i],"balance"),//5
							getDataRequest(hit[i],"balanceT"),//6
							getDataRequest(hit[i],"startRecharge"),//7
							getDataRequest(hit[i],"endRecharge"),//8
							getDataRequest(hit[i],"expirationDate"),//9
							getDataRequest(hit[i],"status"),//10
							getDataRequest(hit[i],"op"),//11
							getDataRequest(hit[i],"mode"),//12
							getDataRequest(hit[i],"last"),//13
							getDataRequest(hit[i],"fileName"),//14
							getDataRequest(hit[i],"modifiedDate"),//15
							getDataRequest(hit[i],"fileReceivedDate"),//16
							getDataRequest(hit[i],"fileProcessedDate"),//17
							getCodeQuery(hit[i],"insertCdvRecordsBalanceExpiration"),//18
							getCodeQuery(hit[i],"deleteCdvMainBalanceExpiration"),//19
							getCodeQuery(hit[i],"insertCDVMain"),//20
							getCodeQuery(hit[i],"updateCDVMainById"),//21
							getCodeQuery(hit[i],"updateCDVRecordsPrevious"),//22
							getCodeQuery(hit[i],"insertCDVRecords"),//23
							
						};
					
					System.out.println(" - content " + content);
				
					allElements.add(content);
					
				}//FIN DE for (int i = 0; i < hit.length; i++) {
			
				writer.writeAll(allElements);
				writer.close();
				
				/*for (String fetchField : FETCH_FIELDS) {
					System.out.println(" - " + fetchField + " " + getDataRequest(hit[i],fetchField));
					
				}*/
				
				
				
			
			
			/*for (SearchHit hit : searchResponse.getHits()) {
				System.out.println("Match: ");
				for (String fetchField : FETCH_FIELDS) {
					System.out.println("hit.getSourceAsMap():" + hit.getSourceAsMap());
						
					System.out.println(" - " + fetchField + " " + hit.getSourceAsMap().get(fetchField));
					
				}
				
			}*/
		
		} else {
			System.out.println("No results matching the criteria.");
		}//FIN DE if (searchResponse.getHits().getTotalHits() > 0) {
		
		client.close();
    }
  
    private static String getDataRequest(SearchHit searchH, String field) {
    	@SuppressWarnings("rawtypes")
		HashMap request = (HashMap) searchH.getSourceAsMap().get("request");
		@SuppressWarnings("rawtypes")
		HashMap Request = (HashMap) request.get("Request");
		System.out.println(" - " + field + " " +(String) Request.get(field));
		return Request.get(field) != null ? (String) Request.get(field) : "blank";
    }
    
    private static String getCodeQuery(SearchHit searchH, String field) {
    	@SuppressWarnings("rawtypes")
		HashMap response = (HashMap) searchH.getSourceAsMap().get("response");
    	//System.out.println("response.get(\""+response+"\"):" + response.get(field));
		
		@SuppressWarnings("rawtypes")
		HashMap responseQuery = (HashMap) response.get(field);
		if(responseQuery != null) {
			@SuppressWarnings("rawtypes")
			HashMap responseCode = (HashMap) responseQuery.get("response");
			System.out.println(" - " + field + " " +(Integer) responseCode.get("code"));
			return String.valueOf((Integer) responseCode.get("code"));
		}
		
		return "blank";
    }
	 
}
