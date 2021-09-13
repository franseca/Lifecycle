package com.tecnotree.lifecycle;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Clock;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;

import com.jcraft.jsch.JSchException;
import com.opencsv.CSVWriter;
import com.tecnotree.tools.Tn3ElasticSearch;
import com.tecnotree.tools.Tn3File;
import com.tecnotree.tools.Tn3Logger;
import com.tecnotree.tools.Tn3SFTP;

public class Logs {
	
	//CONSTANTES
	private static String PROP_FILE_NAME = "configurationLogs.properties";
	
	//VARIABLES
	private static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static String bulk_ip = "", bulk_port = "", bulk_user = "", bulk_pass = "", bulk_path = "", bulk_id = "";
	private static String log_outputDirectory = "";
	private static boolean bulk_sftp = false;
	private static Tn3Logger logger = null;
	private static String file_timeZone = "", file_outputDirectory = "", file_folder = "";
	private static String ipServerElasticSearch = "", ipServerElasticSearchParam = "", schemaElasticSearch  = "", indexElasticSearch = "", dateFrom = "", dateTo = "";
	private static int portServerElasticSearch = 0, sizeHitsElasticSearch = 0, responseCodeQuery = 500;
	private static RestHighLevelClient client = null;
	private static Tn3ElasticSearch tn3ElasticSearch = null;
	private static CSVWriter writer = null;
	private static String pattern1 = "^(\\d{4})(\\/|-)(0[1-9]|1[0-2])\\2([0-2][0-9]|3[0-1])(T)(0[0-9]|1[0-9]|2[0-3])(:)([0-5][0-9])(:)([0-5][0-9])$";
	
    public static void main( String[] args ) throws IOException, NumberFormatException, JSchException
    {
    	//OBTENGO LOS PARAMETROS
    	if(args.length == 0) {//SI NO SE PASARON PARAMETROS
    		System.out.println(dateFormat.format(new Date()) + " - No se envio ningun parametro.");
    		 
    	}else {
   			
    		//IP DEL SERVIDOR ELASTICSEARCH
    		dateFrom = args[0];
    		dateTo = args[1];
    		ipServerElasticSearchParam = args[2];
    		
    		//VALIDO QUE LA FECHA DESDE SEA DEL FORMATO CORRECTO
	  		if(!Pattern.matches(pattern1, dateFrom)) {
	  			System.out.println(dateFormat.format(new Date()) + " - The parameter dateFrom don't have the correct format. The format must be: YYYY-MM-ddTHH:MM:ss (Ex.:2021-07-21T05:00:00).");
	  			System.out.println(dateFormat.format(new Date()) + " - Finished Logs process.");
    			System.exit(0);
	  		}
	  		
	  		try {
		  		
	  			//VALIDO SI LA FECHA DESE ES VALIDA
		  		DateFormat formatoFecha = new SimpleDateFormat("yyyy-MM-dd");
		  		formatoFecha.setLenient(false);
		  		formatoFecha.parse(dateFrom);
		  		
	  		}catch (NullPointerException|NumberFormatException|ParseException e) {
  		  		
	  			System.out.println(dateFormat.format(new Date()) + " - Exception found - " + e.getMessage());
  		  		System.out.println(dateFormat.format(new Date()) + " - Finished Logs process.");
  		  		return;
  		  	}
	  		
	  		//VALIDO QUE LA FECHA HASTA SEA DEL FORMATO CORRECTO
	  		if(!Pattern.matches(pattern1, dateTo)) {
	  			System.out.println(dateFormat.format(new Date()) + " - The parameter dateTo don't have the correct format. The format must be: YYYY-MM-ddTHH:MM:ss (Ex.:2021-07-21T23:00:00).");
	  			System.out.println(dateFormat.format(new Date()) + " - Finished Logs process.");
    			System.exit(0);
	  		}
    		   		
	  		try {
		  		
	  			//VALIDO SI LA FECHA HASTA ES VALIDA
		  		DateFormat formatoFecha = new SimpleDateFormat("yyyy-MM-dd");
		  		formatoFecha.setLenient(false);
		  		formatoFecha.parse(dateTo);
		  		
	  		}catch (NullPointerException|NumberFormatException|ParseException e) {
  		  		
	  			System.out.println(dateFormat.format(new Date()) + " - Exception found - " + e.getMessage());
  		  		System.out.println(dateFormat.format(new Date()) + " - Finished Logs process.");
  		  		return;
  		  	}
	  		
    		try {
    			//CARGO EL ARCHIVO DE PROPIEDADES
    			loadProperties(PROP_FILE_NAME);
		  		
    		}catch (IOException|SecurityException|NullPointerException|NumberFormatException e) {
    			System.out.println(dateFormat.format(new Date()) + " - Exception found - " + e.getMessage());
    			System.out.println(dateFormat.format(new Date()) + " - Finished Logs process.");
    			System.exit(0);
    		}
			
    		try {
	  				
    			//CREO EL DIRECTORIO Y EL ARCHIVO DE LOGS
    			logger = new Tn3Logger("Logs", log_outputDirectory);
		  		
    			logger.info("Command executed: Logs dateFrom:"+ args[0] + " dateTo:" + args[1]);
    			System.out.println(dateFormat.format(new Date()) + " - Command executed: Logs dateFrom:"+ args[0] + " dateTo:" + args[1]);
		  						  		
    			logger.info("Properties file load.");
    			System.out.println(dateFormat.format(new Date()) + " - Starting Logs process...");
    			logger.info("Starting Logs process...");
    			
    			//CONEXION A ELASTICSEARCH
    			System.out.println(dateFormat.format(new Date()) + " - Connecting to ElasticSearch...");
    			logger.info("Connecting to ElasticSearch...");
    			    			
    			tn3ElasticSearch = new Tn3ElasticSearch((ipServerElasticSearchParam == null) ? ipServerElasticSearch : ipServerElasticSearchParam, portServerElasticSearch, schemaElasticSearch, indexElasticSearch);
    			tn3ElasticSearch.startConnection();
    			client = tn3ElasticSearch.getConn();
    			
    			System.out.println(dateFormat.format(new Date()) + " - Connected in ElasticSearch...");
    			logger.info("Connected in ElasticSearch...");
    			
    			//GENERO ARCHIVO CON LA DATA DE ELASTICSEARCH
    			generateCSVFile();
    			    			
    			System.out.println(dateFormat.format(new Date()) + " - Finished Logs process.");
    			logger.info("Finished Logs process.");
    			System.exit(0);
  			
    		}catch (IOException|SecurityException|NullPointerException|NumberFormatException e) {
    			
    			System.out.println(dateFormat.format(new Date()) + " - Exception found - " + e.getMessage());
    			System.out.println(dateFormat.format(new Date()) + " - Finished Logs process.");
    			logger.severe(e);
    			//e.printStackTrace();
    			System.exit(1);
    		}
   		
    	}
    }
    	 
    /**
     * Method loads the properties.
     * 
	 * @param propFileName
	 * @throws IOException
	 */
    public static void loadProperties(String propFileName) throws IOException {
		
    	Properties prop = new Properties();
    	InputStream inputStream = ClassLoader.getSystemResourceAsStream(propFileName);
  		prop.load(inputStream);
		 	  		
  		//PROPIEDADES DEL LOS LOGS
  		log_outputDirectory = prop.getProperty("log.outputDirectory");
  		
  		//PROPIEDADES DE FTP/SFTP
  		bulk_ip = prop.getProperty("bulk.ip");
  		bulk_port = prop.getProperty("bulk.port");
  		bulk_user = prop.getProperty("bulk.user");
  		bulk_pass = prop.getProperty("bulk.pass");
  		bulk_sftp = Boolean.parseBoolean(prop.getProperty("bulk.sftp"));
  		bulk_path = prop.getProperty("bulk.path");
  		bulk_id = prop.getProperty("bulk.id");
  		
  		//PROPIEDADES DEL ARCHIVO
  		file_timeZone = prop.getProperty("file.timeZone");
  		file_outputDirectory = prop.getProperty("file.outputDirectory");
  		file_folder = prop.getProperty("file.folder"); 
  		
  		//PROPIEDADES DE ELASTICSEARCH
  		ipServerElasticSearch = prop.getProperty("elasticSearch.ipServer");
  		portServerElasticSearch = Integer.parseInt(prop.getProperty("elasticSearch.portServer"));
  		schemaElasticSearch = prop.getProperty("elasticSearch.schema"); 
  		indexElasticSearch = prop.getProperty("elasticSearch.index");
  		sizeHitsElasticSearch = Integer.parseInt(prop.getProperty("elasticSearch.sizeHits"));
  		responseCodeQuery = Integer.parseInt(prop.getProperty("elasticSearch.response.code")); 
    }
    
    /**
     * Method creates CSV file and send to Bulk
     * 
     * @param fileName
     * @throws IOException
     */
    @SuppressWarnings("static-access")
	public static void generateCSVFile() throws IOException {
    	System.out.println(dateFormat.format(new Date()) + " - Generating CSV file...");
		logger.info("Generating CSV file...");
		
    	/*SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().query(QueryBuilders.rangeQuery("@timestamp").from("2021-07-21T00:00:00").to("2021-07-21T23:59:59"));
    	searchSourceBuilder.timeout(new TimeValue(30000));*/
		
		BoolQueryBuilder orQuery = QueryBuilders.boolQuery();
		
		//SECCION OR DEL QUERY EN ELASTICSEARCH
		//INSERTS/UPDATES/DELETES DE JOSE COBENA
		BoolQueryBuilder storeQueryBuilder = QueryBuilders.boolQuery();
		storeQueryBuilder.must(QueryBuilders.existsQuery("response.insertCdvRecordsBalanceExpiration"));
		storeQueryBuilder.must(QueryBuilders.matchQuery("response.insertCdvRecordsBalanceExpiration.error.code",responseCodeQuery));
		
		orQuery.should(storeQueryBuilder);
		
		storeQueryBuilder = QueryBuilders.boolQuery();
		storeQueryBuilder.must(QueryBuilders.existsQuery("response.deleteCdvMainBalanceExpiration"));
		storeQueryBuilder.must(QueryBuilders.matchQuery("response.deleteCdvMainBalanceExpiration.error.code",responseCodeQuery));
		
		orQuery.should(storeQueryBuilder);
		
		//INSERTS/UPDATES/DELETES DE JOSE GONZALEZ
		storeQueryBuilder = QueryBuilders.boolQuery();
		storeQueryBuilder.must(QueryBuilders.existsQuery("response.insertCDVMain"));
		storeQueryBuilder.must(QueryBuilders.matchQuery("response.insertCDVMain.error.code",responseCodeQuery));
		
		orQuery.should(storeQueryBuilder);
		
		storeQueryBuilder = QueryBuilders.boolQuery();
		storeQueryBuilder.must(QueryBuilders.existsQuery("response.updateCDVMainById"));
		storeQueryBuilder.must(QueryBuilders.matchQuery("response.updateCDVMainById.error.code",responseCodeQuery));
		
		orQuery.should(storeQueryBuilder);
		
		storeQueryBuilder = QueryBuilders.boolQuery();
		storeQueryBuilder.must(QueryBuilders.existsQuery("response.updateCDVRecordsPrevious"));
		storeQueryBuilder.must(QueryBuilders.matchQuery("response.updateCDVRecordsPrevious.error.code",responseCodeQuery));
		
		orQuery.should(storeQueryBuilder);
		
		storeQueryBuilder = QueryBuilders.boolQuery();
		storeQueryBuilder.must(QueryBuilders.existsQuery("response.insertCDVRecords"));
		storeQueryBuilder.must(QueryBuilders.matchQuery("response.insertCDVRecords.error.code",responseCodeQuery));
		
		orQuery.should(storeQueryBuilder);
		
		//INSERTS/UPDATES/DELETES DE WILLIAM BOLIVAR
		
		storeQueryBuilder = QueryBuilders.boolQuery();
		storeQueryBuilder.must(QueryBuilders.existsQuery("response.insertRowInCdvTables"));
		storeQueryBuilder.must(QueryBuilders.matchQuery("response.insertRowInCdvTables.error.code",responseCodeQuery));
		
		orQuery.should(storeQueryBuilder);
		
		storeQueryBuilder = QueryBuilders.boolQuery();
		storeQueryBuilder.must(QueryBuilders.existsQuery("response.updateCDVMAINLifeCycleBalanceRefund"));
		storeQueryBuilder.must(QueryBuilders.matchQuery("response.updateCDVMAINLifeCycleBalanceRefund.error.code",responseCodeQuery));
		
		orQuery.should(storeQueryBuilder);
		
		storeQueryBuilder = QueryBuilders.boolQuery();
		storeQueryBuilder.must(QueryBuilders.existsQuery("response.insertCDVRECORDSLifeCycleBalanceRefund"));
		storeQueryBuilder.must(QueryBuilders.matchQuery("response.insertCDVRECORDSLifeCycleBalanceRefund.error.code",responseCodeQuery));
		
		orQuery.should(storeQueryBuilder);
		
		storeQueryBuilder = QueryBuilders.boolQuery();
		storeQueryBuilder.must(QueryBuilders.existsQuery("response.updateTopaccountTo"));
		storeQueryBuilder.must(QueryBuilders.matchQuery("response.updateTopaccountTo.error.code",responseCodeQuery));
		
		orQuery.should(storeQueryBuilder);
		
		storeQueryBuilder = QueryBuilders.boolQuery();
		storeQueryBuilder.must(QueryBuilders.existsQuery("response.insertTopaccountTo"));
		storeQueryBuilder.must(QueryBuilders.matchQuery("response.insertTopaccountTo.error.code",responseCodeQuery));
		
		orQuery.should(storeQueryBuilder);
				
		//NOMBRE DE LOS WORKFLOWS
		BoolQueryBuilder orQueryWorkflowId = QueryBuilders.boolQuery();
		
		//WORKFLOW DE JOSE COBENA
		storeQueryBuilder = QueryBuilders.boolQuery();
		storeQueryBuilder.must(QueryBuilders.matchQuery("workFlowId", "lifecycle_expire_frozen_balance"));
		
		orQueryWorkflowId.should(storeQueryBuilder);
		
		//WORKFLOW DE JOSE GONZALEZ
		storeQueryBuilder = QueryBuilders.boolQuery();
		storeQueryBuilder.must(QueryBuilders.matchQuery("workFlowId", "freezing_balance"));
		
		orQueryWorkflowId.should(storeQueryBuilder);
		
		//WORKFLOW DE WILLIAM BOLIVAR
		storeQueryBuilder = QueryBuilders.boolQuery();
		storeQueryBuilder.must(QueryBuilders.matchQuery("workFlowId", "life-cycle-balance-refund"));
		
		orQueryWorkflowId.should(storeQueryBuilder);
			
		//FIN DE SECCION OR DEL QUERY EN ELASTICSEARCH
		
		SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
		BoolQueryBuilder qb = QueryBuilders.boolQuery();
		qb
		 //.must(QueryBuilders.matchQuery("workFlowId", "lifecycle_expire_frozen_balance"))//EQUAL
		 .must(orQueryWorkflowId)//OR
		 .must(QueryBuilders.matchQuery("serviceName", "mysql-lifecycle-service"))//EQUAL
		 .must(orQuery)//OR
		 .must(QueryBuilders.rangeQuery("@timestamp").from(dateFrom).to(dateTo));//BETWEEN
		
		searchSourceBuilder.query(qb);
		searchSourceBuilder.sort(new FieldSortBuilder("@timestamp").order(SortOrder.ASC));//ORDER BY
		searchSourceBuilder.timeout(new TimeValue(30 * 1000));//30 SEGUNDOS
		
		System.out.println(dateFormat.format(new Date()) + " - Query executed:" + searchSourceBuilder.toString());
		logger.info("Query executed:" + searchSourceBuilder.toString());
		
    	SearchRequest searchRequest = new SearchRequest();
    	searchRequest.indices(tn3ElasticSearch.getIndex());
    	searchRequest.source(searchSourceBuilder.size(sizeHitsElasticSearch));
    	
		SearchResponse searchResponse = client.search(searchRequest, RequestOptions.DEFAULT);
		
		if (searchResponse.getHits().getTotalHits() > 0) {
			//CREO EL ARCHIVO CSV
	    	File file = new File(file_outputDirectory);
			if (!file.exists()) {
				file.mkdir();
			}
			
			Clock clock = Clock.system(ZoneId.of(file_timeZone));
		  	  
			SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
			Date date = new Date(clock.millis());
			String time = simpleDateFormat.format(date);
			
			String csvNameFile = "logs_appolo_lifecycle"+"_"+time+"-"+bulk_id+".csv";
			String filePath = file_outputDirectory + csvNameFile;
			
			System.out.println(dateFormat.format(new Date()) + " - CSV file name: "+ filePath);
			logger.info("CSV file name: "+ filePath);
						
			FileOutputStream fileOutputStream = new FileOutputStream(filePath);
			OutputStreamWriter out = new OutputStreamWriter(fileOutputStream, "UTF-8");
		    writer = new CSVWriter(out,
				    CSVWriter.DEFAULT_SEPARATOR,
				    CSVWriter.NO_QUOTE_CHARACTER,
				    CSVWriter.DEFAULT_ESCAPE_CHARACTER,
				    CSVWriter.RFC4180_LINE_END);
		    
		    System.out.println("getTotalHits(): " + searchResponse.getHits().getTotalHits());
		   
			SearchHits hits = searchResponse.getHits();
			SearchHit hit[] = hits.getHits();
			
			List<String[]> allElements = new ArrayList<String[]>();
			
			//System.out.println("hit.length: " +  hit.length);
			
			System.out.println(dateFormat.format(new Date()) + " - Getting content of file...");
			logger.info("Getting content of file...");
						
			for (int i = 0; i < hit.length; i++) {
				
				System.out.println("======================== Record No. " + (i+1) + " ======================== ");
				logger.info("======================== Record No. " + (i+1) + " ========================= ");
				
				System.out.println("transactionID: " + hit[i].getSourceAsMap().get("transactionID"));
				logger.info("transactionID: " + hit[i].getSourceAsMap().get("transactionID"));
				
				System.out.println("@timestamp: " + hit[i].getSourceAsMap().get("@timestamp"));
				logger.info("@timestamp: " + hit[i].getSourceAsMap().get("@timestamp"));
				
				System.out.println("workFlowId: " + hit[i].getSourceAsMap().get("workFlowId"));
				logger.info("workFlowId: " + hit[i].getSourceAsMap().get("workFlowId"));
				
				System.out.println("serviceName: " + hit[i].getSourceAsMap().get("serviceName"));
				logger.info("serviceName: " + hit[i].getSourceAsMap().get("serviceName"));
				
				System.out.println("_source: " + hit[i].getSourceAsMap().toString());
				logger.info("_source: " + hit[i].getSourceAsMap().toString());
				
				// contenido del documento
				String[] content = new String[]{
						tn3ElasticSearch.getDataRequest(hit[i],"msisdn"),//1
						tn3ElasticSearch.getDataRequest(hit[i],"mainId"),//2
						tn3ElasticSearch.getDataRequest(hit[i],"creationDate"),//3
						tn3ElasticSearch.getDataRequest(hit[i],"rechargeValue"),//4
						tn3ElasticSearch.getDataRequest(hit[i],"balance"),//5
						tn3ElasticSearch.getDataRequest(hit[i],"balanceT"),//6
						tn3ElasticSearch.getDataRequest(hit[i],"startRecharge"),//7
						tn3ElasticSearch.getDataRequest(hit[i],"endRecharge"),//8
						tn3ElasticSearch.getDataRequest(hit[i],"expirationDate"),//9
						tn3ElasticSearch.getDataRequest(hit[i],"status"),//10
						tn3ElasticSearch.getDataRequest(hit[i],"op"),//11
						tn3ElasticSearch.getDataRequest(hit[i],"mode"),//12
						tn3ElasticSearch.getDataRequest(hit[i],"last"),//13
						tn3ElasticSearch.getDataRequest(hit[i],"fileName"),//14
						tn3ElasticSearch.getDataRequest(hit[i],"modifiedDate"),//15
						tn3ElasticSearch.getDataRequest(hit[i],"fileReceivedDate"),//16
						tn3ElasticSearch.getDataRequest(hit[i],"fileProcessedDate"),//17
						tn3ElasticSearch.getCodeQuery(hit[i],"insertCdvRecordsBalanceExpiration"),//18
						tn3ElasticSearch.getCodeQuery(hit[i],"deleteCdvMainBalanceExpiration"),//19
						tn3ElasticSearch.getCodeQuery(hit[i],"insertCDVMain"),//20
						tn3ElasticSearch.getCodeQuery(hit[i],"updateCDVMainById"),//21
						tn3ElasticSearch.getCodeQuery(hit[i],"updateCDVRecordsPrevious"),//22
						tn3ElasticSearch.getCodeQuery(hit[i],"insertCDVRecords"),//23
						tn3ElasticSearch.getCodeQuery(hit[i],"insertRowInCdvTables"),//24
						tn3ElasticSearch.getCodeQuery(hit[i],"updateCDVMAINLifeCycleBalanceRefund"),//25
						tn3ElasticSearch.getCodeQuery(hit[i],"insertCDVRECORDSLifeCycleBalanceRefund"),//26
						tn3ElasticSearch.getCodeQuery(hit[i],"updateTopaccountTo"),//27
						tn3ElasticSearch.getCodeQuery(hit[i],"insertTopaccountTo"),//28
						tn3ElasticSearch.getDataRequest(hit[i],"MessageID")//29
						};
					
					//System.out.println(" - content " + content);
				
					allElements.add(content);
					
					System.out.println("===============================================================");
					logger.info("===============================================================");
					
				}//FIN DE for (int i = 0; i < hit.length; i++) {
			
				writer.writeAll(allElements);
				writer.close();
				
				System.out.println(dateFormat.format(new Date()) + " - Total records found:" + searchResponse.getHits().getTotalHits());
				logger.info("Total records found:" + searchResponse.getHits().getTotalHits());
					
				System.out.println(dateFormat.format(new Date()) + " - CSV file generated.");
				logger.info("CSV file generated.");
				
				tn3ElasticSearch.finishConnection();
				
				System.out.println(dateFormat.format(new Date()) + " - Connection to ElasticSearch closed.");
				logger.info("Connection to ElasticSearch closed.");
				
				//ENVIO ARCHIVO VIA SFTP AL DIRECTORIO DEL BULK
    			sendFtp(bulk_ip, bulk_port, bulk_user, bulk_pass, bulk_sftp, bulk_path, csvNameFile, filePath);
    			
						
		} else {
			
			System.out.println(dateFormat.format(new Date()) + " - No results matching the criteria.");
			logger.info("No results matching the criteria.");
			
		}//FIN DE if (searchResponse.getHits().getTotalHits() > 0) {
		
    }	
    
    /**
     * Method sends the file to directoy via SFTP.
     * 
	 * @param fileName
	 * @throws IOException
	 */
    public static void sendFile(String fileName) throws IOException {
		
    	//CREO DIRECTORIO DONDE SE ALMACENA EL ARCHIVO CON EL FORMATO PARA EL BULK
      	Files.createDirectories(Paths.get(file_outputDirectory));
      	
    	//CREA NUEVO ARCHIVO CON EL FORMATO PARA EL BULK
    	System.out.println(dateFormat.format(new Date()) + " - Creating file with bulk format in the directory " + file_outputDirectory + "...");
  		logger.info("Creating file with bulk format in the directory " + file_outputDirectory + "...");
    	Clock clock = Clock.system(ZoneId.of(file_timeZone));
  	  
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd_hhmmss");
		Date date = new Date(clock.millis());
		String time = simpleDateFormat.format(date);
	  
		String[] fileNameSplit = fileName.split("\\.");
		
		String destFileName = fileNameSplit[0]+"_"+time+"-"+bulk_id+"."+fileNameSplit[1];
		//String destFileName = "C:\\tmp\\prueba_"+time+"_"+bulk_id+"."+fileNameSplit[1];
		
		File source = new File(file_folder+fileName);
        File dest = new File(file_outputDirectory+destFileName);
        
        if (!dest.exists()) {
        	dest.createNewFile();
        }
		
        //COPIO EL CONTENIDO DEL ARCHIVO DE LOGS DE KIBANA A UN ARCHIVO CON EL FORMATO QUE EL BULK LEE
        Tn3File.copyFileUsingStream(source, dest);
              	
        System.out.println(dateFormat.format(new Date()) + " - Bulk file created: " + destFileName + " in the directory " + file_outputDirectory +".");
  		logger.info("Bulk file created: " + destFileName + " in the directory " + file_outputDirectory +".");
  		
    	//ENVIO ARCHIVO VIA FTP
		sendFtp(bulk_ip, bulk_port, bulk_user, bulk_pass, bulk_sftp, bulk_path, destFileName, dest.getAbsolutePath());
		
    }
    
    /**
	 * Method sends the file to SFTP/FTP Server
	 *
	 * @param ftp_ip
	 * @param ftp_port
	 * @param ftp_user
	 * @param ftp_pass
	 * @param ftp_sftp
	 * @param fileName
	 * @param absolutePathFile
	 * 
	 */
	 private static void sendFtp(String ftp_ip, String ftp_port, String ftp_user, String ftp_pass, boolean ftp_sftp, String bulk_path, String fileName, String absolutePathFile) {
		
		if (ftp_sftp) {	
			 
			System.out.println(dateFormat.format(new Date()) + " - Sending file " + absolutePathFile + " via SFTP to " + ftp_ip + ":" + ftp_port + "...");
			logger.info("Sending file " + absolutePathFile + " via SFTP to " + ftp_ip + ":" + ftp_port + "...");	
						
	      	Tn3SFTP sftp = new Tn3SFTP(ftp_ip, ftp_port, ftp_user, ftp_pass);
	      	sftp.setDestinationDir(bulk_path);
	      	
    	   	if (!sftp.uploadFileToFTP(fileName, absolutePathFile, false).isEmpty()) {
	      		System.out.println(dateFormat.format(new Date()) + " - The file " + absolutePathFile + " sent via SFTP correctly.");
	      		logger.info("The file " + absolutePathFile + " sent via SFTP correctly.");
	      	
	      	}else {
	      		System.out.println(dateFormat.format(new Date()) + " - The file " + absolutePathFile + " didn't send via SFTP correctly.");
	      		logger.info("The file " + absolutePathFile + " didn't send via SFTP correctly.");
	      	}
	      	
		}
	
	 }
	 
}
