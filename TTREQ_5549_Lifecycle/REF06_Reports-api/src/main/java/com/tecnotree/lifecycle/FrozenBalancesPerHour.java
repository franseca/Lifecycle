package com.tecnotree.lifecycle;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Clock;
import java.time.ZoneId;
import java.util.Date;
import java.util.Properties;
import java.util.regex.Pattern;
//import core.Tn3FTP;
//import core.Tn3SFTP;
import java.util.zip.GZIPOutputStream;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.tecnotree.lifecycle.json.LogJson;
import com.tecnotree.tools.Tn3Date;
import com.tecnotree.tools.Tn3FTP;
import com.tecnotree.tools.Tn3GZIP;
import com.tecnotree.tools.Tn3Logger;
import com.tecnotree.tools.Tn3MySQL;
import com.tecnotree.tools.Tn3SFTP;

@SuppressWarnings("unused")
public class FrozenBalancesPerHour {
    
	//CONSTANTES
	private static String PROP_FILE_NAME = "configurationFrozenBalancesPerHour.properties";
  	
	//VARIABLES
	private static Tn3MySQL tMySql = null;
	private static Tn3Logger logger = null;
	private static String db_ipServer = "", db_portServer = "", db_user = "", db_password = "", db_schema = "";
	private static String file_name = "", file_ext = "", file_timeZone = "", file_separator = "", file_outputDirectory = "";
	private static String log_outputDirectory = "";
	private static String ftp_ip = "", ftp_port = "", ftp_user = "", ftp_pass = "", ftp_path = "";
	private static String ftp2_ip = "", ftp2_port = "", ftp2_user = "", ftp2_pass = "", ftp2_path = "";
	private static boolean ftp_sftp = false, ftp2_sftp = false;
	private static String[] infoFtp = null;
	private static GZIPOutputStream outGzip = null;
		
	private static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	private static DateFormat dateFormatLog = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");
	
	private static String process="";
	private static String date="";
	private static String lastRecords="";
	private static Integer dateOption= 0;
	
	private static String pattern1="^(\\d{4})(\\/|-)(0[1-9]|1[0-2])\\2([0-2][0-9]|3[0-1])$";
	private static String pattern2="^(\\d{4})(\\/|-)(0[1-9]|1[0-2])$";
	private static String pattern3="^(\\d{4})$";
	
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
	  	
  		//OBTENGO LOS PARAMETROS
  		if(args.length == 0) {//SI NO SE PASARON PARAMETROS
  			System.out.println(dateFormat.format(new Date()) + " - No se envio ningun parametro.");
  			
  		}else {
  			
  			process = args[0];
  			
  			if(process.equalsIgnoreCase("A")) {//SI EL PROCESO ES AUTOMATICO
  				
  				try {
  					//CARGO EL ARCHIVO DE PROPIEDADES
			  		loadProperties(PROP_FILE_NAME);
			  		
  				}catch (IOException|SecurityException|NullPointerException|NumberFormatException e) {
	  		  		System.out.println(dateFormat.format(new Date()) + " - Exception found - " + e.getMessage());
	  		  		System.out.println(dateFormat.format(new Date()) + " - Finished Frozen Balances per Hour Report.");
	  		  		return;
	  		  	}
  				
  				//SETEO FECHA ACTUAL
  				Clock clock = Clock.system(ZoneId.of(file_timeZone));
	  			  
  				SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
  				Date dateToday = new Date(clock.millis());
  				
  				//SE RESTA UN DIA
  				date = simpleDateFormat.format(Tn3Date.subtractDays(dateToday, 1));
  				
  				try {
		  				
			  		//CREO EL DIRECTORIO Y EL ARCHIVO DE LOGS
			  		logger = new Tn3Logger("FrozenBalancesPerHour", log_outputDirectory);
			  		
			  		logger.info("Command executed: FrozenBalancesPerHour "+ args[0]);
			  		System.out.println(dateFormat.format(new Date()) + " - Command executed: FrozenBalancesPerHour "+ args[0]);
			  						  		
			  		logger.info("Properties file load.");
			  		System.out.println(dateFormat.format(new Date()) + " - Starting Frozen Balances per Hour Report...");
			  		logger.info("Starting Frozen Balances per Hour Report...");
			  		
					//GENERO EL ARCHIVO DE SALIDA
					generateFrozenBalancesPerHour();
	  			
	  			}catch (IOException|SecurityException|SQLException|ClassNotFoundException|NullPointerException|NumberFormatException e) {
	  		  		System.out.println(dateFormat.format(new Date()) + " - Exception found - " + e.getMessage());
	  		  		System.out.println(dateFormat.format(new Date()) + " - Finished Frozen Balances per Hour Report.");
	  		  		logger.severe(e);
	  		  		//e.printStackTrace();
	  		  		System.exit(1);
	  		  	}
  				
  				
  			}else if(process.equalsIgnoreCase("M")) { //SI EL PROCESO ES MANUAL
  			
  				if(args.length < 3) {//SI NO SE PASARON PARAMETROS
  					System.out.println(dateFormat.format(new Date()) + " - Not found the parameters necesaries.");
	  		  		System.out.println(dateFormat.format(new Date()) + " - Finished Frozen Balances per Hour Report.");
	  		  		return;
  				}
  					  				
  				date = args[1];
  				lastRecords = args[2];
		  			  	
		  		//VALIDO QUE LA FECHA SEA DEL FORMATO CORRECTO
		  		if(!Pattern.matches(pattern1, date) && !Pattern.matches(pattern2, date) && !Pattern.matches(pattern3, date)) {
		  			System.out.println(dateFormat.format(new Date()) + " - The parameter DATE don't have the correct format. The formats can be: dd-MM-yyyy | dd-MM | YYYY.");
		  			System.out.println(dateFormat.format(new Date()) + " - Finished Frozen Balances per Hour Report.");
		  			return;
		  		}
		  		
		  		//VALIDO QUE EXISTA UNA OPCION DE FECHA
		  		if(Pattern.matches(pattern1, date)) {
		  			dateOption = 1;
		  			
		  			try {
				  		
			  			//VALIDO SI LA FECHA ES VALIDA
				  		DateFormat formatoFecha = new SimpleDateFormat("yyyy-MM-dd");
				  		formatoFecha.setLenient(false);
				  		formatoFecha.parse(date);
				  		
			  		}catch (NullPointerException|NumberFormatException|ParseException e) {
		  		  		
			  			System.out.println(dateFormat.format(new Date()) + " - Exception found - " + e.getMessage());
		  		  		System.out.println(dateFormat.format(new Date()) + " - Finished Frozen Balances per Hour Report.");
		  		  		return;
		  		  	}
		  			
		  		}else if(Pattern.matches(pattern2, date)) {
		  			dateOption = 2;
		  			
		  			try {
				  		
			  			//VALIDO SI LA FECHA ES VALIDA
				  		DateFormat formatoFecha = new SimpleDateFormat("yyyy-MM");
				  		formatoFecha.setLenient(false);
				  		formatoFecha.parse(date);
				  		
			  		}catch (NullPointerException|NumberFormatException|ParseException e) {
		  		  		
			  			System.out.println(dateFormat.format(new Date()) + " - Exception found - " + e.getMessage());
		  		  		System.out.println(dateFormat.format(new Date()) + " - Finished Frozen Balances per Hour Report.");
		  		  		return;
		  		  	}

		  		}else if(Pattern.matches(pattern3, date)) {
		  			dateOption = 3;
  				}else {
		  			System.out.println(dateFormat.format(new Date()) + " - Don't find date option.");
	  		  		System.out.println(dateFormat.format(new Date()) + " - Finished Frozen Balances per Hour Report.");
	  		  		return;
		  		}
		  			
		  		
		  		//VALIDO QUE EL PARAMETRO DE REGISTRO ULTIMO SE 0 O 1
		  		if(!lastRecords.equals("0") && !lastRecords.equals("1")) {
		  			System.out.println(dateFormat.format(new Date()) + " - The parameter LAST_RECORD don't have the correct format. The formats can be: 0 | 1.");
	  		  		System.out.println(dateFormat.format(new Date()) + " - Finished Frozen Balances per Hour Report.");
	  		  		return;
		  		}
		  		
		  		
		  		try {
  					//CARGO EL ARCHIVO DE PROPIEDADES
			  		loadProperties(PROP_FILE_NAME);
			  		
  				}catch (IOException|SecurityException|NullPointerException|NumberFormatException e) {
	  		  		System.out.println(dateFormat.format(new Date()) + " - Exception found - " + e.getMessage());
	  		  		System.out.println(dateFormat.format(new Date()) + " - Finished Frozen Balances per Hour Report.");
	  		  		return;
	  		  	}
		  		
		  		try {	
			  		//CREO EL DIRECTORIO Y EL ARCHIVO DE LOGS
			  		logger = new Tn3Logger("FrozenBalancesPerHour", log_outputDirectory);
			  		
			  		logger.info("Command executed: FrozenBalancesPerHour "+ args[0] + " " + args[1] + " " + args[2]);
			  		System.out.println(dateFormat.format(new Date()) + " - Command executed: FrozenBalancesPerHour "+ args[0] + " " + args[1] + " " + args[2]);
			  						  		
			  		logger.info("Properties file load.");
			  		System.out.println(dateFormat.format(new Date()) + " - Starting Frozen Balances per Hour Report...");
			  		logger.info("Starting Frozen Balances per Hour Report..");
			  		
					//GENERO EL ARCHIVO DE SALIDA
			  		generateFrozenBalancesPerHour();
  			
	  			}catch (IOException|SecurityException|SQLException|ClassNotFoundException|NullPointerException|NumberFormatException e) {
	  		  		System.out.println(dateFormat.format(new Date()) + " - Exception found - " + e.getMessage());
	  		  		System.out.println(dateFormat.format(new Date()) + " - Finished Frozen Balances per Hour Report.");
	  		  		logger.severe(e);
	  		  		//e.printStackTrace();
	  		  		System.exit(1);
	  		  	}
		  		
  			}else{//SI NO ES AUTOMATICO NI MANUAL
  				
  				System.out.println(dateFormat.format(new Date()) + " - Proccess not found. The proccess can be: A (Automatic) or M (Manual).");
		  		System.out.println(dateFormat.format(new Date()) + " - Finished Frozen Balances per Hour Report.");
									
  			}//FIN DE if(process.equals("M")) {
  			
  		}//FIN DE if(args.length == 0)
	  		
  		System.exit(0);
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
		  			
  		//PROPIEDADES DE LA BASE DE DATOS
  		db_ipServer = prop.getProperty("db.ipServer");
  		db_portServer = prop.getProperty("db.portServer");
  		db_user = prop.getProperty("db.user");
  		db_password = prop.getProperty("db.password");
  		db_schema = prop.getProperty("db.schema");
  		
  		//PROPIEDADES DEL ARCHIVO
  		file_name = prop.getProperty("file.name");
  		file_ext = prop.getProperty("file.ext");
  		file_timeZone = prop.getProperty("file.timeZone");
  		file_separator = prop.getProperty("file.separator");
  		file_outputDirectory = prop.getProperty("file.outputDirectory");
  		
  		//PROPIEDADES DEL LOS LOGS
  		log_outputDirectory = prop.getProperty("log.outputDirectory");
  		
  		//PROPIEDADES DE FTP/SFTP
  		ftp_ip = prop.getProperty("ftp1.ip");
  		ftp_port = prop.getProperty("ftp1.port");
  		ftp_user = prop.getProperty("ftp1.user");
  		ftp_pass = prop.getProperty("ftp1.pass");
  		ftp_path = prop.getProperty("ftp1.path");
  		ftp_sftp = Boolean.parseBoolean(prop.getProperty("ftp1.sftp"));
  		
  		ftp2_ip = prop.getProperty("ftp2.ip");
  		ftp2_port = prop.getProperty("ftp2.port");
  		ftp2_user = prop.getProperty("ftp2.user");
  		ftp2_pass = prop.getProperty("ftp2.pass");
  		ftp2_path = prop.getProperty("ftp2.path");
  		ftp2_sftp = Boolean.parseBoolean(prop.getProperty("ftp2.sftp"));
		
	}
	
	/**
	 * Method generates a file with the frozen balances per hour
	 * 
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 * @throws IOException 
	 */
	private static void generateFrozenBalancesPerHour() throws ClassNotFoundException, SQLException, IOException {
			
		ResultSet rs = null;
		String query = "";
		
		//CREO ARCHIVO DE SALIDA
		//SETEO EL NOMBRE DEL ACHIVO DE SALIDA
		setFileName(file_timeZone);
			
		System.out.println(dateFormat.format(new Date()) + " - Creating ouput file " + file_name + " in the directory " + file_outputDirectory + "...");
		logger.info("Creating ouput file " + file_name + " in the directory " + file_outputDirectory + "...");	
		
		//CREO DIRECTORIO DONDE SE ALMACENA EL ARCHIVO DE SALIDA
		Files.createDirectories(Paths.get(file_outputDirectory));
		File  fichero = new File (file_outputDirectory+file_name);
		PrintWriter writer = new PrintWriter(fichero);
			
		//ESTABLEZCO CONEXION A LA BASE DE DATOS
		System.out.println(dateFormat.format(new Date()) + " - Connecting to database...");
		logger.info("Connecting to database...");	
		
		tMySql = new Tn3MySQL(db_ipServer, db_portServer, db_schema, db_user, db_password);
		//tMySql = new Tn3MySQL();
		
		//CONSULTO LOS REGISTROS
		System.out.println(dateFormat.format(new Date()) + " - Getting data to the report...");
		logger.info("Getting data to the report...");	
		
		if(process.equals("A")) {
			//LA FECHA YA VIENE CON UN DIA MENOS (date)
			query = "SELECT CONCAT(DATE_FORMAT(CDV_RECORDS.CREATION_DATE,\"%Y-%m-%d %H\"),'h','|',COUNT(*),'|',FORMAT(SUM(IFNULL(CDV_RECORDS.BALANCE,0))/1000,3)) RECORD\r\n"
						+ "FROM CDV_RECORDS\r\n"
						+ "WHERE CDV_RECORDS.LAST = 1\r\n"
						+ "AND CDV_RECORDS.STATUS = 'CONGELADO'\r\n"
						+ "AND DATE_FORMAT(CDV_RECORDS.CREATION_DATE,\"%Y-%m-%d\") = '" + date + "'\r\n"
						+ "GROUP BY DATE_FORMAT(CDV_RECORDS.CREATION_DATE,\"%Y-%m-%d %H\")\r\n"
						+ "ORDER BY DATE_FORMAT(CDV_RECORDS.CREATION_DATE,\"%Y-%m-%d %H\") DESC;";
		}else {
			
			if(dateOption == 1) {//SI SE INGRESO EL PARAMTERO FECHA EN EL FORMATO yyyy-MM-dd
				
				query = "SELECT CONCAT(DATE_FORMAT(CDV_RECORDS.CREATION_DATE,\"%Y-%m-%d %H\"),'h','|',COUNT(*),'|',FORMAT(SUM(IFNULL(CDV_RECORDS.BALANCE,0))/1000,3)) RECORD\r\n"
						+ "FROM CDV_RECORDS\r\n"
						+ "WHERE CDV_RECORDS.LAST = " + lastRecords + "\r\n"
						+ "AND DATE_FORMAT(CDV_RECORDS.CREATION_DATE,\"%Y-%m-%d\") = '" + date + "'\r\n"
						+ "AND CDV_RECORDS.STATUS = 'CONGELADO'\r\n"
						+ "GROUP BY DATE_FORMAT(CDV_RECORDS.CREATION_DATE,\"%Y-%m-%d %H\")\r\n"
						+ "ORDER BY DATE_FORMAT(CDV_RECORDS.CREATION_DATE,\"%Y-%m-%d %H\") DESC;";
				
			}else if (dateOption == 2) {//SI SE INGRESO EL PARAMTERO FECHA EN EL FORMATO yyyy-MM
				
				query = "SELECT CONCAT(DATE_FORMAT(CDV_RECORDS.CREATION_DATE,\"%Y-%m-%d\"),'|',COUNT(*),'|',FORMAT(SUM(IFNULL(CDV_RECORDS.BALANCE,0))/1000,3)) RECORD\r\n"
						+ "FROM CDV_RECORDS\r\n"
						+ "WHERE CDV_RECORDS.LAST = " + lastRecords + "\r\n"
						+ "AND DATE_FORMAT(CDV_RECORDS.CREATION_DATE,\"%Y-%m\") = '"+ date +"'\r\n"
						+ "AND CDV_RECORDS.STATUS = 'CONGELADO'\r\n"
						+ "GROUP BY DATE_FORMAT(CDV_RECORDS.CREATION_DATE,\"%Y-%m-%d\")\r\n"
						+ "ORDER BY DATE_FORMAT(CDV_RECORDS.CREATION_DATE,\"%Y-%m-%d\") DESC;";
				
			}else {// SI SE INGRESO EL PARAMTERO FECHA EN EL FORMATO yyyy
				
				query = "SELECT CONCAT(DATE_FORMAT(CDV_RECORDS.CREATION_DATE,\"%Y-%m\"),'|',COUNT(*),'|',FORMAT(SUM(IFNULL(CDV_RECORDS.BALANCE,0))/1000,3)) RECORD\r\n"
						+ "FROM CDV_RECORDS\r\n"
						+ "WHERE CDV_RECORDS.LAST = " + lastRecords + "\r\n"
						+ "AND DATE_FORMAT(CDV_RECORDS.CREATION_DATE,\"%Y\") = '"+ date +"'\r\n"
						+ "AND CDV_RECORDS.STATUS = 'CONGELADO'\r\n"
						+ "GROUP BY DATE_FORMAT(CDV_RECORDS.CREATION_DATE,\"%Y-%m\")\r\n"
						+ "ORDER BY DATE_FORMAT(CDV_RECORDS.CREATION_DATE,\"%Y-%m\") DESC;";
				
			}
			
		}
			
		System.out.println (query);
		PreparedStatement stmt = tMySql.getConn().prepareStatement(query);
		rs = stmt.executeQuery();
		  
		while (rs.next()) {
			System.out.println (rs.getString("RECORD"));
			//GRABO LOS REGISTROS EN EL ARCHIVO
			writer.println(rs.getString("RECORD"));
		}
						
		rs.close();
		stmt.close();
		  
		//CIERRO CONEXION CON LA BASE DE DATOS
		tMySql.finalizarConexion();
		System.out.println(dateFormat.format(new Date()) + " - Connection to database closed.");
		logger.info("Connection to database closed.");	
				  
		//CIERRO EL ACHIVO
		writer.close();
		
		//GENERO EL ARCHIVO ZIP  
		if(gzipFile(fichero)) {
			
			//ENVIO ARCHIVO DE SALIDA VIA FTP AL 1ER. SERVIDOR
			sendFtp(ftp_ip, ftp_port, ftp_user, ftp_pass, ftp_sftp, ftp_path, file_name, fichero.getAbsolutePath());
			
			//ENVIO ARCHIVO DE SALIDA VIA SFTP AL 2DO. SERVIDOR
			sendFtp(ftp2_ip, ftp2_port, ftp2_user, ftp2_pass, ftp2_sftp, ftp2_path, file_name, fichero.getAbsolutePath());
		}
		
		System.out.println(dateFormat.format(new Date()) + " - Finished Frozen Balances per Hour Report.");
		logger.info("Finished Frozen Balances per Hour Report.");
	}
	 
	/** Method sets the file name of the reports
	 * 
	 * @param file_zonaHoraria
	 */
	private static void setFileName(String file_zonaHoraria) {
		Clock clock = Clock.system(ZoneId.of(file_zonaHoraria));
	  
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyyMMdd_HHmmss");
		Date date = new Date(clock.millis());
		String time = simpleDateFormat.format(date);
	  
		file_name = file_name.replace("yyyyMMdd_hhmmss", time)+file_ext;
	}
  
	/**
	 * Method generates the zip file
	 * 
	 * @param file
	 */
	private static boolean gzipFile(File file) {
		System.out.println(dateFormat.format(new Date()) + " - Generating zip file...");
		logger.info("Generating zip file...");
		
		//CREA EL ARCHIVO GZIP Y BORRA EL ARCHIVO ORIGINAL
		return Tn3GZIP.gzipp(file, true);
	}
	
	/**
	 * Method sends the zip file to SFTP/FTP Server
	 * 
	 * @param absolutePathFile
	 * @throws JsonProcessingException 
	 */
	private static void sendFtp(String ftp_ip, String ftp_port, String ftp_user, String ftp_pass, boolean ftp_sftp, String ftp_path, String file_name, String absolutePathFile) throws JsonProcessingException {
			
		String gzipfileName = absolutePathFile+".gz";
		String fileName = file_name + ".gz";
		
		ObjectMapper mapper = new ObjectMapper();
		mapper.enable(SerializationFeature.WRAP_ROOT_VALUE);
		LogJson logJson = new LogJson();
		logJson.setReport("FrozenBalancesPerHour");
		logJson.setServer(ftp_ip);
  		logJson.setPort(ftp_port);
  		logJson.setFile(fileName);
  		
		if (ftp_sftp) {	
			
			logJson.setProtocol("sftp");
			
			System.out.println(dateFormat.format(new Date()) + " - Sending file " + gzipfileName + " via SFTP to directory " + ftp_path + " of server " + ftp_ip + ":" + ftp_port + "...");
			logger.info("Sending file " + gzipfileName + " via SFTP to directory " + ftp_path + " of server " + ftp_ip + ":" + ftp_port + "...");	
						
	      	Tn3SFTP sftp = new Tn3SFTP(ftp_ip, ftp_port, ftp_user, ftp_pass);
	      	sftp.setDestinationDir(ftp_path);
	      	
    	   	if (!sftp.uploadFileToFTP(fileName, gzipfileName, false).isEmpty()) {
	      		System.out.println(dateFormat.format(new Date()) + " - The file " + gzipfileName + " sent via SFTP correctly.");
	      		logger.info("The file " + gzipfileName + " sent via SFTP correctly.");
	      		
	      		logJson.setStatus("SUCCESS");
	      		logJson.setCode("200");
	      		logJson.setDateTime(dateFormatLog.format(new Date()));
	      		
	      	}else {
	      		System.out.println(dateFormat.format(new Date()) + " - The file " + gzipfileName + " didn't send via SFTP correctly.");
	      		logger.info("The file " + gzipfileName + " didn't send via SFTP correctly.");
	      	
	      		logJson.setStatus("FAILED");
	      		logJson.setCode("500");
	      		logJson.setDateTime(dateFormatLog.format(new Date()));
	      	}
	      	
		}else{
	    	  
			logJson.setProtocol("ftp");
			
			System.out.println(dateFormat.format(new Date()) + " - Sending file " + gzipfileName + " via FTP to directory " + ftp_path + " of server " + ftp_ip + ":" + ftp_port + "...");
			logger.info("Sending file " + gzipfileName + " via FTP to directory " + ftp_path + " of server " + ftp_ip + ":" + ftp_port + "...");	
			
	      	infoFtp = new String[] {ftp_ip,ftp_port,ftp_user,ftp_pass,gzipfileName};
	      	if (Tn3FTP.upload(infoFtp, ftp_path)) {
	      		System.out.println(dateFormat.format(new Date()) + " - The file " + gzipfileName + " sent via FTP correctly.");
	      		logger.info("The file " + gzipfileName + " sent via FTP correctly.");
	      		
	      		logJson.setStatus("SUCCESS");
	      		logJson.setCode("200");
	      		logJson.setDateTime(dateFormatLog.format(new Date()));
	      	
	      	}else{
	      		System.out.println(dateFormat.format(new Date()) + " - The file " + gzipfileName + " didn't send via FTP correctly.");
	      		logger.info("The file " + gzipfileName + " didn't send via FTP correctly.");
	      		
	      		logJson.setStatus("FAILED");
	      		logJson.setCode("500");
	      		logJson.setDateTime(dateFormatLog.format(new Date()));
	      	
	      	}
		}
		
		logger.info(mapper.writeValueAsString(logJson));
	}
  
}

	