package com.tecnotree.lifecycle;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
//import core.Tn3FTP;
//import core.Tn3SFTP;
import java.util.zip.GZIPOutputStream;

import com.tecnotree.tools.Tn3FTP;
import com.tecnotree.tools.Tn3GZIP;
import com.tecnotree.tools.Tn3Logger;
import com.tecnotree.tools.Tn3MySQL;
import com.tecnotree.tools.Tn3SFTP;

@SuppressWarnings("unused")
public class PictureTotalFrozenBalancesPerDay {
    
	//CONSTANTES
	private static String PROP_FILE_NAME = "configurationPictureTotalFrozenBalancesPerDay.properties";
  	
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
	
	private static String process="";
	private static String date="";
	//private static String lastRecords="";
	private static Integer dateOption= 0;
	
	private static String pattern1="^(\\d{4})(\\/|-)(0[1-9]|1[0-2])\\2([0-2][0-9]|3[0-1])$";
	private static String pattern2="^(\\d{4})(\\/|-)(0[1-9]|1[0-2])$";
	private static String pattern3="^(\\d{4})$";
	
	public static void main(String[] args) throws ClassNotFoundException, SQLException {
	  	
		try {
			//CARGO EL ARCHIVO DE PROPIEDADES
	  		loadProperties(PROP_FILE_NAME);
	  		
		}catch (IOException|SecurityException|NullPointerException|NumberFormatException e) {
	  		System.out.println(dateFormat.format(new Date()) + " - Exception found - " + e.getMessage());
	  		System.out.println(dateFormat.format(new Date()) + " - Finished Picture Total Frozen Balances Per Day Report.");
	  		return;
	  	}
		
		try {
  				
	  		//CREO EL DIRECTORIO Y EL ARCHIVO DE LOGS
	  		logger = new Tn3Logger("PictureTotalFrozenBalancesPerDay", log_outputDirectory);
	  		
	  		logger.info("Command executed: PictureTotalFrozenBalancesPerDay ");
	  		System.out.println(dateFormat.format(new Date()) + " - Command executed: PictureTotalFrozenBalancesPerDay ");
	  						  		
	  		logger.info("Properties file load.");
	  		System.out.println(dateFormat.format(new Date()) + " - Starting Picture Total Frozen Balances Per Day Report...");
	  		logger.info("Starting Picture Total Frozen Balances Per Day Report...");
	  		
			//GENERO EL ARCHIVO DE SALIDA
	  		generateTotalFrozenBalancesPerDay();
		
		}catch (IOException|SecurityException|SQLException|ClassNotFoundException|NullPointerException|NumberFormatException e) {
	  		System.out.println(dateFormat.format(new Date()) + " - Exception found - " + e.getMessage());
	  		System.out.println(dateFormat.format(new Date()) + " - Finished Picture Total Frozen Balances Per Day Report.");
	  		logger.severe(e);
	  		//e.printStackTrace();
	  		System.exit(1);
	  	}
		
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
	 * Method generates a file with the picture total frozen balances per day
	 * 
	 * @throws ClassNotFoundException
	 * @throws SQLException
	 * @throws IOException 
	 */
	private static void generateTotalFrozenBalancesPerDay() throws ClassNotFoundException, SQLException, IOException {
			
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
		FileWriter writer = new FileWriter (fichero,true);
			
		/*Iterator s = customerIterator();
        if (s.hasNext()==false){
            System.out.println("Empty");
        }
        
        while(s.hasNext()){
            Customer current  = (Customer) s.next();
            System.out.println(current.toString()+"\n");
            pw.append(current.getName());
            pw.append(",");
            pw.append(current.getAddress());
            pw.append("\n");
        }*/
        
		//ESTABLEZCO CONEXION A LA BASE DE DATOS
		System.out.println(dateFormat.format(new Date()) + " - Connecting to database...");
		logger.info("Connecting to database...");	
		
		tMySql = new Tn3MySQL(db_ipServer, db_portServer, db_schema, db_user, db_password);
		//tMySql = new Tn3MySQL();
		
		//CONSULTO LOS REGISTROS
		System.out.println(dateFormat.format(new Date()) + " - Getting data to the report...");
		logger.info("Getting data to the report...");	
		
		query = "SELECT CONCAT(DATE_FORMAT(NOW(),\"%Y-%m-%d %H:%i:%s\"),'|', COUNT(*),'|', FORMAT(SUM(IFNULL(CDV_RECORDS.BALANCE_T,0))/1000,3)) RECORD\r\n"
				+ "FROM CDV_RECORDS\r\n"
				+ "WHERE CDV_RECORDS.LAST = 1\r\n"
				+ "AND CDV_RECORDS.STATUS = 'CONGELADO'\r\n"
				+ "AND CDV_RECORDS.BALANCE_T > 0;";
				
		System.out.println (query);
		PreparedStatement stmt = tMySql.getConn().prepareStatement(query);
		rs = stmt.executeQuery();
		  
		while (rs.next()) {
			System.out.println (rs.getString("RECORD"));
			//GRABO LOS REGISTROS EN EL ARCHIVO
			writer.append(rs.getString("RECORD")+"\n");
		}
						
		rs.close();
		stmt.close();
		  
		//CIERRO CONEXION CON LA BASE DE DATOS
		tMySql.finalizarConexion();
		System.out.println(dateFormat.format(new Date()) + " - Connection to database closed.");
		logger.info("Connection to database closed.");	
				  
		//CIERRO EL ACHIVO
		writer.flush();
		writer.close();
	
		//ENVIO ARCHIVO DE SALIDA VIA FTP AL 1ER. SERVIDOR
		sendFtp(ftp_ip, ftp_port, ftp_user, ftp_pass, ftp_sftp, ftp_path, file_name, fichero.getAbsolutePath());
		
		//ENVIO ARCHIVO DE SALIDA VIA FTP AL 2DO. SERVIDOR
		sendFtp(ftp2_ip, ftp2_port, ftp2_user, ftp2_pass, ftp2_sftp, ftp2_path, file_name, fichero.getAbsolutePath());
		
		System.out.println(dateFormat.format(new Date()) + " - Finished Picture Total Frozen Balances Per Day Report.");
		logger.info("Finished Picture Total Frozen Balances Per Day Report.");
	}
	 
	/** Method sets the file name of the reports
	 * 
	 * @param file_zonaHoraria
	 */
	private static void setFileName(String file_zonaHoraria) {
		file_name = file_name+file_ext;
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
	 * Method sends the txt file to SFTP/FTP Server
	 * 
	 * @param absolutePathFile
	 */
	private static void sendFtp(String ftp_ip, String ftp_port, String ftp_user, String ftp_pass, boolean ftp_sftp, String ftp_path, String file_name, String absolutePathFile) {
			
		String gzipfileName = absolutePathFile;
		String fileName = file_name;
		if (ftp_sftp) {	
			
			System.out.println(dateFormat.format(new Date()) + " - Sending file " + gzipfileName + " via SFTP to directory " + ftp_path + " of server " + ftp_ip + ":" + ftp_port + "...");
			logger.info("Sending file " + gzipfileName + " via SFTP to directory " + ftp_path + " of server " + ftp_ip + ":" + ftp_port + "...");	
						
	      	Tn3SFTP sftp = new Tn3SFTP(ftp_ip, ftp_port, ftp_user, ftp_pass);
	      	sftp.setDestinationDir(ftp_path);
	      	
    	   	if (!sftp.uploadFileToFTP(fileName, gzipfileName, false).isEmpty()) {
	      		System.out.println(dateFormat.format(new Date()) + " - The file " + gzipfileName + " sent via SFTP correctly.");
	      		logger.info("The file " + gzipfileName + " sent via SFTP correctly.");
	      	
	      	}else {
	      		System.out.println(dateFormat.format(new Date()) + " - The file " + gzipfileName + " didn't send via SFTP correctly.");
	      		logger.info("The file " + gzipfileName + " didn't send via SFTP correctly.");
	      	}
	      	
		}else{
	    	  
			System.out.println(dateFormat.format(new Date()) + " - Sending file " + gzipfileName + " via FTP to " + ftp_ip + ":" + ftp_port + "...");
			logger.info("Sending file " + gzipfileName + " via FTP to " + ftp_ip + ":" + ftp_port + "...");	
				
	      	infoFtp = new String[] {ftp_ip,ftp_port,ftp_user,ftp_pass,gzipfileName};
	      	if (Tn3FTP.uploadd(infoFtp)) {
	      		System.out.println(dateFormat.format(new Date()) + " - The file " + gzipfileName + " sent via FTP correctly.");
	      		logger.info("The file " + gzipfileName + " sent via FTP correctly.");
	      	}else{
	      		System.out.println(dateFormat.format(new Date()) + " - The file " + gzipfileName + " didn't send via FTP correctly.");
	      		logger.info("The file " + gzipfileName + " didn't send via FTP correctly.");
	      	}
		}
	}
  
}

	