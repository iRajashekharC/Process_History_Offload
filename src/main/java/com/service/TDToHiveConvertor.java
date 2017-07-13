package com.service;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;
import java.util.HashMap;
import java.util.List;
import java.io.File;
import java.io.FileInputStream;
import java.io.BufferedReader;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.InputStreamReader;

import org.apache.log4j.Logger;

import com.util.AIMConvertorUtils;

public class TDToHiveConvertor {
	private static final String DDL_CREATE_TABLE = "CREATE TABLE ";
	private static final String DDL_ROW_FORMAT_DELIMITED_FIELDS_TERMINATED_BY = "ROW FORMAT DELIMITED FIELDS TERMINATED BY '|';";

	private static final String CREATE_DATABASE_comments = "--Creating the database if not exists\n";
	private static final String CREATE_DATABASE = "CREATE DATABASE IF NOT EXISTS ${hivevar:hv_database};";

	private static final String USE_DATABASE_comments = "--Logging into the database before executing the below HQLs\n";
	private static final String USE_DATABASE = "USE ${hivevar:hv_database};";

	private static final String DROP_TABLE_comments = "--Dropping the base table if exists\n";
	private static final String DROP_TABLE = "DROP TABLE IF EXISTS ${hivevar:hv_database}.${hivevar:hv_table};";

	private static final String CREATE_TABLE_comments = "--Creating the base table \n";
	private static final String CREATE_TABLE = "CREATE TABLE IF NOT EXISTS ${hivevar:hv_database}.${hivevar:hv_table}";
	private static final String STORED_AS = "\nSTORED AS ORC;";

	private static final String DROP_EXTERNAL_TABLE_comments = "--Dropping the stage table if exists\n";
	private static final String DROP_EXTERNAL_TABLE = "DROP TABLE IF EXISTS ${hivevar:hv_database}.${hivevar:hv_table}_stg;";

	private static final String CREATE_EXTERNAL_TABLE_comments = "--Creating the stage table to store raw data\n";
	private static final String CREATE_EXTERNAL_TABLE = "CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:hv_database}.${hivevar:hv_table}_stg";
	private static final String ROW_FORMAT_DELIMITED_FIELDS_TERMINATED_BY = "ROW FORMAT DELIMITED \n  FIELDS TERMINATED BY '|' \n  LINES TERMINATED BY '\\n'";
	private static final String LOCATION = "LOCATION \n  '${hivevar:hv_loc}';";

	private static final String INSERT_INTO_TABLE_comments = "--Inserting data from the stage table into the base table\n";
	private static final String INSERT_INTO_TABLE = "INSERT INTO TABLE ${hivevar:hv_database}.${hivevar:hv_table}";
	private static final String SELECT_FROM_TABLE = "SELECT * FROM ${hivevar:hv_database}.${hivevar:hv_table}_stg;";
	private static final String FROM_TABLE = "FROM ${hivevar:hv_database}.${hivevar:hv_table}_stg;";

	private static String dbName = "";
	private static String tblName = "";

	Map<Integer, String> columnNameMap = new HashMap<Integer, String>();
	Map<String, String> columnDataTypeMap = new HashMap<String, String>();
	Map<String, String> lastColumnMap = new HashMap<String, String>();

	boolean partition = false;
	String partitionColumn = "";

	public static final String UTF8_BOM = "\uFEFF";

	static Map<String, String> tdHiveDataTypeMapping = new HashMap<String, String>();

	static {
		tdHiveDataTypeMapping.put("BYTEINT", "TINYINT");
		tdHiveDataTypeMapping.put("SMALLINT", "SMALLINT");
		tdHiveDataTypeMapping.put("INTEGER", "INT");
		tdHiveDataTypeMapping.put("INT", "INT");
		tdHiveDataTypeMapping.put("BIGINT", "BIGINT");
		tdHiveDataTypeMapping.put("DECIMAL", "DECIMAL");
		tdHiveDataTypeMapping.put("NUMERIC", "DECIMAL");
		tdHiveDataTypeMapping.put("FLOAT", "DOUBLE");
		tdHiveDataTypeMapping.put("CHAR", "CHAR"); // available after hive 0.13
		tdHiveDataTypeMapping.put("VARCHAR", "VARCHAR");
		tdHiveDataTypeMapping.put("DATE", "DATE"); // formatting is needed as
													// per needs
		tdHiveDataTypeMapping.put("TIME", "STRING");
		tdHiveDataTypeMapping.put("TIMESTAMP", "TIMESTAMP");
	}

	final static Logger logger = Logger.getLogger(TDToHiveConvertor.class);

	ArrayList<Integer> arrayList = new ArrayList<Integer>();
	int maxLength = 0;

	public static void main(String[] args) {
		new TDToHiveConvertor().init(null);
	}

	public void init(Map<String, String> inParams) {
			String tempDir = inParams.get("tmpDir");
			System.out.println("Temp dir is " + tempDir);
			String skipJDBC = inParams.get("skipJDBCConnection");
			try {

				if (null != skipJDBC && "false".equals(skipJDBC)) {
					// logger.info("Getting DDLs from MySQL...");
					getDDLsFromMySql(inParams, tempDir);
					// logger.info("Retrieved DDLs from MySQL...");
				} 

			File mainFolder = new File(tempDir);
			File files[];
			files = mainFolder.listFiles();

			logger.info("Starting DDL Conversion to HIVE...");
			for (int i = 0; i < files.length; i++) {
				columnNameMap.clear();
				FileInputStream fis = new FileInputStream(files[i]);
				InputStreamReader isr = new InputStreamReader(fis, "UTF-8");
				BufferedReader br = new BufferedReader(isr);

				getCommonDDLs(br);

				// for DDL creation - start
				StringBuffer hiveDDL = buildHiveDDL(br);
				new File(tempDir + "/hiveDDL").mkdir();
				String hiveDDLLoc = tempDir + "/hiveDDL" + "/" + tblName + ".txt";

				logger.info("Writing the HIVE DDL to output folder");
				BufferedWriter bwDDL = new BufferedWriter(new FileWriter(hiveDDLLoc));
				bwDDL.write(hiveDDL.toString());
				bwDDL.close();
				// for DDL creation - end

				// for HQL creation - start
				StringBuffer commonDDLs = getHQLCommonParts();
				StringBuffer extrnlTblDDL = buildHiveExternalTbl(commonDDLs);
				StringBuffer createTblDDL = buildHiveMngdTbl(extrnlTblDDL);
				StringBuffer hiveHQL = insertIntoMngdTblAndDropStgTbl(createTblDDL);

				new File(tempDir + "/hiveHQL").mkdir();
				String hiveHQLLoc = tempDir + "/hiveHQL" + "/" + tblName.toLowerCase() + "_history.hql";

				logger.info("Writing the HIVE HQL to output folder");
				BufferedWriter bwHQL = new BufferedWriter(new FileWriter(hiveHQLLoc));
				bwHQL.write(hiveHQL.toString());
				bwHQL.close();
				// for HQL creation - end

				logger.info("Finished DDL conversion for " + dbName + "." + tblName);
				logger.info("**************************************************");
			}

			
			logger.info("Finished all teradata DDL Conversion...");
			logger.info("**************************************************");

			List<File> fileList = new ArrayList<File>();
			File directory = new File(tempDir);
			AIMConvertorUtils.getAllFiles(directory, fileList);
			AIMConvertorUtils.writeZipFile(directory, fileList);
			
			System.out.println(
					"Teradata DDL conversion successfully completed for all tables. See logs for additional information.");
		} catch (Exception exception) {
			logger.error(exception);
			System.out.println("Teradata DDL conversion failed with errors. See logs for additional information.");
		}
	}

		
		private void getDDLsFromMySql(Map<String, String> tableList, String tempDir) throws Exception {
			String uName = tableList.get("dbUserName");
			String pswd = tableList.get("dbPassword");
			String serName = tableList.get("serverName");
			String dbName = tableList.get("serverName");
			String tablesList = tableList.get("tablesList");

			Class.forName("com.teradata.jdbc.TeraDriver");

			String content = "";

			PreparedStatement stmt;
			ResultSet rs;

			for (String str : tablesList.split("\n")) {
				Connection conn = DriverManager.getConnection("jdbc:mysql://" + serName + ":3306/" + str.split("\\.")[0],
						uName, "");
				content = str.trim();
				// System.out.println(content);

				String query = "SHOW CREATE TABLE  " + content;
				// System.out.println("Query : "+query);
				BufferedWriter bwTdDDL = new BufferedWriter(new FileWriter(tempDir + "/" + content + ".txt"));
				stmt = conn.prepareStatement(query);
				rs = stmt.executeQuery();
				while (rs.next()) {
					String tblDDL = rs.getString(1);
					String datatype = rs.getString(2);
					System.out.println(datatype);

					bwTdDDL.write(datatype);
					bwTdDDL.close();
				}
				conn.close();
			}
		}
	/*private void getDDLsFromTD(BufferedReader brFile, String tempDir) throws Exception {
		String connurl = "";

		Class.forName("com.teradata.jdbc.TeraDriver");
		Connection conn = DriverManager.getConnection(connurl, "", "");

		String content = "";

		PreparedStatement stmt;
		ResultSet rs;
		while ((content = brFile.readLine()) != null) {
			content = content.trim();
			// System.out.println(content);

			String query = "SHOW TABLE " + content;
			// System.out.println("Query : "+query);

			stmt = conn.prepareStatement(query);
			rs = stmt.executeQuery();
			while (rs.next()) {
				String tblDDL = rs.getString(1);
				// System.out.println(tblDDL);

				BufferedWriter bwTdDDL = new BufferedWriter(new FileWriter(tempDir + "/" + content + ".txt"));
				bwTdDDL.write(tblDDL);
				bwTdDDL.close();
			}
		}
	}
*/
	public void getCommonDDLs(BufferedReader br) throws Exception {
		int lineNo = 0;
		try {
			String content = "";

			boolean firstLine = true;
			int count = 0;

			// columnNameMap.clear();
			// columnDataTypeMap.clear();
			// lastColumnMap.clear();

			logger.info("Getting the common DDLs...");
			while ((content = br.readLine()) != null) {
				lineNo++;
				if (firstLine) {
					content = TDToHiveConvertor.removeUTF8BOM(content);
					firstLine = false;
				}

				content = content.trim();
				if (content.startsWith("Request") || content.startsWith("NO ") || content.startsWith("CHECKSUM")
						|| content.startsWith("DEFAULT") || content.startsWith("UNIQUE")
						|| content.startsWith("PRIMARY") || content.startsWith("INDEX")
						|| content.startsWith("FREESPACE") || content.startsWith(" NO RANGE")
						|| content.startsWith("(")) {
					continue;
				} else if (content.startsWith("CREATE ")) {
					int index0 = content.indexOf("TABLE");
					int index1 = content.indexOf(".");
					int index2 = content.indexOf(",");

					// System.out.println("Index 0 : "+index0);
					// System.out.println("Index 1 : "+index1);
					// System.out.println("Index 2 : "+index2);

					dbName = content.substring(index0 + 6, index1);
					tblName = content.substring(index1 + 1, index2 - 1);

					if (dbName.equals("WM_SALES")) {
						dbName = "US_WM_TABLES";
					}
					if (dbName.equals("WC_TABLES")) {
						dbName = "US_WC_TABLES";
					}

					logger.info("=========================================================");
					logger.info("Table Name : " + dbName + "." + tblName);
					logger.info("=========================================================");

					// System.out.println("Database and table name : "+ dbName +
					// "." +tblName);
				} else if (content.startsWith("PARTITION")) {
					partition = true;

					int index3 = content.indexOf("(") + 1;
					int index4 = content.indexOf(" ", index3);

					// System.out.println("Index 3 : "+index3);
					// System.out.println("Index 4 : "+index4);

					partitionColumn = content.substring(index3, index4);
					// System.out.println("Partition column :
					// "+partitionColumn);
				} else {
					int contentLength = content.length() - 1;
					// System.out.println("Content length : "+contentLength);

					int index5 = content.indexOf(" "); // getting the index of
														// first space in the
														// line
					// System.out.println("Index 5 : "+index5);

					String columnName = content.substring(0, index5);
					// System.out.println("Column name : "+columnName);

					if (columnName.equalsIgnoreCase("item_nbr"))
						columnName = "mds_fam_id";

					columnNameMap.put(count, columnName);
					count++;

					index5++;
					int index6 = -1;

					if (content.indexOf(" ", index5) != -1) {
						index6 = content.indexOf(" ", index5); // getting the
																// index of
																// second space
																// in the line
					} else {
						index6 = contentLength; // getting the index of last
												// bracket in the line
					}
					// System.out.println("Index 6 : "+index6);

					String dataType = content.substring(index5, index6); // getting
																			// the
																			// entire
																			// data
																			// type
																			// with
																			// precision
					// System.out.println("Column data type : "+dataType);

					int index7 = dataType.indexOf("("); // to segregate the data
														// type name n precision
					// System.out.println("Index 7 : "+index7);

					String convertedDataType = "";
					String dataTypePrecision = "";

					if (index7 != -1) {
						int index8 = dataType.indexOf(")") + 1;
						// System.out.println("Index 8 : "+index8);

						String dataTypeName = dataType.substring(0, index7);
						dataTypePrecision = dataType.substring(index7, index8);

						convertedDataType = tdHiveDataTypeMapping.get(dataTypeName); // converting
																						// TD
																						// type
																						// to
																						// Hive
					} else {
						convertedDataType = tdHiveDataTypeMapping.get(dataType); // converting
																					// TD
																					// type
																					// to
																					// Hive
					}

					// System.out.println("Converted data type :
					// "+convertedDataType);
					// System.out.println("Data type precision :
					// "+dataTypePrecision);

					String newDataType = "";
					if (convertedDataType.equalsIgnoreCase("STRING")
							|| convertedDataType.equalsIgnoreCase("TIMESTAMP")) {
						newDataType = convertedDataType;
					} else {
						newDataType = convertedDataType + dataTypePrecision;
					}

					// System.out.println("New data type : "+newDataType);

					columnDataTypeMap.put(columnName, newDataType);

					if (content.charAt(contentLength) == (','))
						lastColumnMap.put(columnName, ",");
					else
						lastColumnMap.put(columnName, " )");
				}

			}

			for (int i = 0; i < columnNameMap.size(); i++) {
				arrayList.add(columnNameMap.get(i).length());
			}
			maxLength = Collections.max(arrayList) + 2;
			// System.out.println("Max lenth: " + maxLength);
		} catch (Exception exception) {
			logger.error("Error at line no. " + lineNo);
			throw exception;
		}
	}

	public StringBuffer buildHiveDDL(BufferedReader br) throws Exception {
		StringBuffer ddlContent = new StringBuffer();

		logger.info("Start : Converting Teradata DDL to its HIVE equivalent");

		ddlContent.append(DDL_CREATE_TABLE);
		ddlContent.append(dbName).append(".").append(tblName); // appending the
																// db name and
																// table name

		logger.info("Table DDL created...");

		ddlContent.append("(").append(System.getProperty("line.separator")); // appending
																				// the
																				// opening
																				// braces

		for (int i = 0; i < columnNameMap.size(); i++) {
			String columnName = columnNameMap.get(i);
			// System.out.println("Column name inside for loop : "+columnName);

			if (!columnName.equals(partitionColumn)) {
				ddlContent.append("    ").append(columnName.toLowerCase());

				int padding = maxLength - columnName.length();

				StringBuffer sBuffer = new StringBuffer();
				for (int pad = 0; pad < padding; pad++) {
					sBuffer.append(" ");
				}

				ddlContent.append(sBuffer.toString());

				ddlContent.append(columnDataTypeMap.get(columnName).toLowerCase()).append(",")
                			.append(System.getProperty("line.separator"));
			}

			logger.info("Column added to DDL : " + columnName.toLowerCase());
		}

		 String finalDDLContent = ddlContent.toString();
         finalDDLContent = finalDDLContent.substring(0,finalDDLContent.length()-2 )+ ")";
         ddlContent = new StringBuffer();
         ddlContent.append(finalDDLContent);
        
		if (partition) {
			ddlContent.append("PARTITIONED BY (").append(partitionColumn.toLowerCase()).append(" ")
					.append(columnDataTypeMap.get(partitionColumn).toLowerCase()).append(")")
					.append(System.getProperty("line.separator"));

			logger.info("Partition added : " + partitionColumn.toLowerCase());
			//partition = false;
		}

		ddlContent.append(DDL_ROW_FORMAT_DELIMITED_FIELDS_TERMINATED_BY);

		logger.info("End : Converting Teradata DDL to its HIVE equivalent");

		return ddlContent;
	}

	public StringBuffer getHQLCommonParts() throws Exception {
		logger.info("Start : Converting Teradata DDL to HIVE HQL...");

		String header = "";
		String configs = "";

		StringBuffer hqlContent = new StringBuffer();

		header = "";

		configs = "";

		if (partition) {
			configs = configs + "\n--Added the below parameter to enable dynamic partition mode\n"
					+ "SET hive.exec.dynamic.partition.mode=nonstrict;\n";
		}

		hqlContent.append(header).append(System.getProperty("line.separator"));

		logger.info("Added header...");

		hqlContent.append(configs).append(System.getProperty("line.separator"));

		logger.info("Added required set parameters...");

		hqlContent.append(CREATE_DATABASE_comments).append(CREATE_DATABASE).append(System.getProperty("line.separator"))
				.append(System.getProperty("line.separator"));

		logger.info("Database created if not exists...");

		hqlContent.append(USE_DATABASE_comments).append(USE_DATABASE).append(System.getProperty("line.separator"))
				.append(System.getProperty("line.separator"));

		logger.info("Logged into the database...");

		return hqlContent;
	}

	private StringBuffer buildHiveExternalTbl(StringBuffer hqlContent) throws Exception {
		hqlContent.append(DROP_EXTERNAL_TABLE_comments).append(DROP_EXTERNAL_TABLE)
				.append(System.getProperty("line.separator")).append(System.getProperty("line.separator"));

		logger.info("Dropped external table...");

		hqlContent.append(CREATE_EXTERNAL_TABLE_comments).append(CREATE_EXTERNAL_TABLE).append("(")
				.append(System.getProperty("line.separator"));

		logger.info("Start : Create external table...");

		for (int i = 0; i < columnNameMap.size(); i++) {
			String columnName = columnNameMap.get(i);

			hqlContent.append("    ").append(columnName.toLowerCase());

			int padding = maxLength - columnName.length();

			StringBuffer sBuffer = new StringBuffer();
			for (int pad = 0; pad < padding; pad++) {
				sBuffer.append(" ");
			}

			hqlContent.append(sBuffer.toString());

			hqlContent.append(columnDataTypeMap.get(columnName).toLowerCase()).append(lastColumnMap.get(columnName))
					.append(System.getProperty("line.separator"));

			logger.info("Column added to external table DDL : " + columnName.toLowerCase());
		}

		hqlContent.append(ROW_FORMAT_DELIMITED_FIELDS_TERMINATED_BY).append(System.getProperty("line.separator"));

		hqlContent.append(LOCATION);

		hqlContent.append(System.getProperty("line.separator"));
		hqlContent.append(System.getProperty("line.separator"));

		logger.info("End : Create external table...");

		return hqlContent;

	}

	private StringBuffer buildHiveMngdTbl(StringBuffer hqlContent) throws Exception {
		hqlContent.append(DROP_TABLE_comments).append(DROP_TABLE).append(System.getProperty("line.separator"))
				.append(System.getProperty("line.separator"));

		logger.info("Dropped base table...");

		hqlContent.append(CREATE_TABLE_comments).append(CREATE_TABLE).append("(")
				.append(System.getProperty("line.separator"));

		logger.info("Start : Create base table...");

		for (int i = 0; i < columnNameMap.size(); i++) {
			String columnName = columnNameMap.get(i);
			if (!columnName.equals(partitionColumn)) {
				hqlContent.append("    ").append(columnName.toLowerCase());

				int padding = maxLength - columnName.length();

				StringBuffer sBuffer = new StringBuffer();
				for (int pad = 0; pad < padding; pad++) {
					sBuffer.append(" ");
				}

				hqlContent.append(sBuffer.toString());

				hqlContent.append(columnDataTypeMap.get(columnName).toLowerCase()).append(",")
							.append(System.getProperty("line.separator"));

				logger.info("Column added to base table DDL : " + columnName.toLowerCase());
			}
		}
		 String finalDDLContent = hqlContent.toString();
         finalDDLContent = finalDDLContent.substring(0,finalDDLContent.length()-2)+")";
         hqlContent = new StringBuffer();
         hqlContent.append(finalDDLContent);
         
		if (partition) {
			hqlContent.append("PARTITIONED BY (").append(System.getProperty("line.separator")).append("  ")
					.append(partitionColumn.toLowerCase()).append(" ")
					.append(columnDataTypeMap.get(partitionColumn).toLowerCase()).append(")")
					.append(System.getProperty("line.separator"));

			logger.info("Partition column added : " + partitionColumn.toLowerCase());
		}

		hqlContent.append(STORED_AS);

		hqlContent.append(System.getProperty("line.separator"));
		hqlContent.append(System.getProperty("line.separator"));

		logger.info("End : Create base table...");

		return hqlContent;
	}

	private StringBuffer insertIntoMngdTblAndDropStgTbl(StringBuffer hqlContent) throws Exception {
		hqlContent.append(INSERT_INTO_TABLE_comments).append(INSERT_INTO_TABLE).append(" ");

		if (partition) {
			hqlContent.append("PARTITION(").append(partitionColumn.toLowerCase()).append(") ");

			hqlContent.append(System.getProperty("line.separator")).append("SELECT ");

			for (int i = 0; i < columnNameMap.size(); i++) {
				String columnName = columnNameMap.get(i);
				if (!columnName.equals(partitionColumn)) {
					hqlContent.append(columnName.toLowerCase()).append(",")
							.append(System.getProperty("line.separator"));
				}
			}

			hqlContent.append(partitionColumn.toLowerCase()).append(System.getProperty("line.separator"))
					.append(FROM_TABLE).append(System.getProperty("line.separator"))
					.append(System.getProperty("line.separator"));
		} else {
			hqlContent.append(System.getProperty("line.separator")).append(SELECT_FROM_TABLE)
					.append(System.getProperty("line.separator")).append(System.getProperty("line.separator"));
		}

		logger.info("Inserted data into base table...");

		hqlContent.append(DROP_EXTERNAL_TABLE_comments).append(DROP_EXTERNAL_TABLE)
				.append(System.getProperty("line.separator")).append(System.getProperty("line.separator"));

		logger.info("Dropped stg table...");
		logger.info("End : Converting Teradata DDL to HIVE HQL...");

		return hqlContent;
	}

	private static String removeUTF8BOM(String s) throws Exception {
		if (s.startsWith(UTF8_BOM)) {
			s = s.substring(1);
		}

		return s;
	}

	private static void delete(File tempDir) throws Exception {
		if (tempDir.isDirectory()) {

			// directory is empty, then delete it
			if (tempDir.list().length == 0) {
				tempDir.delete();
				System.out.println("Directory is deleted : " + tempDir.getAbsolutePath());

			} else {
				// list all the directory contents
				String files[] = tempDir.list();
				for (String temp : files) {
					// construct the file structure
					File fileDelete = new File(tempDir, temp);

					// recursive delete
					delete(fileDelete);
				}

				// check the directory again, if empty then delete it
				if (tempDir.list().length == 0) {
					tempDir.delete();
					System.out.println("Directory is deleted : " + tempDir.getAbsolutePath());
				}
			}
		} else {
			// if file, then delete it
			tempDir.delete();
			System.out.println("File is deleted : " + tempDir.getAbsolutePath());
		}

	}
}
