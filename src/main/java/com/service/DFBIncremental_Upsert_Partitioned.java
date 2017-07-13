package com.service;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class DFBIncremental_Upsert_Partitioned {
	Map<String, String> stg_base_col_map = new LinkedHashMap<String, String>();
	private String sprint = "demo";
	private String stageTable = "";
	private String primIndexColumns;
	private String partitionColumns;
	private String whereClause="";
	private String inClauseCol = "";
	private String stgTbleColumns = "";
	private String selStgTbleColumns = "";
	private String sCreateTempTble = "";
	private String finalStgTbl = "";
	private String finalInsTemp = "";
	String IMP_MESSAGE= "";
	StringBuffer whereConditionColmns = new StringBuffer();
	private List<String> baseTableColumns = new ArrayList<String>();
	static Map<String, String> tdHiveDataTypeMapping = new HashMap<String, String>();
	private String insTemp_1 = "";
	StringBuffer baseTableDDL = new StringBuffer();
	private static final String CREATE_DB ="CREATE DATABASE IF NOT EXISTS ${hivevar:hv_database}";
	private static final String DROP_S_TBL = "DROP TABLE IF EXISTS ${hivevar:hv_database}.${hivevar:hv_table}_stg;";
	private static final String DROP_T_TBL = "DROP TABLE IF EXISTS ${hivevar:hv_database}.${hivevar:hv_table}_temp;";
	private static final String DROP_ROWNUM_TBL = 	"DROP TABLE IF EXISTS ${hivevar:hv_database}.${hivevar:hv_table}_rownum;";
	private static final String CREATE_ROWNUM_TBL = 	"CREATE TABLE ${hivevar:hv_database}.${hivevar:hv_table}_rownum as\n SELECT ROW_NUMBER() OVER() as ROW_NUM,* from ${hivevar:hv_database}.${hivevar:hv_table}_stg;";
	private static final String DROP_NODUPS_TBL = "DROP TABLE IF EXISTS ${hivevar:hv_database}.${hivevar:hv_table}_nodups;";
	private static final String CREATE_NODUPS_TBL = "CREATE TABLE IF NOT EXISTS ${hivevar:hv_database}.${hivevar:hv_table}_nodups AS \nSELECT * FROM ${hivevar:hv_database}.${hivevar:hv_table}_rownum WHERE row_num IN (SELECT MAX(row_num) from ${hivevar:hv_database}.${hivevar:hv_table}_rownum GROUP BY";
	private static final String DROP_TEMP_TBL = "DROP TABLE IF EXISTS ${hivevar:hv_database}.${hivevar:hv_table}_temp;";
	
	static {
		tdHiveDataTypeMapping.put("BYTEINT", "TINYINT");
		tdHiveDataTypeMapping.put("SMALLINT", "SMALLINT");
		tdHiveDataTypeMapping.put("INTEGER", "INT");
		tdHiveDataTypeMapping.put("INT", "INT");
		tdHiveDataTypeMapping.put("BIGINT", "BIGINT");
		tdHiveDataTypeMapping.put("DECIMAL", "DECIMAL");
		tdHiveDataTypeMapping.put("NUMERIC", "DECIMAL");
		tdHiveDataTypeMapping.put("FLOAT", "DOUBLE");
		tdHiveDataTypeMapping.put("CHAR", "CHAR");// available after hive 0.13
		tdHiveDataTypeMapping.put("VARCHAR", "STRING");
		tdHiveDataTypeMapping.put("DATE", "DATE"); // formatting is needed asper needs
		tdHiveDataTypeMapping.put("TIME", "STRING");
		tdHiveDataTypeMapping.put("TIMESTAMP", "TIMESTAMP");
	}
	private Map<Integer, String> columnNameMap = new LinkedHashMap<Integer, String>();
	boolean isCobolLayout = false;
	boolean isSummrization = false;
	Map<String, String> updateColumnsMap = new LinkedHashMap<String, String>();
	String fileName = "";
	
	/**
	 * please pass <db_name> <table_name> & any one: <upsert|insert|update|delete > isPartitioned\n ex: wm_sales store_info upsert no
	 * @param args
	 */
	public static void main(String[] args) {
		DFBIncremental_Upsert_Partitioned inc = new DFBIncremental_Upsert_Partitioned();
		inc.init(inc,args);
	}
	public void init(DFBIncremental_Upsert_Partitioned inc, String[] args) {
		if(args.length != 3){
			System.err.println("Argument mismatched!!please pass <db_name> <table_name> & any one: <upsert|insert|update|delete > isPartitioned\n ex: wm_sales store_info upsert no");
		} else {
			try {
				String dbName=args[0];
				String tblName=args[1];
				MySQLToHiveDDLConvertor ddlHQLCreator = new MySQLToHiveDDLConvertor();
				String isCobolLayout = args[2];
				String isSummarization = args[3];
				inc.fileName = dbName+"-"+tblName;
				FileInputStream fis = new FileInputStream(
						new File("C:\\work\\incremental\\metadata\\"+sprint+"\\"+inc.fileName+"-ddl.txt"));
				InputStreamReader isr = new InputStreamReader(fis, "UTF-8");
				BufferedReader br = new BufferedReader(isr);
	
				ddlHQLCreator.getCommonDDLs(br);
				StringBuffer hiveDDL = ddlHQLCreator.buildHiveDDL(br);
				StringBuffer commonDDLs = ddlHQLCreator.getHQLCommonParts();
				StringBuffer extrnlTblDDL = ddlHQLCreator
						.buildHiveExternalTbl(new StringBuffer());
				StringBuffer createTblDDL = ddlHQLCreator
						.buildHiveMngdTbl(new StringBuffer());
				
				//inc.columnNameMap = ddlHQLCreator.getColumnNameMap();
				inc.baseTableDDL = createTblDDL;
				if (null != isCobolLayout && !isCobolLayout.isEmpty()
						&& "yes".equalsIgnoreCase(isCobolLayout)) {
					inc.isCobolLayout = Boolean.TRUE;
				}
				if (null != isSummarization && !isSummarization.isEmpty()
						&& "yes".equalsIgnoreCase(isSummarization)) {
					inc.isSummrization = Boolean.TRUE;
				}
				
				// inc.readHiveDDL();
				
				if(inc.isSummrization){
					inc.readUpdateFileForSum();
				}
				//inc.readHiveDDL();
				inc.createStageTable();
	
				inc.createTempTbl();
				inc.getLoadFileToBaseTbleMapping();
				inc.getPartitionPrimIndDetails();
				inc.writeToFile(dbName, tblName);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}
	private  void createStageTable() {
		BufferedReader br = null;

		try {

			String sCurrentLine;

			br = new BufferedReader(new FileReader("C:\\work\\incremental\\metadata\\"+sprint+"\\"+fileName+"-layout.txt"));
			String sField = "FIELD";
			StringBuffer sBuffer = new StringBuffer("CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:hv_database}.${hivevar:hv_table}_stg(\n");
			while ((sCurrentLine = br.readLine()) != null) {
				sCurrentLine = sCurrentLine.toLowerCase();
				 if (!"".equals(sCurrentLine) ){
					boolean boolVal = Boolean.FALSE;
					//System.out.println(sCurrentLine);
					if(sCurrentLine.toLowerCase().startsWith(".field")) {
						sCurrentLine = sCurrentLine.replaceAll("."+sField.toLowerCase(), "");
						boolVal = true;
					}
					else if(sCurrentLine.toLowerCase().startsWith("field")){
						sCurrentLine = sCurrentLine.replaceAll(sField.toLowerCase(), "");
						boolVal = true;
					}if(boolVal) {
						//System.out.println("After replace field: " +sCurrentLine);
						sCurrentLine = sCurrentLine.trim();
						//System.out.println("After trimming: " +sCurrentLine);
						String[] colDatType = sCurrentLine.split("\\*");
						//System.out.println("colDatType[0]: " + colDatType[0]);
						//System.out.println(colDatType[0]+""+colDatType[1]+ "-->" +tdHiveDataTypeMapping.get(colDatType[1]));
						String tmp = (colDatType[1].indexOf("nullif")>0)?(colDatType[1].substring(0,colDatType[1].indexOf("nullif"))+"#"):colDatType[1].substring(0,colDatType[1].length())+"!";
						Set<Entry<String, String>> entrySet = tdHiveDataTypeMapping.entrySet();
						for (Entry<String, String> string : entrySet) {
							tmp = tmp.replaceAll(string.getKey().toLowerCase(), string.getValue().toLowerCase());
						}
						sBuffer.append("  "+colDatType[0] + "   " +tmp.substring(0,tmp.length()-1).replaceAll(";", "")+",\n");
						stgTbleColumns =stgTbleColumns+colDatType[0].trim()+"\n";
						//System.out.println("===============");
					}
				}
			}
			String str = sBuffer.toString().substring(0,sBuffer.toString().length()-1);
			sBuffer = new StringBuffer();
			sBuffer.append(str.substring(0, str.length()-1)+")"+
					"\nROW FORMAT DELIMITED"+
					"\nFIELDS TERMINATED BY '|'"+
					"\nLOCATION '${hivevar:hv_loc}';");
			finalStgTbl = sBuffer.toString().replaceAll("/;/!", "").substring(0,sBuffer.toString().length()-1)+";";
			//finalStgTbl = finalStgTbl.replaceAll("                 ,", ",");
			//System.out.println("Final table" + finalStgTbl.replaceAll("                    ,", ","));
			
			stageTable = finalStgTbl;
			//System.err.println("stageTable: -- " + stageTable);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)
					br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
	}
	
	public void getPartitionPrimIndDetails() {
		BufferedReader br = null;
		try {
			String sCurrentLine;
			br = new BufferedReader(new FileReader("C:\\work\\incremental\\metadata\\"+sprint+"\\"+fileName+"-ddl.txt"));
			String sField = "";
			while ((sCurrentLine = br.readLine()) != null) {
				sCurrentLine = sCurrentLine.toLowerCase();
				//System.err.println(sCurrentLine);
				if(!"".equals(sCurrentLine) && ((sCurrentLine.startsWith("primary index")) || sCurrentLine.startsWith("unique primary index"))){
					primIndexColumns = sCurrentLine.substring(sCurrentLine.indexOf("(")+1,sCurrentLine.indexOf(")"));
				}
				if(!"".equals(sCurrentLine) && ((sCurrentLine.startsWith("partition by")) )){
					partitionColumns = sCurrentLine.substring(sCurrentLine.indexOf("(")+1).split(" ")[0];
				}
			}
			
			System.out.println("partitionColumns: " + partitionColumns);
			StringBuffer sbWhereClause = new StringBuffer();
			if(null != primIndexColumns && !"".equals(primIndexColumns) ){
				String[] primCol = primIndexColumns.split(",");
				
				for (String column : primCol) {
					sbWhereClause.append("m."+column.trim() + "= s."+column.trim()+" AND ");
				}
				whereClause = sbWhereClause.substring(0,sbWhereClause.toString().length()-4).trim();
				if(!"".equals(whereClause) && whereClause.split("AND").length>0){
					inClauseCol = primCol[0].trim();
				}
			}
			
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)
					br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
		
		BufferedReader br1 = null;
		try {
			String sCurrentLine;
			String nwWhereClz = null ;
			br1 = new BufferedReader(new FileReader("C:\\work\\incremental\\metadata\\"+sprint+"\\"+fileName+"-update.txt"));
			StringBuffer fullCommand = new StringBuffer("");
			StringBuffer newWhereClause = new StringBuffer("");
			while ((sCurrentLine = br1.readLine()) != null) {
				sCurrentLine = sCurrentLine.toLowerCase();
				fullCommand.append(sCurrentLine);
			}
			String updateSql = fullCommand.toString();
			updateSql = updateSql.substring(updateSql.indexOf("where")+5,updateSql.length()-1);
			//System.err.println("updateSql" +updateSql);
			String[] split = updateSql.split("and");
			for (String string : split) {
				//System.out.println(string.trim());
				String[] split2 = string.trim().split("=");
				
				if(split2[1].trim().startsWith("substr(")){
					newWhereClause.append("m."+ split2[0].trim()+" = s."+stg_base_col_map.get(split2[0].trim().replaceAll(":", ""))+ " AND ");
				}
				else {
					newWhereClause.append("m."+ split2[0].trim()+" = s."+stg_base_col_map.get(split2[0].trim().replaceAll(":", ""))+ " AND ");
				}
				 nwWhereClz =  newWhereClause.toString().substring(0, newWhereClause.toString().length()-4);
				 whereConditionColmns.append(split2[0].trim()+" ,");
			}
			whereConditionColmns = new StringBuffer(whereConditionColmns.toString().substring(0,whereConditionColmns.toString().length()-1));
			System.err.println("whereConditionColmns: " + whereConditionColmns);
			//System.out.println("nwWhereClz: " + nwWhereClz.toString());
				/*String[] primCol = primIndexColumns.split(",");
				
				for (String column : primCol) {
					sbWhereClause.append("m."+column.trim() + "= s."+column.trim()+" AND ");
				}
				whereClause = sbWhereClause.substring(0,sbWhereClause.toString().length()-4).trim();
				if(!"".equals(whereClause) && whereClause.split("AND").length>0){
					inClauseCol = primCol[0].trim();
				}*/
				
				whereClause = nwWhereClz.replaceAll("[()]", "");
				System.err.println("whereClause: " + whereClause);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br1 != null)
					br1.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
	}
	
	public void createTempTbl() {
		StringBuffer sbCreateTempTble = new StringBuffer("CREATE TABLE ${hivevar:hv_database}.${hivevar:hv_table}_temp AS");
		sbCreateTempTble.append("\n");
		sbCreateTempTble.append("SELECT m.* FROM ${hivevar:hv_database}.${hivevar:hv_table} m");
		sbCreateTempTble.append("\n");
		sbCreateTempTble.append("WHERE m."+inClauseCol.trim() + " NOT IN ( \n");
		sbCreateTempTble.append("SELECT DISTINCT s." +inClauseCol.trim());
		sbCreateTempTble.append("\n");
		sbCreateTempTble.append("FROM ${hivevar:hv_database}.${hivevar:hv_table}_stg s");
		sbCreateTempTble.append("\n");
		sbCreateTempTble.append("WHERE "+whereClause.trim() +" );");
		sCreateTempTble = sbCreateTempTble.toString();
	}
	
	public void getLoadFileToBaseTbleMapping() {

		BufferedReader br = null;
		try {
			
			String sCurrentLine;
			StringBuffer fullCommand = new StringBuffer();
			br = new BufferedReader(new FileReader("C:\\work\\incremental\\metadata\\"+sprint+"\\"+fileName+"-insert.txt"));
			while ((sCurrentLine = br.readLine()) != null) {
				sCurrentLine = sCurrentLine.toLowerCase();
				if(!"".equals(sCurrentLine)){
					fullCommand.append(sCurrentLine);
				}
			}
			//System.out.println("fullCommand: " + fullCommand.toString());
			String baseTbleColmuns = fullCommand.toString().substring(fullCommand.toString().indexOf("(")+1,fullCommand.toString().indexOf("values"));
			baseTbleColmuns = baseTbleColmuns.substring(0,baseTbleColmuns.indexOf(')'));
			//System.out.println("BASE TABLE: " +baseTbleColmuns);
			List<String> asList = new ArrayList<String>();
			for (String string : baseTbleColmuns.split(",")) {
				asList.add(string.trim());
			}
			List<String> missingList = new ArrayList<String>();
			for (String eachVal : baseTableColumns) {
				
				if(!asList.contains(eachVal.trim())){
					missingList.add(eachVal.trim());
				}
			}
			System.out.println("missingList: " + missingList.toString().substring(1,missingList.toString().length()-1));
			
			String stgTbleColumns = fullCommand.toString().substring(fullCommand.toString().indexOf("values")+7,fullCommand.toString().indexOf(";"));
			stgTbleColumns = stgTbleColumns.replaceAll("date,format", "");
			stgTbleColumns = stgTbleColumns.replaceAll(" 'yyyymmdd'", "");
			stgTbleColumns = stgTbleColumns.replaceAll(" 'YYYY-MM-DD'".toLowerCase(), "");
			stgTbleColumns = stgTbleColumns.replaceAll("integer, format", "");
			stgTbleColumns = stgTbleColumns
					.replaceAll(":", "").replaceAll("\\(\\)", "");
			
			
			if(null != stgTbleColumns && !"".equals(stgTbleColumns) ){
					String sourceStr = "";
					if(stgTbleColumns.contains("substr(")) {
						sourceStr = stgTbleColumns.substring(stgTbleColumns.indexOf("substr")+7, stgTbleColumns.indexOf(")"));
						System.out.println("stgTbleColumns: " + stgTbleColumns);
						String replcStr = "";
						replcStr = sourceStr.replaceAll(",", "#");
						stgTbleColumns = stgTbleColumns.replaceAll(sourceStr, replcStr);
					}
					selStgTbleColumns = stgTbleColumns;
					System.err.println("******************************** " + stgTbleColumns);
					System.out.println("Staging table Count     : "+stgTbleColumns.split(",").length );
					System.out.println("Base table Count(insert): "+baseTbleColmuns.split(",").length);
					System.out.println("Base table cnt (hiveddl): "+baseTableColumns.size());
				if(stg_base_col_map.size() != baseTableColumns.size()){
					System.err.println("some of the columns are not selected in the INSERT statement, kindly check the insert overwrite statement and manually make necesary changes");
				}
				
				
				if(selStgTbleColumns.split(",").length == baseTbleColmuns.split(",").length){
					
					for (int i=0; i < selStgTbleColumns.split(",").length; i++) {
						stg_base_col_map.put(baseTbleColmuns.split(",")[i].trim(), selStgTbleColumns.split(",")[i].trim());
					}
				}
				else if(selStgTbleColumns.split(",").length < baseTbleColmuns.split(",").length) {
					for (int i=0; i < selStgTbleColumns.split(",").length; i++) {
						stg_base_col_map.put(baseTbleColmuns.split(",")[i].trim(), selStgTbleColumns.split(",")[i].trim());
					}
				}
				else if(selStgTbleColumns.split(",").length > baseTbleColmuns.split(",").length) {
					for (int i=0; i < baseTbleColmuns.split(",").length; i++) {
						stg_base_col_map.put(baseTbleColmuns.split(",")[i].trim(), selStgTbleColumns.split(",")[i].trim());
					}
				}
				
				if(selStgTbleColumns.split(",").length == baseTbleColmuns.split(",").length) {
					IMP_MESSAGE = "Columns count is not matching with Base and Stage table, please check if columns are hard-coded!!!";
				}
			}
			//Prepare insert into Temp step 1
			StringBuffer sbInsTemp = new StringBuffer("INSERT  INTO  TABLE ${hivevar:hv_database}.${hivevar:hv_table}_temp \n SELECT \n");
			Set<Entry<String, String>> entrySet = stg_base_col_map.entrySet();
			for (Entry<String, String> string : entrySet) {
				String column = string.getValue().toLowerCase();
				String temp = "";
				if(string.getValue().indexOf("||")>0){
					 String[] split = string.getValue().split("\\|\\|");
					 temp = "CONCAT(";
					 for (String string2 : split) {
						 
						 temp+=string2+",";
					}
					 temp=temp.substring(0, temp.length()-1)+")";
					 sbInsTemp.append("  "+ temp+",\n");
				}
				else
					sbInsTemp.append("  "+ string.getValue().trim()+",\n");
			}
			String tmpIns = sbInsTemp.toString();
			tmpIns = tmpIns.substring(0, tmpIns.length()-2).replaceAll("'yyyyddd'", "").replaceAll("'yymmdd'", "");
			sbInsTemp = new StringBuffer(tmpIns);
			sbInsTemp.append("\nFROM ${hivevar:hv_database}.${hivevar:hv_table}_stg;");
			//System.out.println("sbInsTemp: " +sbInsTemp);
			finalInsTemp = sbInsTemp.toString();
			
			//Prepare insert into Temp step 2
			StringBuffer sbInsTemp1 = new StringBuffer("INSERT OVERWRITE TABLE ${hivevar:hv_database}.${hivevar:hv_table}\n" +"SELECT ");
			for (Entry<String, String> string : entrySet) {
				sbInsTemp1.append("  "+string.getKey().toLowerCase().trim()+",\n");
			}
			sbInsTemp1 = new StringBuffer(sbInsTemp1.toString().substring(0,sbInsTemp1.toString().length()-2));
			sbInsTemp1.append("\nFROM ${hivevar:hv_database}.${hivevar:hv_table}_temp;");
			//System.out.println("sbInsTemp1: " +sbInsTemp1);
			insTemp_1 = sbInsTemp1.toString();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)
					br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
	}
	
	public void readHiveDDL() {
		BufferedReader br = null;
		try {
			String sCurrentLine;
			StringBuffer fullCommand = new StringBuffer();
			br = new BufferedReader(new FileReader("C:\\work\\incremental\\metadata\\"+sprint+"\\"+fileName+"-hiveddl.txt"));
			while ((sCurrentLine = br.readLine()) != null) {
				sCurrentLine = sCurrentLine.toLowerCase();
				sCurrentLine=sCurrentLine.replaceAll("hiveconf", "hivevar");
				if(!"".equals(sCurrentLine)){
					fullCommand.append(sCurrentLine.trim());
				}
				String[] split = sCurrentLine.trim().split(" ");
				if(split.length==2){
					baseTableDDL.append(split[0]+"                    "+split[1]+"\n");
				} else{
					baseTableDDL.append(sCurrentLine+"\n");
				}
						
			}
			String actualBaseTblColumns = fullCommand.toString().substring(fullCommand.toString().indexOf('(')+1,fullCommand.toString().lastIndexOf(')'));
			String[] split = actualBaseTblColumns.split(",");
			for (String string : split) {

				String[] split2 = string.replaceAll("\\(.+\\)", "").replaceAll(".+\\)", "").trim().split(" ");
				//System.err.println("string2: " + split2[0].trim());
				//System.out.println("string: "+string +"--> split2: "+split2[0].toString());
				if("".equals(split2[0].trim())){
				//	System.err.println("blank");
				}
				else {
					baseTableColumns.add(split2[0].trim());
				}
			}
			//System.out.println(baseTableColumns);
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				if (br != null)
					br.close();
			} catch (IOException ex) {
				ex.printStackTrace();
			}
		}
	
	
	
	}
	public static void putHQLHeader(BufferedWriter bw ) throws Exception {
		
		bw.write("-- #*************************************************************************#\n");
		bw.write("-- # TITLE: <HQL_NAME>.hql                                          #\n");

		bw.write("-- # DESCRIPTION: The job loads the records for <TABLE_NAME>          #\n");
		bw.write("-- #=========================================================================#\n");
		bw.write("-- # REVISION HISTORY                                                        #\n");
		bw.write("-- #-------------------------------------------------------------------------#\n");
		bw.write("-- #2017-02-15 :: v1.0  :: <USER_ID>     :: Created  <DATE>           #\n");
		bw.write("-- #=========================================================================#\n");
		bw.write("SET hive.exec.compress.output=true ;\n");
		bw.write("SET hive.exec.compress.intermediate=true ;\n");
		bw.write("SET mapred.output.compress=true ;\n");
		bw.write("SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec ;\n");
		bw.write("SET mapred.map.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec ;\n");
		bw.write("SET io.seqfile.compression.type=BLOCK;\n");
		bw.write("SET io.sort.mb=500 ;\n");
		bw.write("SET mapred.reduce.slowstart.completed.maps=0.90;\n");
		bw.write("SET dfs.block.size=536870912;\n");
		bw.write("SET io.file.buffer.size=131072;\n");
		bw.write("SET mapred.compress.map.output=true;\n");
		bw.write("SET mapred.output.compression.type=BLOCK;\n");
		bw.write("SET hive.auto.convert.join=true;\n");
		bw.write("SET mapreduce.map.memory.mb=4096;\n");
		bw.write("SET mapreduce.map.java.opts =-Xmx3072m;\n");
		bw.write("SET mapreduce.reduce.java.opts=-Xmx3072m;\n");
		bw.write("\n--Added the below parameter to enable tez execution engine\n");
		bw.write("SET hive.execution.engine=tez;\n");
		bw.write("SET tez.runtime.io.sort.mb=1600;\n\n");

	}
	public void writeToFile(String dbName, String tblName) {
		String hiveDDLLoc = "C:\\work\\incremental\\generated\\"+sprint+"\\"+dbName+"-"+tblName+".hql";
		try {
		BufferedWriter bw = new BufferedWriter(new FileWriter(hiveDDLLoc));
			putHQLHeader(bw);
			//bw.write(this.IMP_MESSAGE);
			bw.write("--Creating Database if not exists\n");
			bw.write("CREATE DATABASE IF NOT EXISTS ${hivevar:hv_database};\n\n--Logging into the database before executing the below HQLs\nUSE ${hivevar:hv_database};\n\n" );
			bw.write("--Creating Database if not exists\n");
			bw.write(baseTableDDL.toString());
			bw.write("");
			bw.write("");
			bw.write("");
			bw.write("\n\n--drop the Stage table if exists\n"+DROP_S_TBL +"\n\n--creating the stage table if not exists to store raw data\n");
			bw.write(this.finalStgTbl.toString() +"\n\n--Dropping rownum table if present\n");
			bw.write(DROP_ROWNUM_TBL +"\n\n--Create rownum table  select from stage table\n"+CREATE_ROWNUM_TBL);
			bw.write("\n\n--Drop nodups table if present\n");
			bw.write(DROP_NODUPS_TBL+"\n\n--Create table which doesnt have duplicates in it\n");
			bw.write(CREATE_NODUPS_TBL +whereConditionColmns.toString()+"\n\n" +
					"--Drop table temp1 if exists\n" +
					"DROP TABLE IF EXISTS ${hivevar:hv_database}.${hivevar:hv_table}_temp1;" +
					"\n\n" +
					"-- Create temp1 table which holds affected partitions from Base table\n" +
					"CREATE TABLE ${hivevar:hv_database}.${hivevar:hv_table}_temp1 AS\n");
			bw.write("SELECT m.* FROM ${hivevar:hv_database}.${hivevar:hv_table} m\nWHERE m."+partitionColumns +" ");
			bw.write("\nIN (SELECT distinct s."+stg_base_col_map.get(partitionColumns.trim())+" FROM ${hivevar:hv_database}.${hivevar:hv_table}_nodups s);\n\n\n");
			bw.write("--Drop table temp2 if exists\n" +
					"DROP TABLE IF EXISTS ${hivevar:hv_database}.${hivevar:hv_table}_temp2;\n");
			bw.write("\n--Create temp2 table which holds the unaffected rows from affected partitions\n" +
					"CREATE TABLE ${hivevar:hv_database}.${hivevar:hv_table}_temp2 as \n");
			bw.write("SELECT h.*\n");
			bw.write("FROM ${hivevar:hv_database}.${hivevar:hv_table}_temp1 h\n");
			bw.write("WHERE NOT EXISTS (\n");
			bw.write("SELECT 1\n");
			bw.write("FROM ${hivevar:hv_database}.${hivevar:hv_table}_nodups s\n");
			bw.write("WHERE "+whereClause.replaceAll("m\\.", "h\\.") +" );");
			bw.write("\n\n\n--inserting data from the temp2 table into the temp1 table\n");
			bw.write("INSERT OVERWRITE TABLE ${hivevar:hv_database}.${hivevar:hv_table}_temp1\n");
			bw.write("SELECT *\n");
			bw.write("FROM ${hivevar:hv_database}.${hivevar:hv_table}_temp2;\n\n\n");
			bw.write("--inserting data from the stage table into the temp1 table\n");
			bw.write("INSERT INTO TABLE ${hivevar:hv_database}.${hivevar:hv_table}_temp1\n");
			bw.write("SELECT ");
			String tmp ="";
			Set<Entry<String, String>> entrySet = stg_base_col_map.entrySet();
			for (Entry<String, String> string : entrySet) {
				tmp +="\n       "+string.getValue()+",";
				//tmp = tmp.replaceAll(string.getKey().toLowerCase(), string.getValue().toLowerCase());
			}
			bw.write(tmp.substring(0, tmp.length()-1).replaceAll("[()]", ""));
			bw.write("\nFROM ${hivevar:hv_database}.${hivevar:hv_table}_stg;\n\n\n");
			
			bw.write("--inserting data from the temp1 table into the base table\n");
			bw.write("INSERT OVERWRITE TABLE ${hivevar:hv_database}.${hivevar:hv_table} PARTITION("+partitionColumns+")\n");
			bw.write("SELECT *\n");
			bw.write("FROM ${hivevar:hv_database}.${hivevar:hv_table}_temp1;");
			
			//bw.write("\n\n-- Insert into base table from temp table\n\n");
			//bw.write(insTemp_1);
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	private void readUpdateFileForSum() {
		BufferedReader br = null;
		try {
			String updateFullCommand ="";
			String sCurrentLine;
			br = new BufferedReader(new FileReader(
					"C:\\work\\incremental\\metadata\\" + sprint + "\\"
							+ fileName + "-update.txt"));
			StringBuffer sbupdateFullCommand = new StringBuffer();			
			while ((sCurrentLine = br.readLine()) != null) {
				sCurrentLine = sCurrentLine.toLowerCase();
				sbupdateFullCommand.append(sCurrentLine);
			}
		updateFullCommand = sbupdateFullCommand.toString();
		updateFullCommand = updateFullCommand.substring(updateFullCommand.indexOf("set")+4,updateFullCommand.indexOf("where")).trim();
		String[] commaSplit = updateFullCommand.split(",");
		for (String string : commaSplit) {
			string = string.trim();
			String[] equalSplit = string.split("=");
			String s = equalSplit[1].trim() +"               AS "+equalSplit[0].trim();
			updateColumnsMap.put(equalSplit[0].trim(), s);
			}
		}
		
			catch (IOException e) {
				e.printStackTrace();
			} finally {
				try {
					if (br != null)
						br.close();
				} catch (IOException ex) {
					ex.printStackTrace();
				}
			}

	}
}
