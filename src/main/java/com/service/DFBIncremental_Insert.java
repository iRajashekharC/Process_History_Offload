package com.service;


import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

public class DFBIncremental_Insert {
	Map<String, String> stg_base_col_map = new LinkedHashMap<String, String>();
	private String sprint = "sprint8";
	private String stageTable = "";
	private String primIndexColumns;
	private String whereClause="";
	private String inClauseCol = "";
	private String stgTbleColumns = "";
	private String sCreateTempTble = "";
	private String finalStgTbl = "";
	private String finalInsTemp = "";
	String IMP_MESSAGE= "";
	private List<String> baseTableColumns = new ArrayList<String>();
	static Map<String, String> tdHiveDataTypeMapping = new HashMap<String, String>();
	private String insTemp_1 = "";
	StringBuffer baseTableDDL = new StringBuffer();
	private static final String CREATE_DB ="CREATE DATABASE IF NOT EXISTS ${hivevar:hv_database}";
	private static final String DROP_S_TBL = "DROP TABLE IF EXISTS ${hivevar:hv_database}.${hivevar:hv_table}_stg;";
	private static final String DROP_T_TBL = "DROP TABLE IF EXISTS ${hivevar:hv_database}.${hivevar:hv_table}_temp;";
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
	String fileName = "";
	
	/**
	 * please pass <db_name> <table_name> & any one: <upsert|insert|update|delete > isPartitioned\n ex: wm_sales store_info upsert no
	 * @param args
	 */
	public static void main(String[] args) {
		
		if(args.length != 4){
			System.err.println("Argument mismatched!!please pass <db_name> <table_name> & any one: <upsert|insert|update|delete > isPartitioned\n ex: wm_sales store_info upsert no");
		} else {
			String dbName=args[0];
			String tblName=args[1];
			String isPartioned=args[2];
			String isFromFile=args[3];
			DFBIncremental_Insert inc = new DFBIncremental_Insert();
					
			inc.fileName = dbName+"-"+tblName;
			inc.readHiveDDL();
			inc.readInsertFile();
			
			inc.writeToFile(dbName, tblName);
		}
	}
	
	public void getPartitionPrimIndDetails() {
		BufferedReader br = null;
		try {
			String sCurrentLine;
			br = new BufferedReader(new FileReader("C:\\work\\upsert files\\"+fileName+"-ddl.txt"));
			String sField = "";
			while ((sCurrentLine = br.readLine()) != null) {
				sCurrentLine = sCurrentLine.toLowerCase();
				if(!"".equals(sCurrentLine) && ((sCurrentLine.startsWith("primary index")) || sCurrentLine.startsWith("unique primary index"))){
					primIndexColumns = sCurrentLine.substring(sCurrentLine.indexOf("(")+1,sCurrentLine.indexOf(")"));
				}
			}
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
	}
	
	
	public void readInsertFile() {

		BufferedReader br = null;
		try {
			
			String sCurrentLine;
			StringBuffer fullCommand = new StringBuffer();
			br = new BufferedReader(new FileReader("C:\\work\\incremental\\metadata\\"+sprint+"\\"+fileName+"-layout.txt"));
			while ((sCurrentLine = br.readLine()) != null) {
				sCurrentLine = sCurrentLine.toLowerCase();
				if(!"".equals(sCurrentLine)){
					fullCommand.append(" "+sCurrentLine);
				}
			}
			//System.out.println("fullCommand: " + fullCommand.toString());
			String string = fullCommand.toString();
			String fromTbleColmuns = string.substring(string.indexOf(" select ")+8,string.indexOf(" from "));
			String fromTable = string.substring(string.indexOf(" from ")+6,string.indexOf(";"));
			System.out.println("fromTbleColmuns: " + fromTbleColmuns);
			System.out.println("fromTable: " + fromTable);
			//System.out.println("BASE TABLE: " +baseTbleColmuns);
			String stgTbleColumns = string.substring(string.indexOf("values")+6,string.indexOf(";"));
			stgTbleColumns = stgTbleColumns
					.replaceAll(":", "").replaceAll("[()]", "");
			stgTbleColumns = stgTbleColumns.replaceAll("date,format", "");
			stgTbleColumns = stgTbleColumns.replaceAll("'yyyymmdd'", "");
			stgTbleColumns = stgTbleColumns.replaceAll("integer, format", "");
			//System.out.println("STG TABLE: " +stgTbleColumns);
			
			if(stg_base_col_map.size() != baseTableColumns.size()){
				System.err.println("some of the columns are not selected in the INSERT statement, kindly check the insert overwrite statement and manually make necesary changes");
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
	}
	
	
	
	public void readHiveDDL() {
		BufferedReader br = null;
		try {
			String sCurrentLine;
			StringBuffer fullCommand = new StringBuffer();
			br = new BufferedReader(new FileReader("C:\\work\\upsert files\\"+fileName+"-hiveddl.txt"));
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
		bw.write("-- Using TEZ as execution engine\n");
		bw.write("SET hive.execution.engine=tez;\n\n\n");

	}
	public void writeToFile(String dbName, String tblName) {
		String hiveDDLLoc = "C:\\work\\upsert scripts\\"+dbName+"-"+tblName+".hql";
		try {
		BufferedWriter bw = new BufferedWriter(new FileWriter(hiveDDLLoc));
			putHQLHeader(bw);
			bw.write(this.IMP_MESSAGE);
			bw.write("--Creating Database if not exists\n");
			bw.write("CREATE DATABASE IF NOT EXISTS ${hivevar:hv_database};\n\n-- Dropping raw table if present\n"+DROP_S_TBL);
			bw.write("\n\n--creating the base table if not exists \n" +baseTableDDL+"\n\n--creating the stage table if not exists to store raw data\n");
			bw.write(DROP_S_TBL +"\n\n--creating the stage table if not exists to store raw data\n");
			bw.write(this.finalStgTbl.toString() +"\n\n--Dropping table if present\n");
			bw.write(DROP_T_TBL +"\n\n--Create temp table  select from stage table\n");
			bw.write(this.sCreateTempTble +"\n\n--Insert into temp table from stage table\n");
			bw.write(this.finalInsTemp);
			bw.write("\n\n-- Insert into base table from temp table\n\n");
			bw.write(insTemp_1);
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
