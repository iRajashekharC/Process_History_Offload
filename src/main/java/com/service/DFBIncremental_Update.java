package com.service;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

public class DFBIncremental_Update {
	Map<String, String> stg_base_col_map = new LinkedHashMap<String, String>();
	
	private String stageTable = "";
	private String primIndexColumns;
	private String whereClause="";
	private String whereColumns="";
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
	String temp1Create =  "";
	String temp2Create =  "";
	String temp3Create =  "";
	String insBaseTable =  "";
	Map<String, String> hardCodedColumns =new  LinkedHashMap<String, String>();
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
		tdHiveDataTypeMapping.put("VARCHAR", "VARCHAR");
		tdHiveDataTypeMapping.put("DATE", "DATE"); // formatting is needed asper needs
		tdHiveDataTypeMapping.put("TIME", "STRING");
		tdHiveDataTypeMapping.put("TIMESTAMP", "TIMESTAMP");
	}
	String fileName = "";
	String updateFrmFile;
	String isPartioned;
	String tblName;
	String dbName;
	
	/**
	 * please pass <db_name> <table_name> & any one: <upsert|insert|update|delete > isPartitioned\n ex: wm_sales store_info upsert no
	 * @param args
	 */
	public static void main(String[] args) {
		
		if(args.length != 4){
			System.err.println("Argument mismatched!!please pass <db_name> <table_name> & any one: <upsert|insert|update|delete > isPartitioned\n ex: wm_sales store_info upsert no");
		} else {
			DFBIncremental_Update inc = new DFBIncremental_Update();
			inc.dbName=args[0];
			inc.tblName=args[1];
			inc.isPartioned=args[3];
			inc.updateFrmFile=args[2];
			inc.fileName = inc.dbName+"-"+inc.tblName;
			inc.readHiveDDL();
			inc.getPartitionPrimIndDetails();
			inc.prepareForUpdate();
			//Temp Table
			inc.createTempTbl();
			//System.out.println("sCreateTempTble: "+inc.sCreateTempTble);
			
			//Load file to Base Mapping
			inc.getLoadFileToBaseTbleMapping();
			inc.writeToFile(inc.dbName, inc.tblName);
		
			}
	}
	private  void createStageTable() {
		BufferedReader br = null;

		try {

			String sCurrentLine;

			br = new BufferedReader(new FileReader("C:\\work\\upsert files\\"+fileName+"-layout.txt"));
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
		
		BufferedReader br1 = null;
		try {
			String sCurrentLine;
			String nwWhereClz = null ;
			br1 = new BufferedReader(new FileReader("C:\\work\\upsert files\\"+fileName+"-update.txt"));
			StringBuffer fullCommand = new StringBuffer("");
			StringBuffer newWhereClause = new StringBuffer("");
			while ((sCurrentLine = br1.readLine()) != null) {
				sCurrentLine = sCurrentLine.toLowerCase();
				fullCommand.append(sCurrentLine+" ");
			}
			String updateSql = fullCommand.toString();
			
			String updateColumns = fullCommand.toString().substring(fullCommand.toString().indexOf(" set ")+4, fullCommand.toString().indexOf(" where "));
			System.err.println("updateColumns: " + updateColumns);
			String[] split2 = updateColumns.split("=");
			if(null != split2 && split2.length>0){
				hardCodedColumns.put(split2[0], split2[1]);
			}
			updateSql = updateSql.substring(updateSql.indexOf("where")+5,updateSql.length()-1);
			//System.err.println("updateSql" +updateSql);
			
			
				String[] split = updateSql.split("and");
				for (String string : split) {
					System.out.println(string);
					String[] splitEqual=null;
					String[] splitNotEq=null;
					string = string.trim();
					if(string.indexOf('=')>0){
						splitEqual = string.trim().split("=");
					} else if(string.indexOf("<>")>0){
						splitNotEq = string.trim().split("<>");
					}
					if(!"".equals(this.updateFrmFile) && "no".equalsIgnoreCase(this.updateFrmFile)){
						newWhereClause.append("m."+ string.trim()+" AND ");
						if(null != splitNotEq && splitNotEq.length>0 )
							whereColumns +=splitNotEq[0].trim()+",";
						else if(null != splitEqual  && splitEqual.length>0)
								whereColumns +=splitEqual[0].trim()+",";
					}
					else {
						newWhereClause.append("m."+ splitEqual[0].trim()+" = s."+splitEqual[1].trim().replaceAll(":", "")+ " AND ");
					}
					 nwWhereClz =  newWhereClause.toString().substring(0, newWhereClause.toString().length()-4);
			}
				whereColumns = whereColumns.substring(0, whereColumns.length()-1);
				System.out.println("whereColumns: " +whereColumns);
				whereClause = nwWhereClz;
				System.out.println(whereClause);
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
		sbCreateTempTble.append("WHERE "+whereClause.trim() +" ;");
		sCreateTempTble = sbCreateTempTble.toString();
	}
	
	public void prepareForUpdate() {
		StringBuffer sbCreateTemp = new StringBuffer();
		sbCreateTemp.append(" create table ${hiveconf:hv_database}.${hiveconf:hv_table}_temp1 as\n");
		sbCreateTemp.append(" select * from ${hiveconf:hv_database}.${hiveconf:hv_table} m\n");
		sbCreateTemp.append(" where "+whereClause+");");
		
		System.err.println(sbCreateTemp);
		temp1Create = sbCreateTemp.toString();
		// Construct temp2
		sbCreateTemp = new StringBuffer();
		sbCreateTemp.append(" drop table if exists ${hiveconf:hv_database}.${hiveconf:hv_table}_temp2;\n\n");
		sbCreateTemp.append(" create table ${hiveconf:hv_database}.${hiveconf:hv_table}_temp2 as \n");
		sbCreateTemp.append(" select m.* from ${hiveconf:hv_database}.${hiveconf:hv_table} m \n");
		sbCreateTemp.append(" where m."+whereColumns.split(",")[0] +" NOT IN ( ");
		sbCreateTemp.append(" select distinct s."+whereColumns.split(",")[0]+" from ${hiveconf:hv_database}.${hiveconf:hv_table}_temp1 s\n");
		sbCreateTemp.append(" where ");
		if(whereColumns.indexOf(",")>0) {
			for (String str : whereColumns.split(",")) {
				sbCreateTemp.append("m."+str.trim()+"=s."+str.trim()+" AND ");
			}
		}
		sbCreateTemp.append(");");
		temp2Create = sbCreateTemp.toString();
		temp2Create = temp2Create.substring(0, temp2Create.length()-6)+");";
		System.err.println("baseTableColumns: "+ baseTableColumns);
		
		// Construct temp3
		sbCreateTemp = new StringBuffer();
		sbCreateTemp.append("insert  into table ${hiveconf:hv_database}.${hiveconf:hv_table}_temp2\n");
		Set<Entry<String, String>> entrySet = hardCodedColumns.entrySet();
		for (Entry<String, String> string : entrySet) {
			sbCreateTemp.append(baseTableColumns.toString().substring(1,baseTableColumns.toString().length()-1).replaceAll(string.getKey().toLowerCase().trim(), string.getValue().toLowerCase()));
		}
		sbCreateTemp.append(" \n from ${hiveconf:hv_database}.${hiveconf:hv_table}_temp1;");
		temp3Create = sbCreateTemp.toString();
		
		//Construct Base table insert
		sbCreateTemp = new StringBuffer();
		sbCreateTemp.append("insert overwrite table ${hiveconf:hv_database}.${hiveconf:hv_table}\n");
		for (String col : baseTableColumns) {
			sbCreateTemp.append(col+",");
		}
		sbCreateTemp.append(" \nfrom ${hiveconf:hv_database}.${hiveconf:hv_table}_temp2;");
		insBaseTable = sbCreateTemp.toString();
		/*
		 * insert  into table ${hiveconf:hv_database}.${hiveconf:hv_table}_temp2
select store_nbr,scan_id,scan_type_code,visit_date,report_code,upc_nbr,unit_qty,uom_code,"0",cost_amt,scan_cnt
from ${hiveconf:hv_database}.${hiveconf:hv_table}_temp1;
		 * */
		 
	}
	public void getLoadFileToBaseTbleMapping() {

		BufferedReader br = null;
		try {
			
			String sCurrentLine;
			StringBuffer fullCommand = new StringBuffer();
			br = new BufferedReader(new FileReader("C:\\work\\upsert files\\"+fileName+"-insert.txt"));
			while ((sCurrentLine = br.readLine()) != null) {
				sCurrentLine = sCurrentLine.toLowerCase();
				if(!"".equals(sCurrentLine)){
					fullCommand.append(sCurrentLine);
				}
			}
			//System.out.println("fullCommand: " + fullCommand.toString());
			String baseTbleColmuns = fullCommand.toString().substring(fullCommand.toString().indexOf("(")+1,fullCommand.toString().indexOf("values"));
			baseTbleColmuns = baseTbleColmuns.substring(0,baseTbleColmuns.indexOf(')')-1);
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
			
			String stgTbleColumns = fullCommand.toString().substring(fullCommand.toString().indexOf("values")+6,fullCommand.toString().indexOf(";"));
			stgTbleColumns = stgTbleColumns
					.replaceAll(":", "").replaceAll("[()]", "");
			stgTbleColumns = stgTbleColumns.replaceAll("date,format", "");
			stgTbleColumns = stgTbleColumns.replaceAll("'yyyymmdd'", "");
			stgTbleColumns = stgTbleColumns.replaceAll("integer, format", "");
			//System.out.println("STG TABLE: " +stgTbleColumns);
			
			System.out.println("Staging table Count     : "+stgTbleColumns.split(",").length );
			System.out.println("Base table Count(insert): "+baseTbleColmuns.split(",").length);
			System.out.println("Base table cnt (hiveddl): "+baseTableColumns.size());
			if(stg_base_col_map.size() != baseTableColumns.size()){
				System.err.println("some of the columns are not selected in the INSERT statement, kindly check the insert overwrite statement and manually make necesary changes");
			}
			if(stgTbleColumns.split(",").length == baseTbleColmuns.split(",").length){
				for (int i=0; i < stgTbleColumns.split(",").length; i++) {
					stg_base_col_map.put(baseTbleColmuns.split(",")[i], stgTbleColumns.split(",")[i]);
				}
				//System.out.println("stg_base_col_map: "+ stg_base_col_map);
			}
			else if(stgTbleColumns.split(",").length < baseTbleColmuns.split(",").length) {
				
			}
			else {
				IMP_MESSAGE = "*******Columns count is not matching with Base and Stage table, please check if columns are hard-coded!!!";
			}
			
			//Prepare insert into Temp step 1
			StringBuffer sbInsTemp = new StringBuffer("INSERT  INTO  TABLE ${hivevar:hv_database}.${hivevar:hv_table}_temp \n SELECT \n");
			Set<Entry<String, String>> entrySet = stg_base_col_map.entrySet();
			for (Entry<String, String> string : entrySet) {
				sbInsTemp.append("  "+ string.getValue().toLowerCase().trim()+",\n");
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
			
			br = new BufferedReader(new FileReader("C:\\work\\upsert files\\"+fileName+"-hiveddl.txt"));
			while ((sCurrentLine = br.readLine()) != null) {
				sCurrentLine = sCurrentLine.toLowerCase();
				sCurrentLine=sCurrentLine.replaceAll("hiveconf", "hivevar");
				if(!"".equals(sCurrentLine)){
					fullCommand.append(sCurrentLine.trim());
				}
				baseTableDDL.append(sCurrentLine+"\n");
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
			bw.write("CREATE DATABASE IF NOT EXISTS ${hivevar:hv_database};\n\n");
			bw.write("\n\n--creating the base table if not exists \n" +baseTableDDL+"\n\n");
			bw.write("drop table if exists ${hiveconf:hv_database}.${hiveconf:hv_table}_temp1;\n\n");
			bw.write(this.temp1Create +"\n\n");
			bw.write(this.temp2Create +"\n\n");
			bw.write(this.temp3Create +"\n\n");
			bw.write("\n\n-- Insert into base table from temp table\n\n");
			bw.write(this.insBaseTable+"\n\n");
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
