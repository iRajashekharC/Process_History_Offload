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
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;

public class DFBIncremental_InsertFromFile {
	Map<String, String> stg_base_col_map = new LinkedHashMap<String, String>();
	
	private String stageTable = "";
	private String sprint = "sprint13";
	private String primIndexColumns;
	private String whereClause="";
	private String inClauseCol = "";
	private String stgTbleColumns = "";
	private String selStgTbleColumns = "";
	private String sCreateTempTble = "";
	private String finalStgTbl = "";
	private String finalInsTemp = "";
	String IMP_MESSAGE= "";
	private Map<Integer, String> columnNameMap = new LinkedHashMap<Integer, String>();
	private List<String> baseTableColumns = new ArrayList<String>();
	static Map<String, String> tdHiveDataTypeMapping = new HashMap<String, String>();
	private String insTemp_1 = "";
	StringBuffer baseTableDDL = new StringBuffer();
	boolean isCobolLayout = false;
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
	String pattern = null;
	/**
	 * please pass <db_name> <table_name> & any one: <upsert|insert|update|delete > isPartitioned\n ex: wm_sales store_info upsert no
	 * @param args
	 */
	public static void main(String[] args) {
		DFBIncremental_InsertFromFile inc = new DFBIncremental_InsertFromFile();
		inc.init(inc, args);
	}

	public void init(DFBIncremental_InsertFromFile inc, String[] args) {
		if(args.length != 4){
			System.err.println("Argument mismatched!!please pass <db_name> <table_name> & any one: <upsert|insert|update|delete > isPartitioned\n ex: wm_sales store_info upsert no");
		} else {
			MySQLToHiveDDLConvertor ddlHQLCreator = new MySQLToHiveDDLConvertor();
			try {
				String dbName=args[0];
				String tblName=args[1];
				String isCobolLayout = args[3];

				inc.pattern =args[2];
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
				inc.readHiveDDL();
				inc.createStageTable();
				inc.getLoadFileToBaseTbleMapping();
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
	
	public void getLoadFileToBaseTbleMapping() {

		BufferedReader br = null;
		try {
			
			String sCurrentLine;
			StringBuffer fullCommand = new StringBuffer();
			br = new BufferedReader(new FileReader("C:\\work\\incremental\\metadata\\"+sprint+"\\"+fileName+"-insert.txt"));
			while ((sCurrentLine = br.readLine()) != null) {
				sCurrentLine = sCurrentLine.toLowerCase();
				if(!"".equals(sCurrentLine)){
					fullCommand.append(sCurrentLine.trim().replaceAll(" +", " "));
				}
			}
			
			fullCommand = new StringBuffer(fullCommand.toString().trim().replaceAll("\\s{2,}", " "));
			//System.out.println("fullCommand: " + fullCommand.toString());
			//String baseTbleColmuns = fullCommand.toString().substring(fullCommand.toString().indexOf("(")+1,fullCommand.toString().indexOf("values"));
			int insIndex = fullCommand.toString().indexOf("insert ");
			if(insIndex == -1){
				insIndex = fullCommand.toString().indexOf("ins ");
			}
			String baseTbleColmuns = fullCommand.toString().substring(insIndex);
			int valIndex = baseTbleColmuns.indexOf("values");
			if(valIndex == -1){
				valIndex = baseTbleColmuns.toString().indexOf(");");
				valIndex+=2;
			}
			
			String tempBaseTbleColmuns = baseTbleColmuns.substring(baseTbleColmuns.indexOf("(")-1,valIndex);
			baseTbleColmuns = baseTbleColmuns.toString().substring(baseTbleColmuns.toString().indexOf("(")+1,valIndex);
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
			//System.out.println("missingList: " + missingList.toString().substring(1,missingList.toString().length()-1));
			
			//String stgTbleColumns = fullCommand.toString().substring(fullCommand.toString().indexOf("values")+6);
			String stgTbleColumns  = tempBaseTbleColmuns;
			stgTbleColumns = stgTbleColumns.replaceAll("date,format", "");
			stgTbleColumns = stgTbleColumns.replaceAll(" 'yyyymmdd'", "");
			stgTbleColumns = stgTbleColumns.replaceAll("format", "");
			stgTbleColumns = stgTbleColumns.replaceAll(" 'YYYY-MM-DD'".toLowerCase(), "");
			stgTbleColumns = stgTbleColumns.replaceAll(" 'mm/dd/yyyy'", "");
			stgTbleColumns = stgTbleColumns.replaceAll("integer, format", "");
			stgTbleColumns = stgTbleColumns.replaceAll(" 'mm-dd-yyyy'", "");
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
					
					System.out.println("Staging table Count     : "+stgTbleColumns.split(",").length );
					System.out.println("Base table Count(insert): "+baseTbleColmuns.split(",").length);
					System.out.println("Base table cnt (hiveddl): "+baseTableColumns.size());
				/*if(stg_base_col_map.size() != baseTableColumns.size()){
					System.err.println("some of the columns are not selected in the INSERT statement, kindly check the insert overwrite statement and manually make necesary changes");
				}*/
				
				
				if(selStgTbleColumns.split(",").length == baseTbleColmuns.split(",").length){
					
					for (int i=0; i < selStgTbleColumns.split(",").length; i++) {
						stg_base_col_map.put(baseTbleColmuns.split(",")[i], selStgTbleColumns.split(",")[i]);
					}
				}
				else if(selStgTbleColumns.split(",").length < baseTbleColmuns.split(",").length) {
					for (int i=0; i < selStgTbleColumns.split(",").length; i++) {
						stg_base_col_map.put(baseTbleColmuns.split(",")[i], selStgTbleColumns.split(",")[i]);
					}
				}
				else if(selStgTbleColumns.split(",").length > baseTbleColmuns.split(",").length) {
					for (int i=0; i < baseTbleColmuns.split(",").length; i++) {
						stg_base_col_map.put(baseTbleColmuns.split(",")[i], selStgTbleColumns.split(",")[i]);
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
	
	
	/**private  void createStageTable() {
		BufferedReader br = null;

		try {

			String sCurrentLine;

			br = new BufferedReader(new FileReader("C:\\work\\incremental\\metadata\\"+sprint+"\\"+fileName+"-layout.txt"));
			String sField = "FIELD";
			String sFiller = "filler";
			StringBuffer sBuffer = new StringBuffer("CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:hv_database}.${hivevar:hv_table}_stg(\n");
			boolean diffLayout = Boolean.FALSE;
			while ((sCurrentLine = br.readLine()) != null) {
				sCurrentLine = sCurrentLine.toLowerCase().trim();
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
					}else if(sCurrentLine.toLowerCase().trim().startsWith("field")){
						sCurrentLine = sCurrentLine.replaceAll(sField.toLowerCase(), "");
						boolVal = true;
					}
					else if(sCurrentLine.toLowerCase().startsWith(".filler")){
						sCurrentLine = sCurrentLine.replaceFirst("."+sFiller.toLowerCase(), "");
						boolVal = true;
					}
					else if(sCurrentLine.toLowerCase().startsWith("filler")){
						sCurrentLine = sCurrentLine.replaceFirst(sFiller.toLowerCase(), "");
						boolVal = true;
					}
					else if(sCurrentLine.toLowerCase().trim().startsWith("define")){
						diffLayout = true;
					}
					if(boolVal) {
						sCurrentLine = sCurrentLine.trim();
						String[] colDatType = sCurrentLine.split("\\*");
						if(colDatType.length != 2) {
							
						}
						String tmp = (colDatType[1].indexOf("nullif")>0)?(colDatType[1].substring(0,colDatType[1].indexOf("nullif"))+"#"):colDatType[1].substring(0,colDatType[1].length())+"!";
						Set<Entry<String, String>> entrySet = tdHiveDataTypeMapping.entrySet();
						for (Entry<String, String> string : entrySet) {
							tmp = tmp.replaceAll(string.getKey().toLowerCase(), string.getValue().toLowerCase()).replaceAll("[()]", "");
						}
						sBuffer.append("    "+colDatType[0] + "   " +tmp.substring(0,tmp.length()-1).replaceAll(";", "").trim()+",\n");
						stgTbleColumns =stgTbleColumns+colDatType[0].trim()+"\n";
					}
					if(diffLayout && !"define".toLowerCase().equals(sCurrentLine)){
						sCurrentLine = sCurrentLine.trim().replaceAll("\\s{2,}", " ").trim();
						if(sCurrentLine.startsWith(",")) {
							sCurrentLine = sCurrentLine.substring(1).trim()+",";
						}
						Set<Entry<String, String>> entrySet = tdHiveDataTypeMapping.entrySet();
						for (Entry<String, String> string : entrySet) {
							sCurrentLine = sCurrentLine.replaceAll(string.getKey().toLowerCase(), string.getValue().toLowerCase());
						}
						
						while(true){
							if(sCurrentLine.indexOf("( ")>0 || sCurrentLine.indexOf(" )")>0){
								sCurrentLine = sCurrentLine.replaceAll("\\(\\s", "(").replaceAll("\\s\\)", "\\)");
							}
							else {
								break;
							}
						}
						
						String[] tmp = sCurrentLine.split(" ");
						System.err.println(sCurrentLine);
						sBuffer.append("  "+tmp[0] + "                 "+ tmp[1].substring(tmp[1].indexOf("(")+1,tmp[1].lastIndexOf(")") )+",\n");
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
			
			stageTable = finalStgTbl;
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
	*/
	
	/**
	 * Creates the stage table
	 */
	private void createStageTable() {
		BufferedReader br = null;
		String str = "";
		try {
			String sCurrentLine;
			br = new BufferedReader(new FileReader(
					"C:\\work\\incremental\\metadata\\" + sprint + "\\"
							+ fileName + "-layout.txt"));
			StringBuffer sBuffer = new StringBuffer(
					"CREATE EXTERNAL TABLE IF NOT EXISTS ${hivevar:hv_database}.${hivevar:hv_table}_stg(\n");
			String sField = "FIELD";

			while ((sCurrentLine = br.readLine()) != null) {
				sCurrentLine = sCurrentLine.toLowerCase();
				if (!"".equals(sCurrentLine)) {
					if (isCobolLayout) {
						Map<String, String> cobolMapping = new HashMap<String, String>();
						cobolMapping.put("pic~s9##4%%", "int");
						cobolMapping.put("pic~s9##9%%", "int");
						cobolMapping.put("pic~x", "string");
						cobolMapping.put("pic~x##", "char(");

						String[] split = sCurrentLine.split("\\n");
						StringBuffer sBuffer1 = new StringBuffer();
						for (String string : split) {
							// System.out.println("--> " + string);
							string = string.trim();
							String s = "";
							if (string.startsWith("03")) {
								s = string.replaceAll("03", "").trim()
										.replaceAll("COMP\\.", "").trim()
										.replaceAll("COMP-3\\.", "")
										.replaceAll("\\.", "").toLowerCase();
								int hyphenIndex = -100;
								hyphenIndex = s.indexOf("-");
								if (hyphenIndex != -100 || hyphenIndex != -1) {
									s = s.substring(hyphenIndex + 1);
								}
								s = s.replaceAll(" pic s", " pic~s")
										.replaceAll("pic x", "pic~x")
										.replaceAll("-", "_").replaceAll("\\(",
												"##").replaceAll("\\)", "%%");
								String[] split2 = s.split("\\s+");

								Set<Entry<String, String>> entrySet = cobolMapping
										.entrySet();
								for (Entry<String, String> string1 : entrySet) {
									if (split2[1].startsWith("pic~s99")) {
										String[] decimalSplt1 = split2[1]
												.split("v");
										int nineOccurnce1 = StringUtils
												.countMatches(decimalSplt1[0],
														"9");
										int nineOccurnce2 = StringUtils
												.countMatches(decimalSplt1[1],
														"9");
										// System.out.println("nineOccurnce1: "
										// + nineOccurnce1 +" -- "+
										// nineOccurnce2);
										String str1 = "decimal("
												+ nineOccurnce1 + ","
												+ nineOccurnce2 + ")";
										s = s.replaceAll(split2[1], str1);
									} else if (split2[1].contains("v9")) {
										String nineOccurnce2 = "";
										String[] decimalSplt1 = split2[1]
												.split("v");
										String nineOccurnce1 = decimalSplt1[0]
												.substring(decimalSplt1[0]
														.lastIndexOf("#") + 1,
														decimalSplt1[0]
																.indexOf("%"));
										if (decimalSplt1[1].contains("#")) {
											nineOccurnce2 = decimalSplt1[1]
													.substring(
															decimalSplt1[1]
																	.lastIndexOf("#") + 1,
															decimalSplt1[1]
																	.indexOf("%"));
										} else {
											nineOccurnce2 = String
													.valueOf(StringUtils
															.countMatches(
																	decimalSplt1[1],
																	"9"));
										}
										// System.out.println("nineOccurnce1: "
										// + nineOccurnce1 +" -- "+
										// nineOccurnce2);
										String str1 = "decimal("
												+ nineOccurnce1 + ","
												+ nineOccurnce2 + ")";
										s = s.replaceAll(split2[1], str1);
									} else if (split2[1].startsWith(string1
											.getKey().toLowerCase())) {
										s = s.replaceAll(string1.getKey()
												.toLowerCase(), string1
												.getValue().toLowerCase());
										if (s.contains("%%")) {
											s = s.replaceAll("%%", ")");
										}
										break;
									}
								}
								sBuffer1.append(s.toLowerCase() + ",");
								sBuffer1.append("\n");
								str = sBuffer1.toString().substring(0,
										sBuffer1.toString().length() - 1);
							}
						}

					} else {
						boolean boolVal = Boolean.FALSE;
						// System.out.println(sCurrentLine);
						if (sCurrentLine.toLowerCase().startsWith(".field")) {
							sCurrentLine = sCurrentLine.replaceAll("."
									+ sField.toLowerCase(), "");
							boolVal = true;
						} else if (sCurrentLine.toLowerCase().startsWith(
								"field")) {
							sCurrentLine = sCurrentLine.replaceAll(sField
									.toLowerCase(), "");
							boolVal = true;
						}
						if (boolVal) {
							// System.out.println("After replace field: "
							// +sCurrentLine);
							sCurrentLine = sCurrentLine.trim();
							// System.out.println("After trimming: "
							// +sCurrentLine);
							String[] colDatType = sCurrentLine.split("\\*");
							// System.out.println("colDatType[0]: " +
							// colDatType[0]);
							// System.out.println(colDatType[0]+""+colDatType[1]+
							// "-->" +tdHiveDataTypeMapping.get(colDatType[1]));
							String tmp = (colDatType[1].indexOf("nullif") > 0) ? (colDatType[1]
									.substring(0, colDatType[1]
											.indexOf("nullif")) + "#")
									: colDatType[1].substring(0, colDatType[1]
											.length())
											+ "!";
							Set<Entry<String, String>> entrySet = tdHiveDataTypeMapping
									.entrySet();
							for (Entry<String, String> string : entrySet) {
								tmp = tmp.replaceAll(string.getKey()
										.toLowerCase(), string.getValue()
										.toLowerCase());
							}
							String colValue = "";
							colValue = replaceItemNbr(colDatType[0].trim());

							sBuffer.append("    "
									+ colValue
									+ "   "
									+ tmp.substring(0, tmp.length() - 1)
											.replaceAll(";", "").trim() + ",\n");
							stgTbleColumns = stgTbleColumns
									+ colDatType[0].trim() + "\n";
							// System.out.println("===============");
						}
						str = sBuffer.toString().substring(0,
								sBuffer.toString().length() - 1);
					}
				}
			}

			sBuffer = new StringBuffer();
			sBuffer.append(str.substring(0, str.length() - 1) + ")"
					+ "\nROW FORMAT DELIMITED" + "\nFIELDS TERMINATED BY '|'"
					+ "\nLOCATION '${hivevar:hv_loc}';");
			finalStgTbl = sBuffer.toString().replaceAll("/;/!", "").substring(
					0, sBuffer.toString().length() - 1)
					+ ";";

			stageTable = finalStgTbl;
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
	
	/**
	 * 
	 */
	public void getPartitionPrimIndDetails() {
		BufferedReader br = null;
		try {
			String sCurrentLine;
			br = new BufferedReader(new FileReader("C:\\work\\incremental\\metadata\\"+sprint+"\\"+fileName+"-ddl.txt"));
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
			br = new BufferedReader(new FileReader("C:\\work\\incremental\\metadata\\"+sprint+"\\"+fileName+"-insert.txt"));
			while ((sCurrentLine = br.readLine()) != null) {
				sCurrentLine = sCurrentLine.toLowerCase();
				if(!"".equals(sCurrentLine)){
					fullCommand.append(" "+sCurrentLine);
				}
			}
			//System.out.println("fullCommand: " + fullCommand.toString());
			String string = fullCommand.toString();
			String fromTbleColmuns = string.substring(string.indexOf("select ")+8,string.indexOf(" from "));
			String fromTable = string.substring(string.indexOf(" from ")+6,string.indexOf(";"));
			System.out.println("fromTbleColmuns: " + fromTbleColmuns);
			System.out.println("fromTable: " + fromTable);
			//System.out.println("BASE TABLE: " +baseTbleColmuns);
			String stgTbleColumns = string.substring(string.indexOf("values")+6,string.indexOf(";"));
			stgTbleColumns = stgTbleColumns
					.replaceAll(":", "").replaceAll("[()]", "");
			stgTbleColumns = stgTbleColumns.replaceAll("date,format", "");
			stgTbleColumns = stgTbleColumns.replaceAll("'yyyymmdd'", "");
			stgTbleColumns = stgTbleColumns.replaceAll("'mm/dd/yyyy'", "");
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
			br = new BufferedReader(new FileReader("C:\\work\\incremental\\metadata\\"+sprint+"\\"+fileName+"-hiveddl.txt"));
			while ((sCurrentLine = br.readLine()) != null) {
				//sCurrentLine = sCurrentLine.toLowerCase();
				if(sCurrentLine.trim().startsWith("CREATE TABLE")){
					sCurrentLine = sCurrentLine.replace("(", "_temp(");
				}
				sCurrentLine=sCurrentLine.replaceAll("hiveconf", "hivevar");
				if(!"".equals(sCurrentLine)){
					fullCommand.append(sCurrentLine.trim());
				}
				String[] split = sCurrentLine.trim().split(" ");
				if(split.length==2){
					baseTableDDL.append(split[0]+"                    "+split[1]+"\n");
				} else{
					baseTableDDL.append(""+sCurrentLine+"\n");
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
		bw.write("-- # TITLE: <HQL_FILENAME>.hql                                               #\n");
		bw.write("-- # DESCRIPTION: The job loads the records for <TABLE_NAME>                 #\n");
		bw.write("-- #=========================================================================#\n");
		bw.write("-- # REVISION HISTORY                                                        #\n");
		bw.write("-- #-------------------------------------------------------------------------#\n");
		bw.write("-- #2017-02-15 :: v1.0  :: <developer_name>     :: Created  <DATE>           #\n");
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
		bw.write("SET mapreduce.reduce.java.opts=-Xmx3072m;\nSET hive.warehouse.subdir.inherit.perms=true;\n\n");
		bw.write("--Added the below parameter to enable tez execution engine\n");
		bw.write("SET hive.execution.engine=tez;\nSET tez.runtime.io.sort.mb=1600;\n\n");

	}
	public void writeToFile(String dbName, String tblName) {
		String hiveDDLLoc = "C:\\work\\incremental\\generated\\"+sprint+"\\"+dbName+"-"+tblName+".hql";
		try {
		BufferedWriter bw = new BufferedWriter(new FileWriter(hiveDDLLoc));
			putHQLHeader(bw);
			//bw.write(this.IMP_MESSAGE);
			bw.write("--Creating the database if not exists\n");
			bw.write("CREATE DATABASE IF NOT EXISTS ${hivevar:hv_database};\n\n--Logging into the database before executing the below HQLs\nUSE ${hivevar:hv_database};\n\n--Drop the stage table if not exists \n"+DROP_S_TBL);
			//bw.write("\n\n--creating the stage table if not exists to store raw data\n" +baseTableDDL+"\n\n--Drop stage table if exists\n");
			bw.write("\n\n--Creating the stage table if not exists to store raw data\n");
			bw.write(this.finalStgTbl.toString()+"\n\n");
			
			if(null != pattern && !"".equals(pattern)) {
				if("overwrite".equals(pattern)){
					bw.write("--Drop the temp table if  exists\n");
					bw.write("DROP TABLE IF EXISTS ${hivevar:hv_database}.${hivevar:hv_table}_temp;\n");
					bw.write("\n--Creating the temp table if does not exists\n" +baseTableDDL+"\n");
					
					bw.write("--Insert into base table staging table records\n");
					bw.write("INSERT INTO TABLE ${hivevar:hv_database}.${hivevar:hv_table}_temp\n");
					bw.write("SELECT \n     DISTINCT "+ selStgTbleColumns.substring(2,selStgTbleColumns.length()-1).replaceAll(",", ",\n    ").replaceAll("6000101", "TO_DATE(FROM_UNIXTIME(16725225600))"));
					bw.write(" \n FROM ${hivevar:hv_database}.${hivevar:hv_table}_stg;");
					bw.write("\n\n--Drop base table if exists\n");
					bw.write("DROP TABLE IF EXISTS ${hivevar:hv_database}.${hivevar:hv_table};\n");
					bw.write("\n--Rename temp to base table\n");
					bw.write("ALTER TABLE ${hivevar:hv_database}.${hivevar:hv_table}_temp RENAME TO ${hivevar:hv_database}.${hivevar:hv_table};\n");
					bw.write("\n\n--Drop the stage table if already exists");
					bw.write("\n--DROP TABLE IF EXISTS ${hivevar:hv_database}.${hivevar:hv_table}_stg;");
				}
				/**else if("distinct".equals(pattern)){
					bw.write("-- Insert into Temp table from Stage table\n");
					bw.write("INSERT INTO TABLE ${hivevar:hv_database}.${hivevar:hv_table}\n");
					bw.write("SELECT DISTINCT "+ selStgTbleColumns.substring(2,selStgTbleColumns.length()-2).replaceAll(",", ",\n  ")+" FROM ${hivevar:hv_database}.${hivevar:hv_table}_stg;");
				}*/
				else{
					bw.write("--Drop the temp table if  exists\n");
					bw.write("DROP TABLE IF EXISTS ${hivevar:hv_database}.${hivevar:hv_table}_temp;\n");
					bw.write("\n--creating the temp table if does not exists\n");
					bw.write("CREATE TABLE ${hivevar:hv_database}.${hivevar:hv_table}_temp LIKE ${hivevar:hv_database}.${hivevar:hv_table};\n");
					
					bw.write("\n--Creating the base table if does not exists\n" +baseTableDDL+"\n\n");
					
					bw.write("\n--insert into base table from staging table \n");
					bw.write("INSERT INTO TABLE ${hivevar:hv_database}.${hivevar:hv_table}\n");
					
					bw.write("SELECT "+selStgTbleColumns.substring(2,selStgTbleColumns.length()-1).replaceAll(",", ",\n       ")+"\n        FROM ${hivevar:hv_database}.${hivevar:hv_table}_stg;");
					
					bw.write("\n\n-- Insert DISTINCT records into temp table\n");
					bw.write("INSERT INTO TABLE  ${hivevar:hv_database}.${hivevar:hv_table}_temp");
					bw.write("\nSELECT DISTINCT * FROM ${hivevar:hv_database}.${hivevar:hv_table};\n\n");
					bw.write("--drop base table if exists\n");
					bw.write("DROP TABLE IF EXISTS ${hivevar:hv_database}.${hivevar:hv_table};\n\n");
					bw.write("--rename temp to base table\n");
					bw.write("ALTER TABLE ${hivevar:hv_database}.${hivevar:hv_table}_temp RENAME TO ${hivevar:hv_database}.${hivevar:hv_table};\n\n");
					bw.write("--Drop the stage table if already exists\n");
					bw.write("--DROP TABLE IF EXISTS ${hivevar:hv_database}.${hivevar:hv_table}_stg;");
				}
			}
			
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Replaces the ITEM_NBR to mds_fam_id
	 * @param string
	 * @return
	 */
	private String replaceItemNbr(String string) {
		String colValue = string;
		if ("ITEM_NBR".toLowerCase().equals(string.trim().toLowerCase()))
			colValue = "mds_fam_id";

		return colValue;
	}
}
