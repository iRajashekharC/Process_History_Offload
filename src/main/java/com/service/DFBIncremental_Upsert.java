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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;

public class DFBIncremental_Upsert {
	Map<String, String> baseColumnsToValMapping = new HashMap<String, String>();
	private static final String ALTER_FIN_TO_BASE = "ALTER TABLE ${hivevar:hv_database}.${hivevar:hv_table}_final RENAME TO ${hivevar:hv_database}.${hivevar:hv_table};";
	private static final String DROP_BASE_TABLE = "DROP TABLE IF EXISTS ${hivevar:hv_database}.${hivevar:hv_table};";
	private static final String DROP_FINAL_TABLE = "DROP TABLE IF EXISTS ${hivevar:hv_database}.${hivevar:hv_table}_final;";
	private static final String CREATE_FINAL_TABLE = "CREATE TABLE ${hivevar:hv_database}.${hivevar:hv_table}_final LIKE ${hivevar:hv_database}.${hivevar:hv_table};";
	Map<String, String> stg_base_col_map = new LinkedHashMap<String, String>();
	Map<String, String> updateColumnsMap = new LinkedHashMap<String, String>();
	private static String sprint = "sprint14";
	private String stageTable = "";
	private String primIndexColumns;
	private String whereClause = "";
	private String inClauseCol = "";
	private String stgTbleColumns = "";
	private String sCreateTempTble = "";
	private String finalStgTbl = "";
	private String finalInsTemp = "";
	String IMP_MESSAGE = "";
	StringBuffer whereConditionColmns = new StringBuffer();
	private List<String> baseTableInsertColumns = new ArrayList<String>();
	private List<String> baseTableValuesCol = new ArrayList<String>();
	private List<String> updatedBaseTableColumns = new ArrayList<String>();
	static Map<String, String> tdHiveDataTypeMapping = new HashMap<String, String>();
	Map<String, String> baseToInsertColMap = new LinkedHashMap<String, String>();
	private String insTemp_1 = "";
	StringBuffer baseTableDDL = new StringBuffer();
	private static final String CREATE_DB = "CREATE DATABASE IF NOT EXISTS ${hivevar:hv_database}";
	private static final String DROP_S_TBL = "DROP TABLE IF EXISTS ${hivevar:hv_database}.${hivevar:hv_table}_stg;";
	private static final String DROP_T_TBL = "DROP TABLE IF EXISTS ${hivevar:hv_database}.${hivevar:hv_table}_temp;";
	private static final String DROP_ROWNUM_TBL = "DROP TABLE IF EXISTS ${hivevar:hv_database}.${hivevar:hv_table}_rownum;";
	private static final String CREATE_ROWNUM_TBL = "CREATE TABLE ${hivevar:hv_database}.${hivevar:hv_table}_rownum as\n SELECT ROW_NUMBER() OVER() as ROW_NUM,* from ${hivevar:hv_database}.${hivevar:hv_table}_stg;";
	private static final String DROP_NODUPS_TBL = "DROP TABLE IF EXISTS ${hivevar:hv_database}.${hivevar:hv_table}_nodups;";
	private static final String CREATE_NODUPS_TBL = "CREATE TABLE ${hivevar:hv_database}.${hivevar:hv_table}_nodups \nAS \nSELECT * FROM ${hivevar:hv_database}.${hivevar:hv_table}_rownum \nWHERE row_num IN (\n              SELECT MAX(row_num) from ${hivevar:hv_database}.${hivevar:hv_table}_rownum \n              GROUP BY ";
	private static final String DROP_TEMP_TBL = "DROP TABLE IF EXISTS ${hivevar:hv_database}.${hivevar:hv_table}_temp;";
	private boolean misMatchWithBase_Eq = Boolean.FALSE;
	private boolean misMatchWithBase_Ge = Boolean.FALSE;
	private Map<String, String> baseToInsertUnMatchColMap = new LinkedHashMap<String, String>();
	private Map<String, String> baseToInsertFinalMap = new LinkedHashMap<String, String>();

	boolean isCobolLayout = false;
	boolean isSummrization = false;
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
		tdHiveDataTypeMapping.put("DATE", "DATE"); // formatting is needed asper
													// needs
		tdHiveDataTypeMapping.put("TIME", "STRING");
		tdHiveDataTypeMapping.put("TIMESTAMP", "TIMESTAMP");
	}
	private Map<Integer, String> columnNameMap = new LinkedHashMap<Integer, String>();
	String fileName = "";

	/**
	 * please pass <db_name> <table_name> & any one: <upsert|insert|update|delete > isPartitioned\n ex: wm_sales store_info no
	 * 
	 * @param args
	 */
	public static void main(String[] args) {
		DFBIncremental_Upsert inc = new DFBIncremental_Upsert();
		inc.init(inc,args);
	}

	public void init(DFBIncremental_Upsert inc , String[] args) {
		if (args.length != 4) {
			System.err
					.println("Argument mismatched!!please pass <db_name> <table_name> & isCobolLaoyt\n ex: wm_sales store_info upsert no");
		} else {
			MySQLToHiveDDLConvertor ddlHQLCreator = new MySQLToHiveDDLConvertor();
			
			try {
				String dbName = args[0];
				String tblName = args[1];
				String isCobolLayout = args[2];
				String isSummarization = args[3];
				inc.fileName = dbName + "-" + tblName;
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
				inc.createStageTable();

				inc.getLoadFileToBaseTbleMapping();
				inc.getWhereConditionDetails();
				inc.createTempTbl();
				
				inc.writeToFile(dbName, tblName,inc.isSummrization);
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			} catch (Exception e) {
				e.printStackTrace();
			}
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

	/**
	 * 
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

	public void getWhereConditionDetails() {
		BufferedReader br = null;
		try {
			String sCurrentLine;
			br = new BufferedReader(new FileReader(
					"C:\\work\\incremental\\metadata\\" + sprint + "\\"
							+ fileName + "-ddl.txt"));
			String sField = "";
			while ((sCurrentLine = br.readLine()) != null) {
				sCurrentLine = sCurrentLine.toLowerCase();
				if (!"".equals(sCurrentLine)
						&& ((sCurrentLine.startsWith("primary index")) || sCurrentLine
								.startsWith("unique primary index"))) {
					primIndexColumns = sCurrentLine.substring(sCurrentLine
							.indexOf("(") + 1, sCurrentLine.indexOf(")"));
				}
			}
			StringBuffer sbWhereClause = new StringBuffer();
			if (null != primIndexColumns && !"".equals(primIndexColumns)) {
				String[] primCol = primIndexColumns.split(",");

				for (String column : primCol) {
					sbWhereClause.append("m." + column.trim() + "= s."
							+ column.trim() + " AND ");
				}
				whereClause = sbWhereClause.substring(0,
						sbWhereClause.toString().length() - 4).trim();
				if (!"".equals(whereClause)
						&& whereClause.split("AND").length > 0) {
					inClauseCol = primCol[0].trim();
				}
				if(whereClause.contains("wm_yr_wk")){
					inClauseCol= "wm_yr_wk";
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
			String nwWhereClz = null;
			br1 = new BufferedReader(new FileReader(
					"C:\\work\\incremental\\metadata\\" + sprint + "\\"
							+ fileName + "-update.txt"));
			StringBuffer fullCommand = new StringBuffer("");
			StringBuffer newWhereClause = new StringBuffer("");
			while ((sCurrentLine = br1.readLine()) != null) {
				sCurrentLine = sCurrentLine.toLowerCase();
				fullCommand.append(sCurrentLine);
			}
			String updateSql = fullCommand.toString();
			updateSql = updateSql.substring(updateSql.indexOf("where") + 5,
					updateSql.indexOf(";"));
			// System.err.println("updateSql" +updateSql);
			String[] split = updateSql.trim().split("and");
			String colValue1 = "";
			String colValue2 = "";
			for (String string : split) {
				// System.out.println(string.trim());
				String[] split2 = string.trim().split("=");
				String tmpSplt = split2[1].trim().replaceAll(":", "").trim()
						.replaceAll("date,format", "").replaceAll(
								" 'yyyymmdd'", "").replaceAll("\\(\\)", "")
						.trim().replaceAll("\\(.+?\\)", "").trim();

				colValue1 = replaceItemNbr(split2[0]);
				if ("ITEM_NBR".toLowerCase().equals(
						split2[0].trim().toLowerCase()))
					colValue1 = "mds_fam_id";
				else
					colValue1 = split2[0].trim().toLowerCase();
				colValue1 = replaceItemNbr(stg_base_col_map.get(tmpSplt));

				newWhereClause.append("m." + colValue1 + " = s." + colValue1
						+ " AND ");
				whereConditionColmns.append(colValue1 + ",");
				nwWhereClz = newWhereClause.toString().substring(0,
						newWhereClause.toString().length() - 4);

			}
			whereConditionColmns = new StringBuffer(whereConditionColmns
					.toString().substring(0,
							whereConditionColmns.toString().length() - 1));
			// System.err.println("whereConditionColmns: " +
			// whereConditionColmns);
			// System.out.println("nwWhereClz: " + nwWhereClz.toString());
			/*
			 * String[] primCol = primIndexColumns.split(",");
			 * 
			 * for (String column : primCol) {
			 * sbWhereClause.append("m."+column.trim() +
			 * "= s."+column.trim()+" AND "); } whereClause =
			 * sbWhereClause.substring
			 * (0,sbWhereClause.toString().length()-4).trim();
			 * if(!"".equals(whereClause) && whereClause.split("AND").length>0){
			 * inClauseCol = primCol[0].trim(); }
			 */

			whereClause = nwWhereClz;
			
			
			if(isSummrization){
				for (String key : updateColumnsMap.keySet()) {
					String str = updateColumnsMap.get(key).trim();
					str = "a."+str;
					str = str.replaceAll(":", "m.");
					baseToInsertFinalMap.put(key,str);
				}
				String[] split2 = whereConditionColmns.toString().split(",");
				for (String string : split2) {
					String str = baseToInsertFinalMap.get(string).trim();
					str = "m."+str;
					baseToInsertFinalMap.put(string,str);
				}
			}
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
		StringBuffer sb = new StringBuffer();
		StringBuffer sbCreateTempTble = new StringBuffer(
				"CREATE TABLE ${hivevar:hv_database}.${hivevar:hv_table}_temp AS");
		sbCreateTempTble.append("\n");
		sbCreateTempTble
				.append("SELECT m.* FROM ${hivevar:hv_database}.${hivevar:hv_table} m");
		sbCreateTempTble.append("\n");
		sbCreateTempTble.append("WHERE m." + inClauseCol.trim() + " NOT IN ( ");
		for (int i = 0; i < 28; i++) {
			sb.append(" ");
		}
		// sbCreateTempTble.append(sb.toString());
		sbCreateTempTble.append("SELECT DISTINCT s." + inClauseCol.trim());
		sbCreateTempTble.append("\n");
		sbCreateTempTble.append(sb.toString());
		sbCreateTempTble
				.append("FROM ${hivevar:hv_database}.${hivevar:hv_table}_nodups s");
		sbCreateTempTble.append("\n");
		sbCreateTempTble.append(sb.toString());
		sbCreateTempTble.append("WHERE "
				+ whereClause.trim().replaceAll("date,format", "").replaceAll(
						" 'yyyymmdd'", "").replaceAll("\\(\\)", "") + " );");
		sCreateTempTble = sbCreateTempTble.toString();
	}

	/**
	 * 
	 */
	public void getLoadFileToBaseTbleMapping() {

		BufferedReader br = null;
		try {

			String sCurrentLine;
			StringBuffer fullCommand = new StringBuffer();
			br = new BufferedReader(new FileReader(
					"C:\\work\\incremental\\metadata\\" + sprint + "\\"
							+ fileName + "-insert.txt"));
			while ((sCurrentLine = br.readLine()) != null) {
				sCurrentLine = sCurrentLine.toLowerCase();
				if (!"".equals(sCurrentLine)) {
					fullCommand.append(sCurrentLine);
				}
			}
			// System.out.println("fullCommand: " + fullCommand.toString());
			String baseTbleColmuns = fullCommand.toString().substring(
					fullCommand.toString().indexOf("(") + 1,
					fullCommand.toString().indexOf("values"));
			baseTbleColmuns = baseTbleColmuns.substring(0, baseTbleColmuns
					.indexOf(')'));
			// System.out.println("BASE TABLE: " +baseTbleColmuns);
			List<String> asList = new ArrayList<String>();
			for (String string : baseTbleColmuns.split(",")) {
				baseTableInsertColumns.add(string.trim());
			}

			String stgTbleColumns = fullCommand.toString().substring(
					fullCommand.toString().indexOf("values") + 6,
					fullCommand.toString().indexOf(";"));
			stgTbleColumns = stgTbleColumns.substring(stgTbleColumns
					.indexOf("(") + 1, stgTbleColumns.lastIndexOf(")") + 1);
			stgTbleColumns = stgTbleColumns.replaceAll(":", "");

			String re = "\\([^()]*\\)";
			Pattern p = Pattern.compile(re);
			Matcher m = p.matcher(stgTbleColumns);
			while (m.find()) {
				stgTbleColumns = m.replaceAll("");
				m = p.matcher(stgTbleColumns);
			}
			// System.out.println(stgTbleColumns);

			stgTbleColumns = stgTbleColumns.replaceAll("date,format", "");
			stgTbleColumns = stgTbleColumns.replaceAll("'yyyymmdd'", "");
			stgTbleColumns = stgTbleColumns.replaceAll("'yymmdd'", "");
			stgTbleColumns = stgTbleColumns.replaceAll("integer, format", "");
			stgTbleColumns = stgTbleColumns.replaceAll("integer", "");
			stgTbleColumns = stgTbleColumns.replaceAll(
					"DATE, FORMAT 'YYYY-MM-DD'".toLowerCase(), "");
			stgTbleColumns = stgTbleColumns
					.replaceAll(", format 'yyyyddd'", "");
			stgTbleColumns = stgTbleColumns.replaceAll("[()]", "");
			// format 'yyyy-mm-dd'
			// System.out.println("STG TABLE: " +stgTbleColumns);

			System.out.println("Staging table Count     : "
					+ stgTbleColumns.split(",").length);
			System.out.println("Base table Count(insert): "
					+ baseTbleColmuns.split(",").length);
			System.out.println("Base table cnt (DDL): "
					+ columnNameMap.size());

			
			List<String> l = new ArrayList<String>();
			boolean bool = false;
			String sNotMatching = "";
			
			for (int i = 0; i < stgTbleColumns.split(",").length; i++) {
				baseTableValuesCol.add(stgTbleColumns.split(",")[i].trim());
			}
			
			for (int i = 0; i < baseTableValuesCol.size(); i++) {
				baseToInsertColMap.put(replaceItemNbr(baseTableInsertColumns.get(i)), replaceItemNbr(baseTableValuesCol.get(i)));
			}
			
						
			if(columnNameMap.size() == baseToInsertColMap.size()){
				for (String str : baseToInsertColMap.keySet()) {
					if(!baseTableInsertColumns.contains(str)){
						misMatchWithBase_Eq = Boolean.TRUE;
					}
				}
					baseToInsertFinalMap = baseToInsertColMap; 
			}
			// Insert happening only on fewer columns than on all columns
			else if(columnNameMap.size() > baseToInsertColMap.size()){
				misMatchWithBase_Ge = Boolean.TRUE;
				for (String str : columnNameMap.values()) {
					str = str.toLowerCase();
					if(!baseToInsertColMap.containsKey(str)){
						baseToInsertUnMatchColMap.put(str, str);
						baseToInsertColMap.put(str, str);
					}
				}
				for (String unmatchedCol : baseToInsertUnMatchColMap.keySet()) {
					baseToInsertFinalMap = baseToInsertColMap;
					baseToInsertFinalMap.put(unmatchedCol, "m."+unmatchedCol);
				}
			}
			
			
			
			if (stgTbleColumns.split(",").length < baseTableInsertColumns.size()) {

				int cnt = 0;
				for (String string : baseTableInsertColumns) {
					// System.out.println(" <"+string+">"
					// +" <"+ll.get(cnt)+">");
					System.out.println("cnt " + cnt);
					if (!baseTableValuesCol.contains(string)) {
						bool = true;
						sNotMatching = string;
						l.add(sNotMatching);
						// map.put(sNotMatching,
						// baseTbleColmuns.split(",")[cnt]);
						updatedBaseTableColumns.add(sNotMatching);
					} else {
						updatedBaseTableColumns.add(baseTableInsertColumns.get(cnt));
					}
					cnt++;
				}
				

			}

			if (stgTbleColumns.split(",").length == baseTbleColmuns.split(",").length) {

				for (int i = 0; i < stgTbleColumns.split(",").length; i++) {
					stg_base_col_map.put(baseTbleColmuns.split(",")[i].trim(),
							stgTbleColumns.split(",")[i].trim());
				}
			} else if (stgTbleColumns.split(",").length < baseTbleColmuns
					.split(",").length) {
				for (int i = 0; i < stgTbleColumns.split(",").length; i++) {
					for (String string : baseTableInsertColumns) {
						System.out.println(stgTbleColumns.split(",")[i].trim());
						if (string.equals(stgTbleColumns.split(",")[i].trim())) {
							System.out.print(" --> matching");
						} else
							System.out.print(" --> Nope, not matching");
					}

					stg_base_col_map.put(baseTbleColmuns.split(",")[i].trim(),
							stgTbleColumns.split(",")[i].trim());
				}
			} else if (stgTbleColumns.split(",").length > baseTbleColmuns
					.split(",").length) {
				for (int i = 0; i < baseTbleColmuns.split(",").length; i++) {
					stg_base_col_map.put(baseTbleColmuns.split(",")[i].trim(),
							stgTbleColumns.split(",")[i].trim());
				}
			}

			if (stgTbleColumns.split(",").length == baseTbleColmuns.split(",").length) {
				IMP_MESSAGE = "Columns count is not matching with Base and Stage table, please check if columns are hard-coded!!!";
			}

			// Prepare insert into Temp step 1
			StringBuffer sbInsTemp = new StringBuffer(
					"INSERT INTO TABLE ${hivevar:hv_database}.${hivevar:hv_table}_temp \n SELECT \n");
			Set<Entry<String, String>> entrySet = stg_base_col_map.entrySet();
			for (Entry<String, String> string : entrySet) {
				String column = string.getValue().toLowerCase();
				String temp = "";
				if (string.getValue().indexOf("||") > 0) {
					String[] split = string.getValue().split("\\|\\|");
					temp = "CONCAT(";
					for (String string2 : split) {

						temp += string2 + ",";
					}
					temp = temp.substring(0, temp.length() - 1) + ")";
					sbInsTemp.append("       " + temp + ",\n");
				} else {
					sbInsTemp.append("       "
							+ replaceItemNbr(string.getValue()) + ",\n");
				}
			}
			String tmpIns = sbInsTemp.toString();
			tmpIns = tmpIns.substring(0, tmpIns.length() - 2).replaceAll(
					"'yyyyddd'", "").replaceAll("'yymmdd'", "");
			sbInsTemp = new StringBuffer(tmpIns);
			sbInsTemp
					.append("\nFROM ${hivevar:hv_database}.${hivevar:hv_table}_nodups;");
			// System.out.println("sbInsTemp: " +sbInsTemp);
			finalInsTemp = sbInsTemp.toString();

			// Prepare insert into Temp step 2
			StringBuffer sbInsTemp1 = new StringBuffer(
					"INSERT OVERWRITE TABLE ${hivevar:hv_database}.${hivevar:hv_table}\n"
							+ "SELECT ");
			for (Entry<String, String> string : entrySet) {
				sbInsTemp1.append("  " + string.getKey().toLowerCase().trim()
						+ ",\n");
			}
			sbInsTemp1 = new StringBuffer(sbInsTemp1.toString().substring(0,
					sbInsTemp1.toString().length() - 2));
			sbInsTemp1
					.append("\nFROM ${hivevar:hv_database}.${hivevar:hv_table}_temp;");
			// System.out.println("sbInsTemp1: " +sbInsTemp1);
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

	private String replaceItemNbr(String string) {
		String colValue = string;
		if ("ITEM_NBR".toLowerCase().equals(string.trim().toLowerCase()))
			colValue = "mds_fam_id";

		return colValue;
	}

	public static void putHQLHeader(BufferedWriter bw) throws Exception {
		bw
				.write("-- #*************************************************************************#\n");
		bw
				.write("-- # TITLE: <HQL_NAME>.hql                                          #\n");

		bw
				.write("-- # DESCRIPTION: The job loads the records for <TABLE_NAME>          #\n");
		bw
				.write("-- #=========================================================================#\n");
		bw
				.write("-- # REVISION HISTORY                                                        #\n");
		bw
				.write("-- #-------------------------------------------------------------------------#\n");
		bw
				.write("-- #2017-02-15 :: v1.0  :: <USER_ID>     :: Created  <DATE>           #\n");
		bw
				.write("-- #=========================================================================#\n");
		bw.write("SET hive.exec.compress.output=true ;\n");
		bw.write("SET hive.exec.compress.intermediate=true ;\n");
		bw.write("SET mapred.output.compress=true ;\n");
		bw
				.write("SET mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec ;\n");
		bw
				.write("SET mapred.map.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec ;\n");
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
		bw.write("SET hive.warehouse.subdir.inherit.perms=true;\n");
		bw
				.write("\n--Added the below parameter to enable tez execution engine\n");
		bw.write("SET hive.execution.engine=tez;\n");
		bw.write("SET tez.runtime.io.sort.mb=1600;\n\n");

	}

	public void writeToFile(String dbName, String tblName, boolean isSummrization) {
		String hiveDDLLoc = "C:\\work\\incremental\\generated\\" + sprint
				+ "\\" +tblName.toLowerCase() + "_upsert.hql";
		try {
			BufferedWriter bw = new BufferedWriter(new FileWriter(hiveDDLLoc));
			putHQLHeader(bw);
			// bw.write(this.IMP_MESSAGE);
			bw.write("--Creating Database if not exists\n");
			bw.write("CREATE DATABASE IF NOT EXISTS ${hivevar:hv_database};\n\n--Logging into the database before executing the below HQLs\nUSE ${hivevar:hv_database};\n\n");
			bw.write("" + baseTableDDL
							+ "\n--drop the Stage table if exists\n");
			bw.write(DROP_S_TBL
							+ "\n\n--creating the stage table if not exists to store raw data\n");
			bw.write(this.finalStgTbl.toString()
					+ "\n\n--Dropping rownum table if present\n");
			bw.write(DROP_ROWNUM_TBL
					+ "\n\n--Create rownum table  select from stage table\n"
					+ CREATE_ROWNUM_TBL);
			bw.write("\n\n--Drop nodups table if present\n");
			bw.write(DROP_NODUPS_TBL + "\n\n");
			bw.write(CREATE_NODUPS_TBL + whereConditionColmns.toString()
					+ ");\n\n");
			
			if(isSummrization){
				bw.write("--Dropping temp tables if already exists\n");
				bw.write("DROP TABLE IF EXISTS ${hivevar:hv_database}.${hivevar:hv_table}_temp1;\n");
				bw.write("DROP TABLE IF EXISTS ${hivevar:hv_database}.${hivevar:hv_table}_temp2;\n");
				bw.write("DROP TABLE IF EXISTS ${hivevar:hv_database}.${hivevar:hv_table}_temp3;\n\n");
				
				bw.write("--Insert the records into temp table to store unaffected rows from base table\n");
				bw.write("CREATE TABLE ${hivevar:hv_database}.${hivevar:hv_table}_temp1 AS\n");
				bw.write("SELECT m.*\nFROM ${hivevar:hv_database}.${hivevar:hv_table} m\nWHERE m."+inClauseCol);
				bw.write(" NOT IN (SELECT s."+inClauseCol);
				bw.write(" FROM ${hivevar:hv_database}.${hivevar:hv_table}_nodups s\n");
				bw.write("                         WHERE " +whereClause.replaceAll("AND", "AND\n                         ") +");\n\n");
				bw.write("--Creating temp table to store 'to be inserted' rows from nodups table\n");
				bw.write("CREATE TABLE ${hivevar:hv_database}.${hivevar:hv_table}_temp2\nAS\nSELECT m.*\n");
				bw.write("FROM ${hivevar:hv_database}.${hivevar:hv_table}_nodups m\nWHERE m."+inClauseCol);
				bw.write(" NOT IN (SELECT s."+inClauseCol);
				bw.write(" FROM ${hivevar:hv_database}.${hivevar:hv_table} s\n");
				bw.write("                         WHERE " +whereClause.replaceAll("AND", "AND\n                         ") +");\n\n");
				bw.write("--Creating temp table to store 'to be updated' rows from nodups table\n");
				bw.write("CREATE TABLE ${hivevar:hv_database}.${hivevar:hv_table}_temp3\nAS\nSELECT\n");
				StringBuffer sb = new StringBuffer();
				for(String eachCol : baseToInsertFinalMap.values()){
					sb.append(eachCol+",\n       ");
				}
				bw.write(sb.toString().substring(0,sb.toString().length() - 9));
				bw.write("\nFROM\n${hivevar:hv_database}.${hivevar:hv_table}_nodups m\n");
				bw.write("${hivevar:hv_database}.${hivevar:hv_table} a\nON\n");
				bw.write(whereClause);
				bw.write("\nWHERE m."+inClauseCol +" IN (SELECT s."+inClauseCol);
				bw.write("FROM ${hivevar:hv_database}.${hivevar:hv_table} s\n");
				bw.write("                         WHERE "+whereClause.replaceAll("AND", "AND\n                         ") +");\n\n");
				bw.write("--Inserting data into the first temp table from the second temp table\n");
				bw.write("INSERT INTO TABLE ${hivevar:hv_database}.${hivevar:hv_table}_temp1\n");
				bw.write("SELECT *\n--Inserting data into the first temp table from the third temp table");
				bw.write("FROM ${hivevar:hv_database}.${hivevar:hv_table}_temp2;\n\n");
				bw.write("--Inserting data into the first temp table from the third temp table\n");
				bw.write("INSERT INTO TABLE ${hivevar:hv_database}.${hivevar:hv_table}_temp1\n");
				bw.write("SELECT *\n");
				bw.write("FROM ${hivevar:hv_database}.${hivevar:hv_table}_temp3;\n\n");
				bw.write("--Drop the base table if already exists\n");
				bw.write("DROP TABLE IF EXISTS ${hivevar:hv_database}.${hivevar:hv_table};\n\n");
				bw.write("--Rename the Temp table with base table\n");
				bw.write("ALTER TABLE ${hivevar:hv_database}.${hivevar:hv_table}_temp1 RENAME TO ${hivevar:hv_database}.${hivevar:hv_table};\n\n");
				bw.write("--Drop the temporary and external stage tables\n");
				bw.write("DROP TABLE ${hivevar:hv_database}.${hivevar:hv_table}_stg;\n");
				bw.write("DROP TABLE ${hivevar:hv_database}.${hivevar:hv_table}_nodups;\n");
				bw.write("DROP TABLE ${hivevar:hv_database}.${hivevar:hv_table}_temp2;\n");
				bw.write("DROP TABLE ${hivevar:hv_database}.${hivevar:hv_table}_temp3;\n");
				bw.write("");
				}
			else {
				bw.write("--Dropping table if present\n");
				bw.write(DROP_TEMP_TBL + "\n\n");
				bw.write(this.sCreateTempTble
						+ "\n\n--Insert into temp table from stage table\n");
				bw.write(this.finalInsTemp + "\n\n");
				bw.write("-- Drop base table if exists\n");
				bw.write(DROP_FINAL_TABLE + "\n\n");
				bw.write("-- Create empty table similar to base table (ORC table)\n");
				bw.write(CREATE_FINAL_TABLE + "\n\n");
				bw.write("--Inserting data from the stage table into the base table\n");
				bw.write("INSERT INTO TABLE ${hivevar:hv_database}.${hivevar:hv_table}_final\n");
				bw.write("SELECT ");
				StringBuffer sb = new StringBuffer();
				for(String eachCol : baseToInsertFinalMap.values()){
					if(!eachCol.contains("m.")){
						sb.append("t."+eachCol+",\n       ");
					}
					else {
						sb.append(eachCol+",\n       ");
					}
				}
				bw.write(sb.toString().substring(0,sb.toString().length() - 9));
				if(misMatchWithBase_Ge){
					bw.write("\n FROM ${hivevar:hv_database}.${hivevar:hv_table}_temp t, ${hivevar:hv_database}.${hivevar:hv_table} m\n");
					bw.write( " WHERE "+whereClause+";\n\n");
				} else {
					bw.write("\n FROM ${hivevar:hv_database}.${hivevar:hv_table}_temp t; \n\n");
				}
					bw.write("-- Drop base table if exists\n");
				bw.write(DROP_BASE_TABLE + "\n\n");
				bw.write("--Renaming the FINAL temporary table(ORC) into base table\n");
				bw.write(ALTER_FIN_TO_BASE);
				bw.write("\n\n--Drop intermediate tables created\n");
				bw.write("DROP TABLE ${hivevar:hv_database}.${hivevar:hv_table}_temp;\n");
				bw.write("DROP TABLE ${hivevar:hv_database}.${hivevar:hv_table}_stg;\n");
				bw.write("DROP TABLE ${hivevar:hv_database}.${hivevar:hv_table}_nodups;\n");
			}
			bw.close();
		} catch (IOException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
