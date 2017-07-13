package com;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.ResponseBuilder;
import javax.ws.rs.core.UriInfo;

import org.apache.commons.lang3.StringUtils;

import com.service.DFBIncrHQLGenerator;
import com.service.MySQLToHiveDDLConvertor;
import com.service.TDToHiveConvertor;
import com.sun.jersey.core.header.FormDataContentDisposition;
import com.sun.jersey.multipart.BodyPartEntity;
import com.sun.jersey.multipart.FormDataBodyPart;
import com.sun.jersey.multipart.FormDataMultiPart;
import com.sun.jersey.multipart.FormDataParam;
import com.util.AIMConvertorUtils;

/**
 * This example shows how to build Java REST web-service to upload files
 * accepting POST requests with encoding type "multipart/form-data". For more
 * details please read the full tutorial on
 * https://javatutorial.net/java-file-upload-rest-service
 * 
 * @author javatutorial.net
 */
@Path("/convertor")
public class AIMConvertor {

	/** The path to the folder where we want to store the uploaded files */
	private static final String UPLOAD_FOLDER = "/Users/raj/Documents/WebEx/";
	private static  Object file = null;

	public AIMConvertor() {
	}

	@Context
	private UriInfo context;

	/**
	 * Returns text response to caller containing current time-stamp
	 * 
	 * @return error response in case of missing parameters an internal
	 *         exception or success response if file has been stored
	 *         successfully
	 * @throws IOException 
	 */
	@Path("/history")
	@POST
	@Produces("application/zip")
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	public Response generateHistoryHQL(@FormDataParam("file") InputStream uploadedInputStream,
			@FormDataParam("file") FormDataContentDisposition fileDetail, @FormDataParam("serverName") String serverName, @FormDataParam("dbUserName") String dbUserName, 
			@FormDataParam("dbPassword") String dbPassword,@FormDataParam("sourceLoc") String sourceLoc, @FormDataParam("mappingDtls") String mappingDtls, 
			@FormDataParam("dbType") String dbType, @FormDataParam("tablesList") String tablesList, final FormDataMultiPart multiPart) throws IOException {
		String tmpDir = System.getProperty("java.io.tmpdir")+"tempDDL/"; 
		Map<String,String> inParams = new HashMap<String, String>();
		MySQLToHiveDDLConvertor mySQL = new MySQLToHiveDDLConvertor();
		TDToHiveConvertor td = new TDToHiveConvertor();
		System.out.println("tmpDir: "+tmpDir);
		try {
				AIMConvertorUtils.delete((File)new File(tmpDir));
				 new File(tmpDir).mkdir();
			} catch (Exception e) {
				e.printStackTrace();
			}
		inParams.put("serverName", serverName);
		inParams.put("dbPassword",dbPassword);
		inParams.put("sourceLoc",sourceLoc);
		inParams.put("mappingDtls",mappingDtls);
		inParams.put("dbUserName",dbUserName);
		inParams.put("dbType",dbType);
		inParams.put("tablesList",tablesList);
		inParams.put("tmpDir", tmpDir);
		inParams.put("skipJDBCConnection", "false");
		
		if( null != sourceLoc && "file".equalsIgnoreCase(sourceLoc)){
			List<FormDataBodyPart> bodyParts = multiPart.getFields("files");
			
			StringBuffer fileDetails = new StringBuffer("");
			String uploadedFileLocation = "";
			/* Save multiple files */
			for (int i = 0; i < bodyParts.size(); i++) {
				BodyPartEntity bodyPartEntity = (BodyPartEntity) bodyParts.get(i).getEntity();
				String fileName = bodyParts.get(i).getContentDisposition().getFileName();
				saveToFile(bodyPartEntity.getInputStream(), tmpDir + fileName);
				fileDetails.append(" File saved at /Volumes/Drive2/temp/file/" + fileName);
			}
			inParams.put("skipJDBCConnection", "true");
		}
			if(null != dbType && "mysql".equalsIgnoreCase(dbType)){
				mySQL.init(inParams);
			}
			else if(null != dbType && "TD".equalsIgnoreCase(dbType)){
				td.init(inParams);
			}
		
		file = new File(tmpDir+"/tempDDL.zip");
		ResponseBuilder responseBuilder = Response.ok((Object) file);
	        responseBuilder.header("Content-Disposition", "attachment; filename=\"HQL_DDL_Files.zip\"");
	       
	        return responseBuilder.build();
	}

	/**
	 * Utility method to save InputStream data to target location/file
	 * 
	 * @param inStream
	 *            - InputStream to be saved
	 * @param target
	 *            - full path to destination file
	 */
	private void saveToFile(InputStream inStream, String target) throws IOException {
		System.out.println("Files are being stored @:" + target);
		OutputStream out = null;
		int read = 0;
		byte[] bytes = new byte[1024];

		out = new FileOutputStream(new File(target));
		while ((read = inStream.read(bytes)) != -1) {
			//System.out.println(bytes.toString()+"---"+read);
			//System.out.println(new String(bytes));
			out.write(bytes, 0, read);
		}
		out.flush();
		out.close();
	}

	/**
	 * Returns text response to caller containing current time-stamp
	 * 
	 * @return error response in case of missing parameters an internal
	 *         exception or success response if file has been stored
	 *         successfully
	 * @throws IOException 
	 */
	@Path("/incremental")
	@POST
	@Produces("application/zip")
	@Consumes(MediaType.MULTIPART_FORM_DATA)
	public Response generateIncrementalHQL(
			@FormDataParam("i_insertLayoutFile") InputStream i_insertLayoutFile_Stream,	@FormDataParam("i_insertLayoutFile") FormDataContentDisposition i_insertLayoutFile_Detail, 
			@FormDataParam("i_insertSQLFile") InputStream i_insertSQLFile_Stream, @FormDataParam("i_insertSQLFile") FormDataContentDisposition i_insertSQLFile_Detail,
			@FormDataParam("i_insertDDLFile") InputStream i_insertDDLFile_Stream, @FormDataParam("i_insertDDLFile") FormDataContentDisposition i_insertDDLFile_Detail,
			@FormDataParam("i_insertDDLFile") InputStream i_UpdateSQLFile_Stream, @FormDataParam("i_insertDDLFile") FormDataContentDisposition i_updateSQLFile_Detail,
			@FormDataParam("patternRadio") String patternRadio, @FormDataParam("partition") String partition, 
			@FormDataParam("insertPatternRadio") String insertPatternRadio,@FormDataParam("sourceLoc") String sourceLoc, @FormDataParam("mappingDtls") String mappingDtls, 
			@FormDataParam("dbType") String dbType, @FormDataParam("tablesList") String tablesList, final FormDataMultiPart multiPart) throws IOException {
		String tmpDir = System.getProperty("java.io.tmpdir")+"tempIncrmenetal/"; 
		Map<String,String> inParams = new HashMap<String, String>();
		MySQLToHiveDDLConvertor mySQL = new MySQLToHiveDDLConvertor();
		TDToHiveConvertor td = new TDToHiveConvertor();
		System.out.println("tmpDir: "+tmpDir);
		try {
				AIMConvertorUtils.delete((File)new File(tmpDir));
				 new File(tmpDir).mkdir();
			} catch (Exception e) {
				e.printStackTrace();
			}
	
		 //insert arguments: db tabl Pattern layout type partition  ("" "" insert overwrite no)
		if(!StringUtils.isNotEmpty(patternRadio)){
			DFBIncrHQLGenerator incHqlGen = new DFBIncrHQLGenerator();
			String[] inArguments = new String[10];
			if("insertRadio".equalsIgnoreCase(patternRadio)){
				saveToFile(i_insertDDLFile_Stream, tmpDir+i_insertDDLFile_Detail.getFileName());
				saveToFile(i_insertSQLFile_Stream, tmpDir+i_insertSQLFile_Detail.getFileName());
				saveToFile(i_insertLayoutFile_Stream, tmpDir+i_insertLayoutFile_Detail.getFileName());
				if(!StringUtils.isNotEmpty(insertPatternRadio)){
					if("fullRefresh".equalsIgnoreCase(insertPatternRadio)){
						inArguments[0]="db_name";
						inArguments[1]="table_name";
						inArguments[0]="table_name";
						inArguments[0]="table_name";
						inArguments[0]="table_name";
						inArguments[0]="table_name";
						inArguments[0]="table_name";
						
					} else if("insertInto".equalsIgnoreCase(insertPatternRadio)){
						
					}
				}
				
			} else if("upsertRadio".equalsIgnoreCase(patternRadio)){
				saveToFile(i_insertDDLFile_Stream, tmpDir+i_insertLayoutFile_Detail.getFileName());
				saveToFile(i_insertSQLFile_Stream, tmpDir+i_insertSQLFile_Detail.getFileName());
				saveToFile(i_insertLayoutFile_Stream, tmpDir+i_insertLayoutFile_Detail.getFileName());
				saveToFile(i_UpdateSQLFile_Stream, tmpDir+i_updateSQLFile_Detail.getFileName());
			}
			incHqlGen.init(inArguments);
			
		}
		
		
		
		if( null != sourceLoc && "file".equalsIgnoreCase(sourceLoc)){
			List<FormDataBodyPart> bodyParts = multiPart.getFields("files");
			
			StringBuffer fileDetails = new StringBuffer("");
			String uploadedFileLocation = "";
			/* Save multiple files */
			for (int i = 0; i < bodyParts.size(); i++) {
				BodyPartEntity bodyPartEntity = (BodyPartEntity) bodyParts.get(i).getEntity();
				String fileName = bodyParts.get(i).getContentDisposition().getFileName();
				saveToFile(bodyPartEntity.getInputStream(), tmpDir + fileName);
				fileDetails.append(" File saved at /Volumes/Drive2/temp/file/" + fileName);
			}
			inParams.put("skipJDBCConnection", "true");
		}
			if(null != dbType && "mysql".equalsIgnoreCase(dbType)){
				mySQL.init(inParams);
			}
			else if(null != dbType && "TD".equalsIgnoreCase(dbType)){
				td.init(inParams);
			}
		
		file = new File(tmpDir+"/tempDDL.zip");
		ResponseBuilder responseBuilder = Response.ok((Object) file);
	        responseBuilder.header("Content-Disposition", "attachment; filename=\"HQL_DDL_Files.zip\"");
	       
	        return responseBuilder.build();
	}

}