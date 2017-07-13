package com.form;

import javax.ws.rs.PathParam;

import com.sun.jersey.multipart.FormDataMultiPart;

public class HistoryForm {
	
	@PathParam("serverName")
	String serverName;
	String dbUserName; 
	String dbPassword;
	String sourceLoc;  
	String mappingDtls; 
	String dbType; 
	String tablesList; 
	FormDataMultiPart multiPart;
	
	
	public String getServerName() {
		return serverName;
	}
	public void setServerName(String serverName) {
		this.serverName = serverName;
	}
	public String getDbUserName() {
		return dbUserName;
	}
	public void setDbUserName(String dbUserName) {
		this.dbUserName = dbUserName;
	}
	public String getDbPassword() {
		return dbPassword;
	}
	public void setDbPassword(String dbPassword) {
		this.dbPassword = dbPassword;
	}
	public String getSourceLoc() {
		return sourceLoc;
	}
	public void setSourceLoc(String sourceLoc) {
		this.sourceLoc = sourceLoc;
	}
	public String getMappingDtls() {
		return mappingDtls;
	}
	public void setMappingDtls(String mappingDtls) {
		this.mappingDtls = mappingDtls;
	}
	public String getDbType() {
		return dbType;
	}
	public void setDbType(String dbType) {
		this.dbType = dbType;
	}
	public String getTablesList() {
		return tablesList;
	}
	public void setTablesList(String tablesList) {
		this.tablesList = tablesList;
	}
	public FormDataMultiPart getMultiPart() {
		return multiPart;
	}
	public void setMultiPart(FormDataMultiPart multiPart) {
		this.multiPart = multiPart;
	}

}
