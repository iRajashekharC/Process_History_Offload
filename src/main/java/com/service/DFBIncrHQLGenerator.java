package com.service;
import org.apache.commons.lang3.StringUtils;

public class DFBIncrHQLGenerator {
	public static void main(String[] args) {
		
		new DFBIncrHQLGenerator().init(args);
	}

	public  void init(String[] args) {
		String dbName = "";
		String tblName =  "";
		String pattern =  "";
		String isCobolLayout =  "";
		String isSummarization =  "";
		String isPartitioned =  "";
		String insertPattern =  "";
		if(args.length <4){
			System.err.println("Number of argument should be 4 or more, but you have passed: "+args.length);
		}
		else
		{
			 dbName = args[0];
			 tblName = args[1];
			 pattern = args[2];
			 isCobolLayout = args[3];
			 if(!StringUtils.isEmpty(pattern)){
				 //upsert arguments: db tabl Pattern layout summ partition  (WC_Tables ITEM_WKLY_INV upsert yes no no)
				 //insert arguments: db tabl Pattern layout type partition  ()
				 if(StringUtils.equalsIgnoreCase("upsert", pattern)){
					 isSummarization = args[4];
					 isPartitioned =args[5];
					 if(!StringUtils.isEmpty(isPartitioned) ){
						 if(StringUtils.equalsIgnoreCase("yes", isPartitioned)){
							 
						 } 
						 else if(StringUtils.equalsIgnoreCase("no", isPartitioned)){
							 DFBIncremental_Upsert upsert = new DFBIncremental_Upsert();
							 String[] args1 = new String[4];
							 args1[0]=args[0];
							 args1[1]=args[1];
							 args1[2]=args[3];
							 args1[3]=args[4];
							 upsert.init(upsert, args1);
						 }
					 }
				 }
				 else if(StringUtils.equalsIgnoreCase("insert", pattern)){
					 isPartitioned =args[4];
					 if(!StringUtils.isEmpty(isPartitioned) ){
						 if(StringUtils.equalsIgnoreCase("yes", isPartitioned)){
							 
						 } 
						 else if(StringUtils.equalsIgnoreCase("no", isPartitioned)){
							 String[] args1 = new String[4];
							 DFBIncremental_InsertFromFile inc = new DFBIncremental_InsertFromFile();
							 args1[0]=args[0];
							 args1[1]=args[1];
							 args1[2]=args[2];
							 args1[3]=args[4];
							 inc.init(inc, args1);
						 }
					 }
				 }
			 }
		}
	}
}
