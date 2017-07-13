package com.util;

import java.io.File;

public class Main {

	public static void main(String[] args) {
		String tmpDir = System.getProperty("java.io.tmpdir")+"tempDDL"; 
		System.err.println(tmpDir);
		try {
			AIMConvertorUtils.delete(new File(tmpDir));
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
