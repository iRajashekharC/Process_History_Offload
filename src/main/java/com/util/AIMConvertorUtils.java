package com.util;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

public class AIMConvertorUtils {

	public static void getAllFiles(File dir, List<File> fileList) {
		try {
			File[] files = dir.listFiles();
			for (File file : files) {
				fileList.add(file);
				if (file.isDirectory()) {
					System.out.println("directory:" + file.getCanonicalPath());
					getAllFiles(file, fileList);
				} else {
					System.out.println("     file:" + file.getCanonicalPath());
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void writeZipFile(File directoryToZip, List<File> fileList) {

		try {
			FileOutputStream fos = new FileOutputStream(directoryToZip+"/"+directoryToZip.getName() + ".zip");
			ZipOutputStream zos = new ZipOutputStream(fos);

			for (File file : fileList) {
				if (!file.isDirectory()) { // we only zip files, not directories
					addToZip(directoryToZip, file, zos);
				}
			}

			zos.close();
			fos.close();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void addToZip(File directoryToZip, File file, ZipOutputStream zos) throws FileNotFoundException,
			IOException {

		FileInputStream fis = new FileInputStream(file);

		// we want the zipEntry's path to be a relative path that is relative
		// to the directory being zipped, so chop off the rest of the path
		String zipFilePath = file.getCanonicalPath().substring(directoryToZip.getCanonicalPath().length() + 1,
				file.getCanonicalPath().length());
		System.out.println("Writing '" + zipFilePath + "' to zip file");
		ZipEntry zipEntry = new ZipEntry(zipFilePath);
		zos.putNextEntry(zipEntry);

		byte[] bytes = new byte[1024];
		int length;
		while ((length = fis.read(bytes)) >= 0) {
			zos.write(bytes, 0, length);
		}

		zos.closeEntry();
		fis.close();
	}
	

    public static void delete(File tempDir) throws Exception
	{
		if (tempDir.isDirectory()) {
			System.err.println("THis is directory: " + tempDir.getPath());
			// directory is empty, then delete it
			if (tempDir.list().length == 0) {
				tempDir.delete();
				System.out.println("Directory is deleted : " + tempDir.getAbsolutePath());

			} else {
				System.err.println("There are some files inside the directory" );
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
