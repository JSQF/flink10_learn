/*
 *
 * ************************************************************************************
 *
 *   Project:        Internet Car
 *
 *   Copyright ?     2014-2017 Banma Technologies Co.,Ltd
 *                   All rights reserved.
 *
 *   This software is supplied only under the terms of a license agreement,
 *   nondisclosure agreement or other written agreement with Banma Technologies
 *   Co.,Ltd. Use, redistribution or other disclosure of any parts of this
 *   software is prohibited except in accordance with the terms of such written
 *   agreement with Banma Technologies Co.,Ltd. This software is confidential
 *   and proprietary information of Banma Technologies Co.,Ltd.
 *
 * ************************************************************************************
 *
 *   Class Name:  FileUtils
 *
 *   General Description:
 *
 *   Revision History:
 *                            Modification
 *    Author                Date(MM/DD/YYYY)   JiraID           Description of Changes
 *    ---------------------   ------------    ----------     -----------------------------
 *    Bill Zhang                2017-12-06
 *
 * *************************************************************************************
 *
 */

package com.yyb.flink10.util1;

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;

import java.io.*;
import java.util.zip.GZIPOutputStream;


public class FileUtils {

	public FileUtils() {
		// TODO Auto-generated constructor stub
	}

	public static void writeToFile(String content, String path, boolean append) {
		BufferedWriter bw = null;
		try {
			bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(path, append), "UTF-8"));
			bw.write(content);

			bw.flush();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				bw.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	public static void byte2File(byte[] buf, String path, boolean append) {
		FileOutputStream fos = null;
		BufferedOutputStream bos = null;
		try {
			fos = new FileOutputStream(path, append);
			bos = new BufferedOutputStream(fos);
			bos.write(buf);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				bos.flush();
				bos.close();
				fos.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

	}

	public static byte[] compressGZbytes(byte[] data) {
		byte[] b = null;
		ByteArrayOutputStream bos = null;
		GZIPOutputStream gzip = null;
		try {
			bos = new ByteArrayOutputStream();
			gzip = new GZIPOutputStream(bos);
			gzip.write(data);
			gzip.finish();
			b = bos.toByteArray();
		} catch (Exception ex) {
			ex.printStackTrace();
		}finally{
			try {
				gzip.close();
				bos.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return b;
	}
	
	public static byte[] uncompressGz(byte[] buf) {
		byte[] uncompressedBytes = null;
		BufferedInputStream in = null;
		ByteArrayOutputStream out = null;
		GzipCompressorInputStream gzIn = null;
		try {
			in = new BufferedInputStream(new ByteArrayInputStream(buf));
			out = new ByteArrayOutputStream();
			gzIn = new GzipCompressorInputStream(in);
			final byte[] buffer = new byte[2048];
			int n = 0;
			while (-1 != (n = gzIn.read(buffer))) {
				out.write(buffer, 0, n);
			}
			uncompressedBytes = out.toByteArray();

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				in.close();
				out.close();
				gzIn.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return uncompressedBytes;
	}

	public static String readFile(String fileName) {
		StringBuilder sb = new StringBuilder();
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new InputStreamReader(new FileInputStream(fileName)));
			String line = null;
			while ((line = reader.readLine()) != null) {
				sb.append(line);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return sb.toString();
	}
	
	public static String readFileClassPath(String fileName){
		
		StringBuilder sb = new StringBuilder();
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new InputStreamReader(FileUtils.class.getClassLoader().getResourceAsStream(fileName)));
			String line = null;
			while ((line = reader.readLine()) != null) {
				sb.append(line);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				reader.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return sb.toString();
	}

	public static byte[] readBytes(String filename) {

		File file = new File(filename);
		long len = file.length();
		byte[] bytes = new byte[(int) len];

		try {
			BufferedInputStream bufferedInputStream = new BufferedInputStream(new FileInputStream(file));
			bufferedInputStream.read(bytes);
			bufferedInputStream.close();
		} catch (Exception e) {
			e.printStackTrace();
		}

		return bytes;
	}

	public static byte[] readBytes(BufferedInputStream inputStream) {

		StringBuilder sb = new StringBuilder();
		try {
			byte[] bytes = new byte[1024];
			int byteRead = 0;

			while ((byteRead = inputStream.read(bytes)) != -1) {
				sb.append(new String(bytes, 0, byteRead));
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				inputStream.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return sb.toString().getBytes();
	}

/*	public static String getChannelPath(){
		 String prefix = AppConfig.getVal(Constants.HDFS_SPARK_TBOX_CHANNEL_PATH);
		 String channelPath=prefix+"/"+DateUtils.getDateTimeWith10mins();
		 return channelPath;
	}

	public static String getEventPath(){
	    String eventPrefix = AppConfig.getVal(Constants.HDFS_SPARK_TBOX_EVENT_PATH);
	    String eventPath=eventPrefix+"/"+DateUtils.getDateTimeWith10mins();
		 return eventPath;
	}

	public static String getJourneyPath(){
	    String eventPrefix = AppConfig.getVal(Constants.HDFS_SPARK_TBOX_JOURNEY_PATH);
	    String eventPath=eventPrefix+"/"+DateUtils.getDateTimeWith10mins();
		 return eventPath;
	}

	public static String getNgJourneyPath(){
	    String eventPrefix = AppConfig.getVal(Constants.HDFS_SPARK_NG_TBOX_JOURNEY_PATH);
	    String eventPath=eventPrefix+"/"+DateUtils.getDateTimeWith10mins();
		 return eventPath;
	}

	public static String getNgChannelPath(){
		 String prefix = AppConfig.getVal(Constants.HDFS_SPARK_NG_TBOX_CHANNEL_PATH);
		 String channelPath=prefix+"/"+DateUtils.getDateTimeWith10mins();
		 return channelPath;
	}

	public static String getNgEventPath(){
	    String eventPrefix = AppConfig.getVal(Constants.HDFS_SPARK_NG_TBOX_EVENT_PATH);
	    String eventPath=eventPrefix+"/"+DateUtils.getDateTimeWith10mins();
		 return eventPath;
	}

	public static String getTboxPathRaw(){
		String prefix = AppConfig.getVal(Constants.HDFS_SPARK_TBOX_PATH);
		return prefix+"/"+DateUtils.getDateTimeWith10mins();
	}

	public static String getNgTboxPathRaw(){
		String prefix = AppConfig.getVal(Constants.HDFS_SPARK_TBOX_NG_PATH);
		return prefix+"/"+DateUtils.getDateTimeWith10mins();
	}*/

}
