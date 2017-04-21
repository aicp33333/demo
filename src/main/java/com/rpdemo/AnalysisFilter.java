/**
 * 
 */
package com.rpdemo;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Iterator;

import net.sf.json.JSONObject;



/**
 * @author rongpei
 *
 */
public class AnalysisFilter {
	 public static String mkString(Iterator it,String str){
		  StringBuffer sb =new StringBuffer();
		  while (it.hasNext()) {
			 sb.append(it.next()+str);
		 }
		  String tmp=sb.toString() ;
		  return  tmp.substring(0, tmp.length()-str.length());
	  }

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		File file = new File("/Users/rongpei/54.169.233.67-notice-ad_server_empty.log.2017-03-12-00");  
        BufferedReader reader = null; 
		try {
			reader = new BufferedReader(new FileReader(file));  
            String tempString = null; 
//            while ((tempString = reader.readLine()) != null) {  
//            	  tempString = reader.readLine();
//                  String[] strs = tempString.split("\t");
//                  String jsonstr = strs[2].split("-params:")[1];
//                  JSONObject js= JSONObject.fromObject(jsonstr);
//                  System.out.println(js.get("googleAdvertisingId"));
//                  if("de49ebde-5d0a-4454-ad53-fb6ba2d47db0".equals(js.get("googleAdvertisingId"))||
//                  		"3278b6f8-09ea-4a27-aca8-b4bafa798f12".equals(js.get("googleAdvertisingId"))){
//                  	System.out.println(tempString);
//                  } 
//            }  
            tempString = reader.readLine();
            String[] strs = tempString.split("\t");
            String jsonstr = strs[2].split("-params:")[1];
            JSONObject js= JSONObject.fromObject(jsonstr);
            System.out.println(mkString(js.keys(),"\\x01"));
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}    

	}

}
