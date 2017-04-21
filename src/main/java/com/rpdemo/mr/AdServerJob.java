/**
 * 
 */
package com.rpdemo.mr;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


import net.sf.json.JSONObject;

/**
 * @author rongpei
 *
 */
public class AdServerJob extends Configured implements Tool{

	public static class AdServerMap extends Mapper<LongWritable, Text, NullWritable, Text> {
		   
		 public static String mkString(Iterator it,String str){
			  StringBuffer sb =new StringBuffer();
			  while (it.hasNext()) {
				 sb.append(it.next()+str);
			 }
			  String tmp=sb.toString() ;
			  return  tmp.substring(0, tmp.length()-str.length());
		  }
		  @Override
			public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			  String str = value.toString();
	          String[] strs = str.split("\t",-1);
	          String jsonstr = strs[2];
              JSONObject js= JSONObject.fromObject(jsonstr);
              //if request_body['googleAdvertisingId'] == "" and request_body['idfa'] == None :
              //python 中idfa值为null 读出来None
              if(!"".equals(js.get("googleAdvertisingId"))||!"null".equals(js.get("idfa"))){
            	String  campaign =null;
            	String pack = null;
            	  if(!"null".equals(js.get("installIdSet")))
            		 campaign =mkString(JSONObject.fromObject(js.get("installIdSet").toString()).keys(),"\\x01");
            	  if(!"null".equals(js.get("includePackageNameSet")))
            		  pack =mkString(JSONObject.fromObject(js.get("includePackageNameSet").toString()).keys(),"\\x01");
            	  if(campaign!=null||pack!=null){
            		  String tmp =js.get("googleAdvertisingId")+"\t"+js.get("idfa")+"\t"+js.get("appId")
            		  +"\t"+js.get("scenario")+"\t"+campaign+"\t"+pack;
            		  context.write(NullWritable.get(), new Text(tmp));
            	  }
            		  
            	  
              }
             
	          
		  }
	
	}
	public int run(String[] args) throws Exception {
		if(args.length < 2) {
		    return 1;
		}
	 
		String outputFile = args[1];
		 Path outputPath = new Path(outputFile);
	     FileSystem fileSystem = outputPath.getFileSystem(getConf());
	     if (fileSystem.exists(outputPath)) {
	         fileSystem.delete(outputPath, true);
	     }
	 
		    Job job = Job.getInstance(getConf(), "AdServerJob");
	        job.setJarByClass(getClass());
	        job.setJobName(getClass().getName());
	        job.setMapperClass(AdServerMap.class);
		    job.setNumReduceTasks(0);
		    
		    //job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			job.setOutputFormatClass(TextOutputFormat.class);
			FileInputFormat.setInputPaths(job, new Path(args[0]));
			FileOutputFormat.setOutputPath(job,new Path(args[1]));
			
			job.waitForCompletion(true);
			return 0;
	}
	 public static void main(String[] args) throws Exception {
	        try {
	            int res = ToolRunner.run(new Configuration(), new AdServerJob(), args);
	            System.exit(res);
	        } catch (Exception e) {
	            e.printStackTrace();
	            System.exit(255);
	        }

}
}
