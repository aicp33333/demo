package com.rpdemo.mr;

import java.io.IOException;

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

public class AnalysisFilterJob extends Configured implements Tool{
	
	public static class AnalysisFilterMap extends Mapper<LongWritable, Text, NullWritable, Text> {
		  @Override
			public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			  String str = value.toString();
	          String[] strs = str.split("\t",-1);
	          String jsonstr = strs[2].split("-params:")[1];
              JSONObject js= JSONObject.fromObject(jsonstr);
         
              if("de49ebde-5d0a-4454-ad53-fb6ba2d47db0".equals(js.get("googleAdvertisingId"))||
              		"3278b6f8-09ea-4a27-aca8-b4bafa798f12".equals(js.get("googleAdvertisingId"))){
            	  context.write(NullWritable.get(), value);
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
	 
		    Job job = Job.getInstance(getConf(), "AnalysisFilterJob");
	        job.setJarByClass(getClass());
	        job.setJobName(getClass().getName());
	        job.setMapperClass(AnalysisFilterMap.class);
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
	            int res = ToolRunner.run(new Configuration(), new AnalysisFilterJob(), args);
	            System.exit(res);
	        } catch (Exception e) {
	            e.printStackTrace();
	            System.exit(255);
	        }

}
}
