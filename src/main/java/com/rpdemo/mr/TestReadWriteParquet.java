package com.rpdemo.mr;


import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import  org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.Log;
import org.apache.parquet.example.data.Group;
 
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.format.PageHeader;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetRecordReader;
import org.apache.parquet.hadoop.example.ExampleOutputFormat;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.util.ContextUtil;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;



public class TestReadWriteParquet  extends Configured implements Tool{
	private static final Log LOG = Log.getLog(TestReadWriteParquet.class);
    /*
     * Read a Parquet record, write a Parquet record
     */
    public static class ReadRequestMap extends Mapper<LongWritable, Text, Void, Group> {
    	private SimpleGroupFactory factory; 
    	 @Override 
    	    protected void setup(Context context) throws IOException, InterruptedException { 
    	      factory = new SimpleGroupFactory(GroupWriteSupport.getSchema(ContextUtil.getConfiguration(context))); 
    	    } 
        @Override
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        	String str = value.toString();
        	String[] strs = str.split("\t",-1);
        	Group group = factory.newGroup();
        	 
        	group.add("date", strs[0]);
        	group.add("time", strs[1]);
        	group.add("created", strs[2]);
        	group.add("publisher_id", strs[3]);
        	group.add("app_id", strs[4]);
        	group.add("unit_id", strs[5]);
        	group.add("advertiser_id", strs[6]);
        	group.add("campaign_id", strs[7]);
        	group.add("creative_id", strs[8]);
        	group.add("scenario", strs[9]);
        	group.add("ad_type", strs[10]);
        	group.add("image_size", strs[11]);
        	group.add("request_type", strs[12]);
        	group.add("platform", strs[13]);
        	group.add("os_version", strs[14]);
        	group.add("sdk_version", strs[15]);
        	group.add("device_model", strs[16]);
        	group.add("screen_size", strs[17]);
        	group.add("orientation", strs[18]);
        	group.add("country_code", strs[19]);
        	group.add("language", strs[20]);
        	group.add("network_type", strs[21]);
        	group.add("mcc_mnc", strs[22]);
        	group.add("request_id", strs[23]);
        	group.add("ip", strs[24]);
        	group.add("imei", strs[25]);
        	group.add("mac", strs[26]);
        	group.add("dev_id", strs[27]);
        	group.add("server_id", strs[28]);
        	group.add("reduce_flag", strs[29]);
        	group.add("price_in", strs[30]);
        	group.add("price_out", strs[31]);
        	group.add("gaid", strs[32]);
        	group.add("idfa", strs[33]);
        	group.add("app_version", strs[34]);
        	group.add("device_brand", strs[35]);
        	group.add("remote_ip", strs[36]);
        	group.add("session_id", strs[37]);
        	group.add("parent_session_id", strs[38]);
        	group.add("algorithm", strs[39]);
        	group.add("campaigns_id", strs[40]);
        	group.add("strategy", strs[41]);
        	group.add("log_id", strs[42]);
        	group.add("requestid", strs[43]);
        	group.add("ad_nums_back", strs[44]);
        	group.add("ad_nums_request", strs[45]);
        	group.add("advertiser_id_2", strs[46]);
        	group.add("ios_ab", strs[47]);
        	group.add("ad_source_id", strs[48]);
        	group.add("frame_id", strs[49]);
        	group.add("template", strs[50]);
        	group.add("cdn_ab", strs[51]);
        	group.add("jump_type_ab", strs[52]);
        	
	        context.write(null, group);
        }
    }
    
    public static MessageType schema = MessageTypeParser.parseMessageType( 
            " message people { " + 
                    "required binary date ;" +
"required binary time;" +
"required binary created;" +
"required binary publisher_id;" +
"required binary app_id;" +
"required binary unit_id;" +
"required binary advertiser_id;" +
"required binary campaign_id;" +
"required binary creative_id;" +
"required binary scenario;" +
"required binary ad_type;" +
"required binary image_size;" +
"required binary request_type;" +
"required binary platform;" +
"required binary os_version;" +
"required binary sdk_version;" +
"required binary device_model;" +
"required binary screen_size;" +
"required binary orientation;" +
"required binary country_code;" +
"required binary language;" +
"required binary network_type;" +
"required binary mcc_mnc;" +
"required binary request_id;" +
"required binary ip;" +
"required binary imei;" +
"required binary mac;" +
"required binary dev_id;" +
"required binary server_id;" +
"required binary reduce_flag;" +
"required binary price_in;" +
"required binary price_out;" +
"required binary gaid;" +
"required binary idfa;" +
"required binary app_version;" +
"required binary device_brand;" +
"required binary remote_ip;" +
"required binary session_id;" +
"required binary parent_session_id;" +
"required binary algorithm;" +
"required binary campaigns_id;" +
"required binary strategy;" +
"required binary log_id;" +
"required binary requestid;" +
"required binary ad_nums_back;" +
"required binary ad_nums_request;" +
"required binary advertiser_id_2;" +
"required binary ios_ab;" +
"required binary ad_source_id;" +
"required binary frame_id;" +
"required binary template;" +
"required binary cdn_ab;" +
"required binary jump_type_ab;"+
                    " }" 
    ); 
    
    public int run(String[] args) throws Exception {
	if(args.length < 2) {
	    LOG.error("Usage: " + getClass().getName() + " INPUTFILE OUTPUTFILE [compression]");
	    return 1;
	}
	String inputFile = args[0];
	String outputFile = args[1];
//	String compression = (args.length > 2) ? args[2] : "none";
	 Path outputPath = new Path(args[1]);
     FileSystem fileSystem = outputPath.getFileSystem(getConf());
     if (fileSystem.exists(outputPath)) {
         fileSystem.delete(outputPath, true);
     }

	 Job job = Job.getInstance(getConf(), "ParquetJob");
        job.setJarByClass(getClass());
        job.setJobName(getClass().getName());
        job.setMapperClass(ReadRequestMap.class);
	job.setNumReduceTasks(0);
	//job.setInputFormatClass(TextInputFormat.class);
	job.setOutputFormatClass(ExampleOutputFormat.class);
	ExampleOutputFormat.setSchema(job, schema);
	ExampleOutputFormat.setBlockSize(job, 500 * 1024 * 1024);
	ExampleOutputFormat.setCompression(job, CompressionCodecName.GZIP);
	ExampleOutputFormat.setEnableDictionary(job, true);
	


	    FileInputFormat.setInputPaths(job, new Path(inputFile));
        FileOutputFormat.setOutputPath(job, new Path(outputFile));

        job.waitForCompletion(true);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        try {
            int res = ToolRunner.run(new Configuration(), new TestReadWriteParquet(), args);
            System.exit(res);
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(255);
        }
    }

}
