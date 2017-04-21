package com.rpdemo.mr;


import org.apache.avro.Schema;
import org.apache.avro.Schema.Type;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.avro.AvroParquetOutputFormat;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Types;


/**
 * Created by rongpei on 2017/4/13.
 */
public class ParquetJob extends Configured implements Tool {
	
	
	private static String schema = "{\"namespace\":\"example\",\"type\":\"record\",\"name\":\"parquet.avro\",\"fields\":["
			+"{\"name\":\"date\",\"type\":\"string\"},{\"name\":\"time\",\"type\":\"string\"},{\"name\":\"created\",\"type\":\"string\"},"
			+"{\"name\":\"publisher_id\",\"type\":\"string\"},{\"name\":\"app_id\",\"type\":\"string\"},"
			+"{\"name\":\"unit_id\",\"type\":\"string\"},{\"name\":\"advertiser_id\",\"type\":\"string\"},"
			+"{\"name\":\"campaign_id\",\"type\":\"string\"},{\"name\":\"creative_id\",\"type\":\"string\"},"
			+"{\"name\":\"scenario\",\"type\":\"string\"},{\"name\":\"ad_type\",\"type\":\"string\"},"
			+"{\"name\":\"image_size\",\"type\":\"string\"},{\"name\":\"request_type\",\"type\":\"string\"},"
			+"{\"name\":\"platform\",\"type\":\"string\"},{\"name\":\"os_version\",\"type\":\"string\"},"
			+"{\"name\":\"sdk_version\",\"type\":\"string\"},{\"name\":\"device_model\",\"type\":\"string\"},"
			+"{\"name\":\"screen_size\",\"type\":\"string\"},{\"name\":\"orientation\",\"type\":\"string\"},"
			+"{\"name\":\"country_code\",\"type\":\"string\"},{\"name\":\"language\",\"type\":\"string\"},"
			+"{\"name\":\"network_type\",\"type\":\"string\"},{\"name\":\"mcc_mnc\",\"type\":\"string\"},"
			+"{\"name\":\"request_id\",\"type\":\"string\"},{\"name\":\"ip\",\"type\":\"string\"},"
			+"{\"name\":\"imei\",\"type\":\"string\"},{\"name\":\"mac\",\"type\":\"string\"},"
			+"{\"name\":\"dev_id\",\"type\":\"string\"},{\"name\":\"server_id\",\"type\":\"string\"},"
			+"{\"name\":\"reduce_flag\",\"type\":\"string\"},{\"name\":\"price_in\",\"type\":\"string\"},"
			+"{\"name\":\"price_out\",\"type\":\"string\"},{\"name\":\"gaid\",\"type\":\"string\"},"
			+"{\"name\":\"idfa\",\"type\":\"string\"},{\"name\":\"app_version\",\"type\":\"string\"},"
			+"{\"name\":\"device_brand\",\"type\":\"string\"},{\"name\":\"remote_ip\",\"type\":\"string\"},"
			+"{\"name\":\"session_id\",\"type\":\"string\"},{\"name\":\"parent_session_id\",\"type\":\"string\"},"
			+"{\"name\":\"algorithm\",\"type\":\"string\"},{\"name\":\"campaigns_id\",\"type\":\"string\"},"
			+"{\"name\":\"strategy\",\"type\":\"string\"},{\"name\":\"log_id\",\"type\":\"string\"},{\"name\":\"requestid\",\"type\":\"string\"},"
			+"{\"name\":\"ad_nums_back\",\"type\":\"string\"},{\"name\":\"ad_nums_request\",\"type\":\"string\"},"
			+"{\"name\":\"advertiser_id_2\",\"type\":\"string\"},{\"name\":\"ios_ab\",\"type\":\"string\"},{\"name\":\"ad_source_id\",\"type\":\"string\"},"
			+"{\"name\":\"frame_id\",\"type\":\"string\"},{\"name\":\"template\",\"type\":\"string\"},"
			+"{\"name\":\"cdn_ab\",\"type\":\"string\"},{\"name\":\"jump_type_ab\",\"type\":\"string\"}]}" ;
	 
    public int run(String[] args) throws  Exception{

    	Configuration conf = getConf();

        Job job = Job.getInstance(conf, "ParquetJob");
        job.setJarByClass(ParquetJob.class);
        

        FileInputFormat.addInputPath(job, new Path(args[0]));
 

        Path outputPath = new Path(args[1]);
        FileSystem fileSystem = outputPath.getFileSystem(conf);
        if (fileSystem.exists(outputPath)) {
            fileSystem.delete(outputPath, true);
        }
       
//        job.setOutputFormatClass(AvroParquetOutputFormat.class);
//        FileOutputFormat.setOutputPath(job,outputPath);
//        Schema sc =  new Schema.Parser().parse(schema);
//        AvroParquetOutputFormat.setSchema(job, sc);
       
        FileOutputFormat.setOutputPath(job,outputPath);
        job.setInputFormatClass(TextInputFormat.class);
        
        job.setMapperClass(ParquetMap.class);
        //job.setReducerClass(ParquetReduce.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        
         // job.setNumReduceTasks(4);
//        job.setOutputFormatClass(TextOutputFormat.class);
//        job.setOutputKeyClass(Text.class);
//        job.setOutputValueClass(NullWritable.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }
    
   
    
    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new ParquetJob(), args);
        System.exit(ret);
//    	 Schema sc =  new Schema.Parser().parse(schema);
//    	System.out.println(sc.getName());
    }
     
}
