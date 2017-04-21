package com.rpdemo.mr;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.util.Tool;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.example.ExampleInputFormat;
import org.apache.parquet.hadoop.example.ExampleOutputFormat;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
 
public  class ParquetMRjob extends Configured implements Tool{
	
	
	
	
	 

	public int run(String[] args) throws Exception {
		String inputFile = args[0];
		String outputFile = args[1];
		String compression = (args.length > 2) ? args[2] : "none";

		
		Path parquetFilePath = null;
		// Find a file in case a directory was passed
		RemoteIterator<LocatedFileStatus> it = FileSystem.get(getConf()).listFiles(new Path(inputFile), true);
		while(it.hasNext()) {
		    FileStatus fs = it.next();
		    if(fs.isFile()) {
			parquetFilePath = fs.getPath();
			break;
		    }
		}
		if(parquetFilePath == null) {
		     
		    return 1;
		}
		ParquetMetadata readFooter = ParquetFileReader.readFooter(getConf(), parquetFilePath);
		MessageType schema = readFooter.getFileMetaData().getSchema();
		GroupWriteSupport.setSchema(schema, getConf());
		Configuration conf = getConf();
		Job job = Job.getInstance(conf, "ParquetJob");
	        job.setJarByClass(getClass());
	        job.setJobName(getClass().getName());
	        job.setMapperClass(ParquetMap.class);
		job.setNumReduceTasks(0);
		job.setInputFormatClass(ExampleInputFormat.class);
		job.setOutputFormatClass(ExampleOutputFormat.class);

		CompressionCodecName codec = CompressionCodecName.UNCOMPRESSED;
		if(compression.equalsIgnoreCase("snappy")) {
		    codec = CompressionCodecName.SNAPPY;
		} else if(compression.equalsIgnoreCase("gzip")) {
		    codec = CompressionCodecName.GZIP;
		}
		ExampleOutputFormat.setCompression(job, codec);

		FileInputFormat.setInputPaths(job, new Path(inputFile));
	        FileOutputFormat.setOutputPath(job, new Path(outputFile));

	        job.waitForCompletion(true);
		// TODO Auto-generated method stub
	        return job.waitForCompletion(true) ? 0 : 1;
	}
	

    public static void main(String[] args) throws Exception {
        int ret = ToolRunner.run(new ParquetJob(), args);
        System.exit(ret);
 
    }
}