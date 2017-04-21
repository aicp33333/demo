package com.rpdemo.mr;

import org.apache.avro.Schema;
import org.apache.commons.io.output.NullWriter;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.Counters.Group;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * Created by rongpei on 2017/4/13.
 */
public class ParquetMap extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        //String line = value.toString();
        System.out.println("-----元数据----" + value.toString());
        context.write(new Text(""), value);
    }

	public static void main(String[] args) {
    	
    	Schema sc= new Schema.Parser().parse("{\"namespace\": \"example\", \"type\": \"record\",  \"name\": \"parquet.avro\", \"fields\": [ { \"name\": \"date\", \"type\": \"string\" }   ]}");
    	System.out.println(sc.getName());
	}
}
