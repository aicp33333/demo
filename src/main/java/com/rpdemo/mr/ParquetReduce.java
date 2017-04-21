package com.rpdemo.mr;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class ParquetReduce extends Reducer< Text, Iterable<Text>,NullWritable,Text> {

	 protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
         
		 StringBuilder  sb = new StringBuilder();
		 for (Text text : values) {
			 sb.append(text+"\n");
		}
		 context.write(NullWritable.get(),new Text(sb.toString()));
     }
}
