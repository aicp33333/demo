/**
 * 
 */
package com.rpdemo.reducejoin;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/**
 * @author rongpei
 * 2017年4月29日
 */
public class KeyPartitioner extends Partitioner<CombineValues,Text>{
	@Override
	public int getPartition(CombineValues key, Text value, int numPartitions) {
		return (key.getJoinKey().hashCode()&Integer.MAX_VALUE) % numPartitions;
	}
}
