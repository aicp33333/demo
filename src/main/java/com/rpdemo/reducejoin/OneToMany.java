/**
 * 
 */
package com.rpdemo.reducejoin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapred.TextInputFormat;
 
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonProcessingException;
import org.codehaus.jackson.map.ObjectMapper;

import com.rpdemo.utils.MRUtils;

 

/**
 * @author rongpei installIdSet 转换为 packagename packagename存在多个时，以\0x1为分割 拼接字符串
 *         输出格式：id \t tyep(gaid/idfa) \t (ios/ard) \t packagename
 */
public class OneToMany extends Configured implements Tool {

	public static class OneToManyMap extends Mapper<LongWritable, Text, CombineValues, Text> {

		protected List<CombineValues> list = new ArrayList<CombineValues>();
		protected ObjectMapper om = new ObjectMapper();
		protected JsonNode jn = null;
		protected Text secondPart = new Text();
		protected String regex = "^[0-9a-fA-F]{8}(-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}$";

		private void getCombineValues(String jsonStr, String date) throws JsonProcessingException, IOException {
			list.clear();
			if (jsonStr.startsWith("{")) {
				jn = om.readTree(jsonStr);
				/**
				 * googleAdvertisingId,idfa 不能同时为空 googleAdvertisingId=gaid
				 */
				if (!"".equals(jn.get("googleAdvertisingId").asText()) || !"null".equals(jn.get("idfa").asText())) {
					String campaign = jn.get("installIdSet").toString();
					String pack = jn.get("includePackageNameSet").toString();

					/**
					 * installIdSet 安装列表id includePackageNameSet 安装包名
					 * 同时为空数据无意义不处理
					 */
					if ("null".equals(campaign) && "null".equals(pack)) {
						return;
					}
					// packagename 拼接字符串
					StringBuilder packName = new StringBuilder();
					if (!"null".equals(pack)) {
						Map<String, String> packMap = om.readValue(pack, Map.class);
						Set<String> set = packMap.keySet();
						for (String string : set) {
							packName.append(string).append("\0x1");
						}
					}

					String gaid = jn.get("googleAdvertisingId").asText();
					String idfa = jn.get("idfa").asText();
					// platform =1 是 gaid adr platform=2 是 idfa ios
					if (jn.get("platform").asText().equals("1") && gaid.matches(regex)) {
						secondPart.set(MRUtils.JOINER.join(jn.get("googleAdvertisingId").asText(), "gaid", "android",packName.toString()));
					} else if (jn.get("platform").asText().equals("2") && idfa.matches(regex)) {
						secondPart.set(MRUtils.JOINER.join(jn.get("idfa").asText(), "idfa", "ios", packName.toString()));
					} else {
						secondPart.set("");
					}
					if ("null".equals(campaign) && !secondPart.toString().equals("")) {
						CombineValues combineValues = new CombineValues();
						combineValues.setJoinKey(new Text(""));
						combineValues.setFlag(new Text("2"));
						list.add(combineValues);
					} else if (!secondPart.toString().equals("")) {
						// 每个installId输出一条数据
						Map<String, String> packMap = om.readValue(campaign, Map.class);
						Set<String> set = packMap.keySet();
						for (String string : set) {
							CombineValues combineValues = new CombineValues();
							combineValues.setJoinKey(new Text(string));
							combineValues.setFlag(new Text("2"));
							list.add(combineValues);
						}

					}
				}
			}
		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			String line = value.toString();

			String[] lines = line.split("\t", -1);
			// ad_server_empty 数据长度为6
			// ad_server数据长度为4
			// dim_campaign_package_adn 数据长度为4
			if (lines.length == 6) {
				String date = lines[0].split(" ")[0];
				String jsonstr = lines[2].split("-params:")[1].split("-resul")[0].trim();
				getCombineValues(jsonstr, date);
				if (!secondPart.toString().equals("")) {
					for (CombineValues combineValues : list) {
						context.write(combineValues, secondPart);
					}
				}
			} else if (lines.length == 4) {
				if (!lines[0].matches("[0-9]*")) {
					String date = lines[0].split(" ")[0];
					String jsonstr = lines[2];
					getCombineValues(jsonstr, date);
					if (!secondPart.toString().equals("")) {
						for (CombineValues combineValues : list) {
							context.write(combineValues, secondPart);
						}
					}

				} else {
					CombineValues combineValues = new CombineValues();
					combineValues.setJoinKey(new Text(lines[0]));
					combineValues.setFlag(new Text("1"));
					context.write(combineValues, new Text(lines[2]));
				}
			}
		}
	}

	public static class OneToManyReduce extends Reducer<CombineValues, Text, Text, Text> {

		protected Text secondPar = null;
		protected Text output = new Text();
		protected String regex = "^[0-9a-fA-F]{8}(-[0-9a-fA-F]{4}){3}-[0-9a-fA-F]{12}$";
		/**
		 * dim_campaign_package_adn adserver 是 1 对 多
		 */
		@Override
		protected void reduce(CombineValues key, Iterable<Text> value, Context context)
				throws IOException, InterruptedException {

			String packageName = value.iterator().next().toString();
			if(packageName.matches(regex)){
				packageName = "";
				context.getCounter("packageName", "errorname").increment(1);
			}
			if (packageName.indexOf("\t") != -1) {
				String[] lines = packageName.split("\t", -1);
				String pack = lines[3];
				if (!pack.equals("") && !pack.matches(regex)) {
					output.set(MRUtils.JOINER.join(lines[0], lines[1], lines[2], pack));
					context.write(secondPar, output);
				}
				packageName = "";
			} 

			for (Text text : value) {
				String[] lines = text.toString().split("\t", -1);
				String pack = lines[3];
				if(!pack.matches(regex)){
					pack = pack + packageName;
				}else{
					pack = packageName;
				}
				
				if (!pack.equals("")) {
					output.set(MRUtils.JOINER.join(lines[0], lines[1], lines[2], pack));
					context.write(secondPar, output);
				}
			}
		}
	}

	public int run(String[] args) throws Exception {
		Path outputPath = new Path(args[3]);
		FileSystem fileSystem = outputPath.getFileSystem(getConf());
		if (fileSystem.exists(outputPath)) {
			fileSystem.delete(outputPath, true);
		}
		Job job = Job.getInstance(getConf(), "AdServerPackage");
		job.setJarByClass(OneToMany.class);
		FileInputFormat.addInputPath(job, new Path(args[0])); // 设置map输入文件路径
		FileInputFormat.addInputPath(job, new Path(args[1]));
		FileInputFormat.addInputPath(job, new Path(args[2]));
		FileOutputFormat.setOutputPath(job, new Path(args[3])); // 设置reduce输出文件路径
         
//		Path onePath = new Path(args[0]);
//		Path manyPath = new Path(args[1]);
//		MultipleInputs.addInputPath(job, onePath, TextInputFormat.class, OneToManyMap.class);
//		MultipleInputs.addInputPath(job, manyPath, TextInputFormat.class, OneToManyMap.class);
		
		job.setMapperClass(OneToManyMap.class);
		job.setReducerClass(OneToManyReduce.class);
		job.setNumReduceTasks(80);
		// job.setOutputFormatClass(TextOutputFormat.class);// 使用默认的output格格式
		job.setPartitionerClass(KeyPartitioner.class);
		job.setGroupingComparatorClass(CombineValues.FirstComparator.class);

		FileOutputFormat.setCompressOutput(job, true);
		FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class);
		// 设置map的输出key和value类型
		job.setMapOutputKeyClass(CombineValues.class);
		job.setMapOutputValueClass(Text.class);

		// 设置reduce的输出key和value类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.waitForCompletion(true);
		return job.isSuccessful() ? 0 : 1;
	}

	public static void main(String[] args) throws Exception {
		try {
			int res = ToolRunner.run(new Configuration(), new OneToMany(), args);
			System.exit(res);
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(255);
		}

	}

}
