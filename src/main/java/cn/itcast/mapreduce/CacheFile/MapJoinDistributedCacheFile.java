package cn.itcast.mapreduce.CacheFile;


import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class MapJoinDistributedCacheFile {
	private static final Log log = LogFactory.getLog(MapJoinDistributedCacheFile.class);
	public static class MapJoinDistributedCacheFileMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
	
		FileReader in = null;
		BufferedReader reader = null;
		HashMap<String,String[]> b_tab = new HashMap<String, String[]>();
		
		@Override
		protected void setup(Context context)throws IOException, InterruptedException {
			// 此处加载的是产品表的数据
			in = new FileReader("pdts.txt");
			reader = new BufferedReader(in);
			String line =null;
			while(StringUtils.isNotBlank((line=reader.readLine()))){
				String[] split = line.split(",");
				String[] products = {split[0],split[1]};
				b_tab.put(split[0], products);
			}
			IOUtils.closeStream(reader);
			IOUtils.closeStream(in);
		}
		
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] orderFields = line.split(",");
			String pdt_id = orderFields[1];
			String[] pdtFields = b_tab.get(pdt_id);
			String ll = orderFields[0] + "\t" + pdtFields[1] + "\t" + orderFields[1] + "\t" + orderFields[2] ;
			context.write(new Text(ll), NullWritable.get());
		}
	}
	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(MapJoinDistributedCacheFile.class);
		job.setMapperClass(MapJoinDistributedCacheFileMapper.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		FileInputFormat.setInputPaths(job, new Path("D:/mapjoin/input"));
		FileOutputFormat.setOutputPath(job, new Path("D:/mapjoin/output"));
			
		job.setNumReduceTasks(0);
		
		job.addCacheFile(new URI("file:/D:/pdts.txt"));
//		job.addCacheFile(new URI("hdfs://mini1:9000/cachefile/pdts.txt"));
		
		
		job.waitForCompletion(true);
	}
}
