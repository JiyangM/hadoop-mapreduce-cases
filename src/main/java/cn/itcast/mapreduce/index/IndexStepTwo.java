package cn.itcast.mapreduce.index;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class IndexStepTwo {
	
	public static class IndexStepTwoMapper extends Mapper<LongWritable, Text, Text, Text>{
		
		Text k = new Text();
		Text v = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			String line = value.toString();
			String[] fields = line.split("\t");
			String word_file = fields[0];
			String count = fields[1];
			String[] split = word_file.split("--");
			String word = split[0];
			String file = split[1];
			
			k.set(word);
			v.set(file+"--"+count);
			
			context.write(k, v);
		
		}
	}
	
	
	
	public static class IndexStepTwoReducer extends Reducer<Text, Text, Text, Text>{
		
		Text v = new Text();
		@Override
		protected void reduce(Text key, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			
			StringBuffer sBuffer = new StringBuffer();
			for (Text value : values) {
				sBuffer.append(value.toString()).append(" ");
			}
			v.set(sBuffer.toString());
			context.write(key, v);
		}
		
	}
	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(IndexStepTwo.class);
		
		//告诉程序，我们的程序所用的mapper类和reducer类是什么
		job.setMapperClass(IndexStepTwoMapper.class);
		job.setReducerClass(IndexStepTwoReducer.class);
		
		//告诉框架，我们程序输出的数据类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//这里可以进行combiner组件的设置
		job.setCombinerClass(IndexStepTwoReducer.class);
		
		//告诉框架，我们要处理的数据文件在那个路劲下
		FileInputFormat.setInputPaths(job, new Path("D:/index/output-1"));
		
		//告诉框架，我们的处理结果要输出到什么地方
		FileOutputFormat.setOutputPath(job, new Path("D:/index/output-2"));
		
		boolean res = job.waitForCompletion(true);
		
		System.exit(res?0:1);
		
	}
	
	
	
	
	
	
	

}
