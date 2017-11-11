package cn.itcast.mapreduce.index;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class IndexStepOne {
	
	public static class IndexStepOneMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		
		Text k = new Text();
		IntWritable v = new IntWritable(1);
		
		@Override
		protected void map(LongWritable key, Text value,Context context)
				throws IOException, InterruptedException {

			String line = value.toString();
			String[] words = line.split(" ");
			
			FileSplit Split = (FileSplit)context.getInputSplit();
			String filename = Split.getPath().getName();
			
			//输出key :单词--文件名  value:1
			for(String word : words){
				k.set(word +"--"+ filename);
				
				context.write(k, v);
			}
		
		}
	}
	
	public static class IndexStepOneReducer extends Reducer<Text, IntWritable, Text, IntWritable>{
		
		IntWritable v = new IntWritable();
		
		@Override
		protected void reduce(Text key, Iterable<IntWritable> values,
				Context context) throws IOException, InterruptedException {

			int count = 0;
		    for(IntWritable value : values){
		    	count += value.get();
		    }
		    
		    v.set(count);
		    context.write(key, v);
		}
	}

	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(IndexStepOne.class);
		
		//告诉程序，我们的程序所用的mapper类和reducer类是什么
		job.setMapperClass(IndexStepOneMapper.class);
		job.setReducerClass(IndexStepOneReducer.class);
		
		//告诉框架，我们程序输出的数据类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		//这里可以进行combiner组件的设置
		job.setCombinerClass(IndexStepOneReducer.class);
		
		//告诉框架，我们程序使用的数据读取组件 结果输出所用的组件是什么
		//TextInputFormat是mapreduce程序中内置的一种读取数据组件  准确的说 叫做 读取文本文件的输入组件
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		//告诉框架，我们要处理的数据文件在那个路劲下
		FileInputFormat.setInputPaths(job, new Path("D:/index/input"));
		
		//告诉框架，我们的处理结果要输出到什么地方
		FileOutputFormat.setOutputPath(job, new Path("D:/index/output-1"));
		
		boolean res = job.waitForCompletion(true);
		
		System.exit(res?0:1);
		
		
		
	}
	
	
	
	
	
	
	
}
