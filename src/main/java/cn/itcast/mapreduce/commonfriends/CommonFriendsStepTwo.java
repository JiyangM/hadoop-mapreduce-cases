package cn.itcast.mapreduce.commonfriends;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class CommonFriendsStepTwo {
	
	/**
	 *  A	I-K-C-B-G-F-H-O-D-
		B	A-F-J-E-
		C	A-E-B-H-F-G-K-
	 *
	 */
	public static class CommonFriendsStepTwoMapper extends Mapper<LongWritable, Text, Text, Text>{
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
				
			String line = value.toString();
			String[] splits = line.split("\t");
			String friend = splits[0];
			String[] persons = splits[1].split("-");
			
			Arrays.sort(persons);
			
			for (int i = 0; i < persons.length-1; i++) {
				for (int j = i+1; j < persons.length; j++) {
					context.write(new Text(persons[i]+"-"+persons[j]), new Text(friend));
				}
				
			}

		}
	}
	
	
	public static class CommonFriendsStepTwoReducer extends Reducer<Text, Text, Text, Text>{
		@Override
		protected void reduce(Text person_pair, Iterable<Text> friends, Context context)
				throws IOException, InterruptedException {
			
			StringBuffer sBuffer = new StringBuffer();
			for(Text fText : friends){
				sBuffer.append(fText).append(" ");
			}
			context.write(person_pair, new Text(sBuffer.toString()));
		}
	}
	
	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(CommonFriendsStepTwo.class);
		
		//告诉程序，我们的程序所用的mapper类和reducer类是什么
		job.setMapperClass(CommonFriendsStepTwoMapper.class);
		job.setReducerClass(CommonFriendsStepTwoReducer.class);
		
		//告诉框架，我们程序输出的数据类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//告诉框架，我们要处理的数据文件在那个路劲下
		FileInputFormat.setInputPaths(job, new Path("D:\\commonfriends\\output-1"));
		
		//告诉框架，我们的处理结果要输出到什么地方
		FileOutputFormat.setOutputPath(job, new Path("D:\\commonfriends\\output-2"));
		
		boolean res = job.waitForCompletion(true);
		
		System.exit(res?0:1);
	}
	
	
	
	
	
	
	
	
	
	
	
	

}
