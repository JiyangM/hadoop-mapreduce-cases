package cn.itcast.mapreduce.commonfriends;

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

import cn.itcast.mapreduce.index.IndexStepTwo;
import cn.itcast.mapreduce.index.IndexStepTwo.IndexStepTwoMapper;
import cn.itcast.mapreduce.index.IndexStepTwo.IndexStepTwoReducer;

public class CommonFriendsStepOne {
	
	public static class CommonFriendsStepOneMapper extends Mapper<LongWritable, Text, Text, Text>{
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
				String line = value.toString();
				String[] splits = line.split(":");
				String person = splits[0];
				String[] friends = splits[1].split(",");
				
				for(String fString :friends){
					context.write(new Text(fString), new Text(person));
				}

		}
	}
	
	public static class CommonFriendsStepOneReducer extends Reducer<Text, Text, Text, Text>{
		
		@Override
		protected void reduce(Text friend, Iterable<Text> persons, Context context)
				throws IOException, InterruptedException {

               StringBuffer sBuffer = new StringBuffer();
               for(Text pText :persons){
            	   sBuffer.append(pText).append("-");
               }
               
               context.write(friend, new Text(sBuffer.toString()));
		}
	}
	
	
	public static void main(String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);
		
		job.setJarByClass(CommonFriendsStepOne.class);
		
		//告诉程序，我们的程序所用的mapper类和reducer类是什么
		job.setMapperClass(CommonFriendsStepOneMapper.class);
		job.setReducerClass(CommonFriendsStepOneReducer.class);
		
		//告诉框架，我们程序输出的数据类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		//告诉框架，我们要处理的数据文件在那个路劲下
		FileInputFormat.setInputPaths(job, new Path("D:\\commonfriends\\input"));
		
		//告诉框架，我们的处理结果要输出到什么地方
		FileOutputFormat.setOutputPath(job, new Path("D:\\commonfriends\\output-1"));
		
		boolean res = job.waitForCompletion(true);
		
		System.exit(res?0:1);
	}
	
	
	

}
