package cn.itcast.mapreduce.json;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.codehaus.jackson.map.ObjectMapper;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;

public class JsonToText {

	static class MyMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

		Text k = new Text();
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// Bean bean = mapper.readValue(value.toString(), Bean.class);

			JSONObject valueJson = JSON.parseObject(value.toString());

			Long movie = valueJson.getLong("movie");

			OriginBean bean = new OriginBean(movie, valueJson.getLong("rate"), valueJson.getLong("timeStamp"), valueJson.getLong("uid"));
			k.set(bean.toString());
			context.write(k, NullWritable.get());
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		conf.set("mapreduce.input.fileinputformat.split.maxsize", "16777216");

		Job job = Job.getInstance(conf);

		job.setJarByClass(JsonToText.class);

		job.setMapperClass(MyMapper.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		// job.setOutputFormatClass(SequenceFileOutputFormat.class);
		job.setNumReduceTasks(0);
		
		FileInputFormat.setInputPaths(job, new Path("D:/josn/input"));
		FileOutputFormat.setOutputPath(job, new Path("D:/josn/output"));
		// FileInputFormat.setInputPaths(job, new Path(args[0]));
		// FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}
