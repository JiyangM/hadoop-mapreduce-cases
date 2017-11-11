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

public class MovieRateSum {

	static class MyMapper extends Mapper<LongWritable, Text, LongWritable, OriginBean> {

		ObjectMapper mapper = new ObjectMapper();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			// Bean bean = mapper.readValue(value.toString(), Bean.class);

			JSONObject valueJson = JSON.parseObject(value.toString());

			Long movie = valueJson.getLong("movie");

			OriginBean bean = new OriginBean(movie, valueJson.getLong("rate"), valueJson.getLong("timeStamp"), valueJson.getLong("uid"));

			context.write(new LongWritable(bean.getMovie()), bean);
		}
	}

	static class MyReduce extends Reducer<LongWritable, OriginBean, ResultBean, NullWritable> {

		@Override
		protected void reduce(LongWritable movie, Iterable<OriginBean> beans, Context context) throws IOException, InterruptedException {

			long sum = 0L;

			for (OriginBean bean : beans) {
				sum += bean.getRate();
			}
			ResultBean bean = new ResultBean();
			bean.setMovie(movie.get());
			bean.setSumRate(sum);
			context.write(bean, NullWritable.get());
		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);

		job.setJarByClass(MovieRateSum.class);

		job.setMapperClass(MyMapper.class);
		job.setReducerClass(MyReduce.class);

		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(OriginBean.class);

		job.setOutputKeyClass(ResultBean.class);
		job.setOutputValueClass(NullWritable.class);

		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path("x:/wordcount/jsoninput"));
		FileOutputFormat.setOutputPath(job, new Path("x:/wordcount/jsonoutput-seq"));
		// FileInputFormat.setInputPaths(job, new Path(args[0]));
		// FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
	}

}
