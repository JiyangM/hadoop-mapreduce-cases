package cn.itcast.mapreduce.top.n;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shell.Count;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.sun.xml.bind.v2.schemagen.xmlschema.List;


public class TopN {

	static class TopNMapper extends Mapper<LongWritable, Text, OrderBean, OrderBean> {
		OrderBean v = new OrderBean();
		Text k = new Text();

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();
			String[] fields = StringUtils.split(line, ",");
			k.set(fields[0]);

			v.set(new Text(fields[0]), new DoubleWritable(Double.parseDouble(fields[2])));

			context.write(v, v);

		}

	}

	static class TopNReducer extends Reducer<OrderBean, OrderBean, NullWritable, OrderBean> {
		int topn = 1;
		int count = 0;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			Configuration conf = context.getConfiguration();
			topn = Integer.parseInt(conf.get("topn"));
		}

		@Override
		protected void reduce(OrderBean key, Iterable<OrderBean> values, Context context) throws IOException, InterruptedException {
			for (OrderBean bean : values) {
				if ((count++) == topn) {
					count = 0;
					return;
				}
				context.write(NullWritable.get(), bean);
			}
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
//		conf.addResource("userconfig.xml");
//		System.out.println(conf.get("top.n"));
		conf.set("topn", "2");
		Job job = Job.getInstance(conf);

		job.setJarByClass(TopN.class);

		job.setMapperClass(TopNMapper.class);
		job.setReducerClass(TopNReducer.class);

		job.setMapOutputKeyClass(OrderBean.class);
		job.setMapOutputValueClass(OrderBean.class);

		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(OrderBean.class);

		FileInputFormat.setInputPaths(job, new Path("D:/top/input"));
		FileOutputFormat.setOutputPath(job, new Path("D:/top/outputn"));
		
		job.setPartitionerClass(ItemIdPartitioner.class);
		job.setGroupingComparatorClass(ItemidGroupingComparator.class);

		job.setNumReduceTasks(1);

		job.waitForCompletion(true);

	}

}
