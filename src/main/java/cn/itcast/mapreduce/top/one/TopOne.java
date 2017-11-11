package cn.itcast.mapreduce.top.one;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
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

/**
 * ����secondarysort�������ÿ��item����������ļ�¼
 * 
 * @author AllenWoon
 * 
 */
public class TopOne {

	static class TopOneMapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {

		OrderBean bean = new OrderBean();

		/* Text itemid = new Text(); */

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			String line = value.toString();
			String[] fields = StringUtils.split(line, ",");

			bean.set(new Text(fields[0]), new DoubleWritable(Double.parseDouble(fields[2])));

			context.write(bean, NullWritable.get());

		}

	}

	static class TopOneReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {

		@Override
		protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
			context.write(key, NullWritable.get());
		}
	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		Job job = Job.getInstance(conf);

		job.setJarByClass(TopOne.class);

		job.setMapperClass(TopOneMapper.class);
		job.setReducerClass(TopOneReducer.class);

		job.setOutputKeyClass(OrderBean.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.setInputPaths(job, new Path("D:/top/input"));
		FileOutputFormat.setOutputPath(job, new Path("D:/top/output1"));
		// FileInputFormat.setInputPaths(job, new Path(args[0]));
		// FileOutputFormat.setOutputPath(job, new Path(args[1]));
		// ָ��shuffle��ʹ�õ�GroupingComparator��
		job.setGroupingComparatorClass(ItemidGroupingComparator.class);
		// ָ��shuffle��ʹ�õ�partitioner��
		job.setPartitionerClass(ItemIdPartitioner.class);

		job.setNumReduceTasks(1);

		job.waitForCompletion(true);

	}

}
