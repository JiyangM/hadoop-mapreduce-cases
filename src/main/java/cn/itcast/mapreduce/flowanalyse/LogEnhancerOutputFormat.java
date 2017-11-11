package cn.itcast.mapreduce.flowanalyse;

import java.io.IOException;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * map task 或者 reduce task在做最终输出时，它的工作步骤是：
 * 1、通过用户conf中设置的outputformat实现类的getRecordWriter()方法获取到一个RecordWriter的具体实例对象
 * 2、利用RecordWriter的具体实例对象调用其中的write(k,v,context)方法来将数据写出
 * 
 * @author
 * 
 */
public class LogEnhancerOutputFormat extends FileOutputFormat<Text, NullWritable> {

	@Override
	public RecordWriter<Text, NullWritable> getRecordWriter(TaskAttemptContext context) throws IOException, InterruptedException {
		Path enhaPath = new Path("hdfs://mini1:9000/flow/enhance/enhanced.log");
		Path tocrawlPath = new Path("hdfs://mini1:9000/flow/tocrawl/tocrawl.dat");

		FileSystem fs = FileSystem.get(context.getConfiguration());
		FSDataOutputStream enhanceOut = fs.create(enhaPath);
		FSDataOutputStream tocrawlOut = fs.create(tocrawlPath);

		return new LogEnhanceRecordWriter(enhanceOut,tocrawlOut);
	}

	public class LogEnhanceRecordWriter extends RecordWriter<Text, NullWritable> {

		FSDataOutputStream enhanceOut;
		FSDataOutputStream tocrawlOut;

		public LogEnhanceRecordWriter(FSDataOutputStream enhanceOut, FSDataOutputStream tocrawlOut) {
			this.enhanceOut = enhanceOut;
			this.tocrawlOut = tocrawlOut;
		}

		// 具体的输出逻辑就在此方法
		@Override
		public void write(Text key, NullWritable value) throws IOException, InterruptedException {
			if (key.toString().contains("tocrawl")) {

				tocrawlOut.write(key.toString().getBytes());
			} else {
				enhanceOut.write(key.toString().getBytes());
			}

		}

		@Override
		public void close(TaskAttemptContext context) throws IOException, InterruptedException {

			if (tocrawlOut != null) {
				tocrawlOut.close();
			}
			if (enhanceOut != null) {
				enhanceOut.close();
			}

		}

	}

}
