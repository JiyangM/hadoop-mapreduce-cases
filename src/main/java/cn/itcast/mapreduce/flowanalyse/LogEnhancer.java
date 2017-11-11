package cn.itcast.mapreduce.flowanalyse;

import java.io.IOException;
import java.util.HashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;

/**
 * 日志增强：从原始日志中获取url，然后去知识库查找内容信息标签，追加到原始日志中
 * 如果查不到的url，就输出到待爬清单中
 * 
 * @author
 * 
 */
public class LogEnhancer extends Configured implements Tool{

	static class LogEnhancerMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

		HashMap<String, String> knowledgeMap = new HashMap<String, String>();

		/**
		 * maptask   在setup方法中，将外部的知识库加载到内存中，以便于快速匹配
		 */
		@Override
		protected void setup(Context context) throws IOException, InterruptedException {

			try {
				DBLoader.loadDB(knowledgeMap);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
//			Counter counter = context.getCounter("malformed", "malformed_line");
			String line = value.toString();

			String[] fields = StringUtils.split(line, "\t");
			try {
				
				String url = fields[26];
//				if(!url.startsWith("http")){counter.increment(1);;}
				// 从规则库中匹配标签信息
				String content = knowledgeMap.get(url);

				// 判断规则库匹配结果
				String result = "";
				if (null == content) {
					// 如果匹配失败，则将这条url到带爬清单文件中
					result = url + "\t" + "tocrawl\n";
				} else {
					// 如果匹配成功，则将这条“原始日志+内容标签”输出到增强日志文件中
					result = line + "\t" + content + "\n";
				}

				context.write(new Text(result), NullWritable.get());
			} catch (Exception e) {
//				counter.increment(1);;
			}
		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);

		job.setJarByClass(LogEnhancer.class);

		job.setMapperClass(LogEnhancerMapper.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		// 要控制不同的内容写往不同的目标路径，可以采用自定义outputformat的方法
		job.setOutputFormatClass(LogEnhancerOutputFormat.class);

		FileInputFormat.setInputPaths(job, new Path("/flow/input"));

		//尽管我们用的是自定义outputformat，但是它是继承制fileoutputformat
		//在fileoutputformat中，必须输出一个success文件，所以在此还需要设置输出path
		FileOutputFormat.setOutputPath(job, new Path(args[0]));

		//不需要reducer
		job.setNumReduceTasks(0);
		
		
		boolean res = job.waitForCompletion(true);
		System.exit(res?0:1);

	}

	@Override
	public int run(String[] args) throws Exception {
		// TODO Auto-generated method stub
		return 0;
	}

}
