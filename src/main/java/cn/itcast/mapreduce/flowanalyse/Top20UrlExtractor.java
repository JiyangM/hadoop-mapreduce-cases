package cn.itcast.mapreduce.flowanalyse;

import java.io.IOException;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Top20UrlExtractor {

	/**
	 * 
	 * @author
	 * 
	 */
	static class Top20UrlExtractorMapper extends Mapper<LongWritable, Text, Text, FlowBean> {

		private Text k = new Text();
		private FlowBean bean = new FlowBean();
		Counter malformedlines = null;
		@Override
		protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			malformedlines = context.getCounter("malformed", "malformedlines");
			String line = value.toString();

			String[] fields = StringUtils.split(line, "\t");

			try {
				// 鎶藉彇url鍜屼笂琛屾祦閲�
				// 杩樻湁寰堝缁嗚妭锛屾湁浜泆rl鍙槸鏈�悗甯︾殑鍙傛暟涓嶅悓,鎵�互锛岃繖閲屾彁鍙栦粠瀛楁杩樺彲浠ヨ繘琛屼竴浜涢�褰撶殑鎴彇
				String url = fields[26];
				if (StringUtils.isNotBlank(url) && url.startsWith("http")) {
					long upflow = Long.parseLong(fields[30]);
					k.set(url);
					bean.set(url, upflow);
					context.write(k, bean);
				}else{
					malformedlines.increment(1);
				}
			} catch (Exception e) {

				// 鑷畾涔夎鏁板櫒
				malformedlines.increment(1);

			}

		}

	}

	/**
	 * 瑕佸湪杩欎釜reducer涓畬鎴愭眹鎬伙紝鎺掑簭锛屽拰杈撳嚭鍓�0%鐨勯�杈�
	 * 
	 * @author
	 * 
	 */
	static class Top20UrlExtractorReducer extends Reducer<Text, FlowBean, FlowBean, NullWritable> {

		// 鐢ㄦ潵绱鏍锋湰鏁版嵁鍏ㄥ眬鎬绘祦閲�
		private long globalCount = 0;

		// 鍒涘缓涓�釜TreeMap鏉ョ紦瀛樺崟鏉rl娴侀噺缁熻缁撴灉
		private TreeMap<FlowBean, String> treeMap = new TreeMap<FlowBean, String>();


		// 鍙傛暟key鏄痷rl锛宐eans鏄繖涓猽rl鐨勬墍鏈変笂琛屾祦閲廱ean
		@Override
		protected void reduce(Text key, Iterable<FlowBean> beans, Context context) throws IOException, InterruptedException {

			long count = 0;
			for (FlowBean value : beans) {

				count += value.getUpflow();
			}

			// 鎶婅繖涓�潯url鐨勭粺璁＄粨鏋滃皝瑁呭埌countBean涓�
			FlowBean countBean = new FlowBean();
			countBean.set(key.toString(), count);

			// 鎶婅繖娆＄粺璁″嚭鏉ョ殑涓�潯url鐨勪笂琛屾�娴侀噺绱姞鍒板叏灞��娴侀噺涓�
			globalCount += count;

			// 灏嗚繖娆＄粺璁″嚭鏉ョ殑涓�潯url鐨勪笂琛屾�娴侀噺缂撳瓨鍒皌reemap涓�
			// treemap涓殑key鏄祦閲廱ean锛寁alue鏄痷rl锛屼互渚胯treemap涓烘垜浠娴侀噺鍊艰繘琛屽�搴忔帓搴�
			treeMap.put(countBean, key.toString());

		}

		/**
		 * 涓�釜reducetask鍦ㄥ鐞嗗畬鎵�湁鏁版嵁鍚庯紝浼氬湪finally涓皟鐢ㄤ竴娆leanup
		 */
		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {

			Set<Entry<FlowBean, String>> entrySet = treeMap.entrySet();

			long tmpCount = 0;
			for (Entry<FlowBean, String> ent : entrySet) {

				if (tmpCount / globalCount < 0.8) {
					tmpCount += ent.getKey().getUpflow();
					context.write(ent.getKey(), NullWritable.get());
				} else {
					return;
				}
			}

		}

	}

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();

		Job job = Job.getInstance(conf);

		job.setJarByClass(Top20UrlExtractor.class);

		job.setMapperClass(Top20UrlExtractorMapper.class);
		job.setReducerClass(Top20UrlExtractorReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);

		job.setOutputKeyClass(FlowBean.class);
		job.setOutputValueClass(NullWritable.class);

		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));

		job.waitForCompletion(true);
		System.exit(0);

	}

}
