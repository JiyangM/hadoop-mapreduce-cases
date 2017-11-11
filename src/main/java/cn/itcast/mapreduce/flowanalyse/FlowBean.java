package cn.itcast.mapreduce.flowanalyse;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.WritableComparable;

public class FlowBean implements WritableComparable<FlowBean> {

	private String url;
	private long upflow;

	// 鏃犲弬鏋勯�涓�畾瑕佹湁
	public FlowBean() {
	}

	public void set(String url, long upflow) {

		this.url = url;
		this.upflow = upflow;

	}

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}

	public long getUpflow() {
		return upflow;
	}

	public void setUpflow(long upflow) {
		this.upflow = upflow;
	}

	@Override
	public void readFields(DataInput in) throws IOException {

		url = in.readUTF();
		upflow = in.readLong();

	}

	@Override
	public void write(DataOutput out) throws IOException {

		out.writeUTF(url);
		out.writeLong(upflow);

	}

	@Override
	public int compareTo(FlowBean o) {

		return this.upflow > o.getUpflow() ? -1 : 1;
	}

	
	@Override
	public String toString() {
		 
		return  this.url + "\t" + this.upflow;
	}
	
	
	public static void main(String[] args) {
		String ss = "1374609503.88	1374609503.92	1374609503.92	1374609504.08	110	5	8613674936776	460003460238162	3595900488375164	2	460	0	14306			20193	10.184.41.121	123.125.114.195	45926	80	6	cmnet	1	221.177.156.14	221.177.217.145	221.177.156.14	221.177.217.155	mobads-logs.baidu.com	http://mobads-logs.baidu.com/dc?type=20&st=pv&rnd=0.9601120455190539		Mozilla/5.0 (Linux; U; Android 2.3.4; zh-CN; ST18i Build/4.0.2.A.0.62) AppleWebKit/534.31 (KHTML, like Gecko) UCBrowser/9.1.1.309 U3/0.8.0 Mobile Safari/534.31	GET	200	1317	291	7	2	0	0	7	2	0	0	0	0	http://mobads-logs.baidu.com/dc?type=20&st=pv&rnd=0.9601120455190539	5903902610650624011	5903902862210048011	5966330";
		String[] fields = StringUtils.split(ss,"\t");
		System.out.println(fields[26]);
		System.out.println(fields[30]);
		
	}
	
}
