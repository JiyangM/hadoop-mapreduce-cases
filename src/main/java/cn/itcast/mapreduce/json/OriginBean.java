package cn.itcast.mapreduce.json;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.WritableComparable;

public class OriginBean implements WritableComparable<OriginBean> {

	private Long movie;

	private Long rate;

	private Long timeStamp;

	private Long uid;


	public Long getMovie() {
		return movie;
	}

	public void setMovie(Long movie) {
		this.movie = movie;
	}

	public Long getRate() {
		return rate;
	}

	public void setRate(Long rate) {
		this.rate = rate;
	}

	public Long getTimeStamp() {
		return timeStamp;
	}

	public void setTimeStamp(Long timeStamp) {
		this.timeStamp = timeStamp;
	}

	public Long getUid() {
		return uid;
	}

	public void setUid(Long uid) {
		this.uid = uid;
	}

	public OriginBean(Long movie, Long rate, Long timeStamp, Long uid) {
		this.movie = movie;
		this.rate = rate;
		this.timeStamp = timeStamp;
		this.uid = uid;
	}

	public OriginBean() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public int compareTo(OriginBean o) {
		return this.movie.compareTo(o.movie);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(movie);
		out.writeLong(rate);
		out.writeLong(timeStamp);
		out.writeLong(uid);
	}


	@Override
	public void readFields(DataInput in) throws IOException {
		this.movie = in.readLong();
		this.rate = in.readLong();
		this.timeStamp = in.readLong();
		this.uid = in.readLong();
	}

	@Override
	public String toString() {
		return this.movie + "\t" + this.rate +"\t"+ this.timeStamp + "\t" + this.uid;
	}

}
