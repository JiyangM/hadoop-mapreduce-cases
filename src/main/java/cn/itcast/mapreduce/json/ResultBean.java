package cn.itcast.mapreduce.json;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class ResultBean implements WritableComparable<ResultBean>{

	private Long movie;
	private Long sumRate;
	
	public void setSumRate(long sumRate){
		this.sumRate = sumRate;
	}

	public Long getMovie() {
		return movie;
	}

	public void setMovie(Long movie) {
		this.movie = movie;
	}

	

	public ResultBean(Long movie,Long sumRate) {
		this.movie = movie;
		this.sumRate = sumRate;
	}

	public ResultBean() {
		// TODO Auto-generated constructor stub
	}

	@Override
	public int compareTo(ResultBean o) {
		if(this.movie-o.movie!=0){
			return (int) (this.movie-o.movie);
		}
		return (int) (o.sumRate - this.sumRate);
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(movie);
		out.writeLong(sumRate);
	}
	
	
	
	

	public ResultBean(Long sumRate) {
		super();
		this.sumRate = sumRate;
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.movie = in.readLong();
		this.sumRate = in.readLong();
	}
	
	@Override
	public String toString() {
		//return movie + "\t" + sumRate;
		return movie + "\t" +sumRate;
	}
	
}
