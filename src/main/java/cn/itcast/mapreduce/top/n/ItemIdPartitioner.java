package cn.itcast.mapreduce.top.n;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;


public class ItemIdPartitioner extends Partitioner<OrderBean, NullWritable>{

	@Override
	public int getPartition(OrderBean key, NullWritable value, int numPartitions) {
		return (key.getItemid().hashCode() & Integer.MAX_VALUE) % numPartitions;
	}

}
