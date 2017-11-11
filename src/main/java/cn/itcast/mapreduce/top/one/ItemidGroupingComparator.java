package cn.itcast.mapreduce.top.one;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 *
 */
public class ItemidGroupingComparator extends WritableComparator {

	protected ItemidGroupingComparator() {

		super(OrderBean.class, true);
	}
	

	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		OrderBean abean = (OrderBean) a;
		OrderBean bbean = (OrderBean) b;
		
		//��item_id��ͬ��bean����Ϊ��ͬ���Ӷ�ۺ�Ϊһ��
		return abean.getItemid().compareTo(bbean.getItemid());
		
	}
	
	

}
