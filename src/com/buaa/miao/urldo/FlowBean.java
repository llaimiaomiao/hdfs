package com.buaa.miao.urldo;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class FlowBean implements WritableComparable<FlowBean>{
	private long up_flow;
	private long d_flow;
	private long sum_flow;
	
	public FlowBean() {
	}
	
	public FlowBean(long up_flow, long d_flow) {
		this.up_flow = up_flow;
		this.d_flow = d_flow;
		this.sum_flow = up_flow+d_flow;
	}
	private void set(long up_flow, long d_flow) {
		this.up_flow = up_flow;
		this.d_flow = d_flow;
		this.sum_flow = up_flow+d_flow;
	}
	public long getUp_flow() {
		return up_flow;
	}
	public void setUp_flow(long up_flow) {
		this.up_flow = up_flow;
	}
	public long getD_flow() {
		return d_flow;
	}
	public void setD_flow(long d_flow) {
		this.d_flow = d_flow;
	}
	public long getSum_flow() {
		return sum_flow;
	}
	public void setSum_flow(long sum_flow) {
		this.sum_flow = sum_flow;
	}
	@Override
	public String toString() {
		return up_flow+ "  " + d_flow+ "  "+ sum_flow;
	}
	@Override
	public void readFields(DataInput in) throws IOException {
		up_flow = in.readLong();
		d_flow = in.readLong();
		sum_flow = in.readLong();
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeLong(up_flow);
		out.writeLong(d_flow);
		out.writeLong(sum_flow);
		
	}

	@Override
	public int compareTo(FlowBean o) {
		return sum_flow>o.sum_flow? -1: 1;
	}

}
