package com.bie.mapreduce.lesson05;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/** 
* @author  Author:别先生 
* @date Date:2017年12月3日 下午9:38:52 
*
*
*/
public class FlowBean implements Writable{

	private long upFlow;//上行流量
	private long downFlow;//下行流量
	private long sumFlow;//总流量
	
	//反序列化时，需要反射调用空参构造函数，所以要显示定义一个
	public FlowBean() {}
	
	//构造函数
	public FlowBean(long upFlow, long downFlow) {
		this.upFlow = upFlow;
		this.downFlow = downFlow;
		this.sumFlow = upFlow + downFlow;
	}


	public long getUpFlow() {
		return upFlow;
	}
	public void setUpFlow(long upFlow) {
		this.upFlow = upFlow;
	}
	public long getDownFlow() {
		return downFlow;
	}
	public void setDownFlow(long downFlow) {
		this.downFlow = downFlow;
	}
	public long getSumFlow() {
		return sumFlow;
	}
	public void setSumFlow(long sumFlow) {
		this.sumFlow = sumFlow;
	}


	//反序列化方法,注意：反序列化的顺序跟序列化的顺序完全一致
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		upFlow = dataInput.readLong();
		downFlow = dataInput.readLong();
		sumFlow = dataInput.readLong();
	}
	
	//序列化方法
	@Override
	public void write(DataOutput dataOutput) throws IOException {
		dataOutput.writeLong(upFlow);
		dataOutput.writeLong(downFlow);
		dataOutput.writeLong(sumFlow);
	}

	@Override
	public String toString() {
		return "FlowBean [upFlow=" + upFlow + ", downFlow=" + downFlow + ", sumFlow=" + sumFlow + "]";
	}
	
	
}
