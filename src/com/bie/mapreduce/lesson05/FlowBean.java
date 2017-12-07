package com.bie.mapreduce.lesson05;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;

/** 
* @author  Author:������ 
* @date Date:2017��12��3�� ����9:38:52 
*
*
*/
public class FlowBean implements Writable{

	private long upFlow;//��������
	private long downFlow;//��������
	private long sumFlow;//������
	
	//�����л�ʱ����Ҫ������ÿղι��캯��������Ҫ��ʾ����һ��
	public FlowBean() {}
	
	//���캯��
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


	//�����л�����,ע�⣺�����л���˳������л���˳����ȫһ��
	@Override
	public void readFields(DataInput dataInput) throws IOException {
		upFlow = dataInput.readLong();
		downFlow = dataInput.readLong();
		sumFlow = dataInput.readLong();
	}
	
	//���л�����
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
