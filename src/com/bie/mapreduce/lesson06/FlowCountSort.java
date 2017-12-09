package com.bie.mapreduce.lesson06;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/** 
* @author  Author:������ 
* @date Date:2017��12��9�� ����11:56:50 
*
* 1:����������Ϊkey,�ֻ�������Ϊvalue
*/
public class FlowCountSort {

	/***
	 * ����һ:�����ļ�
	 * ������:�ֻ�����
	 * ������:FlowBean
	 * ������:�ֻ�����
	 * @author bhlgo
	 *
	 */
	static class FlowCountSortMapper extends Mapper<LongWritable, Text, FlowBean, Text>{
		
		FlowBean bean = new FlowBean();
		Text value = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			//�õ���һ������ͳ�Ƴ����������
			String line = value.toString();
			//����tab���н�ȡ����
			String[] fields = line.split("/t");
			//��ȡ�绰����
			String phoneNumber = fields[0];
			//��ȡ��������
			long upFlow = Long.parseLong(fields[1]);
			//��ȡ��������
			long downFlow = Long.parseLong(fields[2]);
			
			//�˲���ֻ����һ����,����ʡ����Դ
			bean.setBean(upFlow, downFlow);
			value.set(phoneNumber);
			
			//��������뵽Map���л���
			context.write(bean, value);
		}
		
	}
	
	/***
	 * ����key����, ���������Ƕ���, ÿ�������ǲ�һ����, ����ÿ�����󶼵���һ��reduce����
	 * ����һ:FlowBean,����
	 * ������:�ֻ�����
	 * ������:�ֻ�����
	 * ������:����ͳ��
	 * @author bhlgo
	 *
	 */
	static class FlowCountSortReducer extends Reducer<FlowBean, Text, Text, FlowBean>{
		@Override
		protected void reduce(FlowBean bean, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			//�����ֱ�����뵽�ļ�����,��������������
			Text value = values.iterator().next();
			context.write(value, bean);
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
		/*conf.set("mapreduce.framework.name", "yarn");
		conf.set("yarn.resoucemanager.hostname", "mini1");*/
		Job job = Job.getInstance(conf);
		
		/*job.setJar("/home/hadoop/wc.jar");*/
		//ָ���������jar�����ڵı���·��
		job.setJarByClass(FlowCountSort.class);
		
		//ָ����ҵ��jobҪʹ�õ�mapper/Reducerҵ����
		job.setMapperClass(FlowCountSortMapper.class);
		job.setReducerClass(FlowCountSortReducer.class);
		
		//ָ��mapper������ݵ�kv����
		job.setMapOutputKeyClass(FlowBean.class);
		job.setMapOutputValueClass(Text.class);
		
		//ָ��������������ݵ�kv����
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		//ָ��job������ԭʼ�ļ�����Ŀ¼
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		//ָ��job������������Ŀ¼
		Path outPath = new Path(args[1]);
		/*FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outPath)){
			fs.delete(outPath, true);
		}*/
		FileOutputFormat.setOutputPath(job, outPath);
		
		//��job�����õ���ز������Լ�job���õ�java�����ڵ�jar�����ύ��yarnȥ����
		/*job.submit();*/
		boolean res = job.waitForCompletion(true);
		System.exit(res?0:1);
	}
}
