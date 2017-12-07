package com.bie.mapreduce.lesson05;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.yarn.webapp.hamlet.HamletSpec.Flow;

import com.bie.mapreduce.lesson04.WordCountDriver;
import com.bie.mapreduce.lesson04.WordCountMapper;
import com.bie.mapreduce.lesson04.WordCountReducer;

/** 
* @author  Author:������ 
* @date Date:2017��12��3�� ����9:35:55 
*
* 1:����ͳ�Ƶ���ϰ
* 2:[root@master data_hadoop]# hadoop jar flowBean.jar com.bie.mapreduce.lesson05.FlowCount /flowSum/input /flowSum/output 

*/
public class FlowCount {

	//��̬�ڲ���
	static class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean>{
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			//��һ������ת��String���͵�.
			String line = value.toString();
			//�з��ֶ�
			String[] fields = line.split("\t");
			//String[] fields = line.split("	");
			//ȡ���ֻ���
			String phone = fields[1];
			//ȡ��������������������
			long upFlow = Long.parseLong(fields[fields.length-3]);
			long downFlow = Long.parseLong(fields[fields.length-2]);
			//�������
			context.write(new Text(phone), new FlowBean(upFlow, downFlow));
		}
	}
	
	static class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean>{
		@Override
		protected void reduce(Text key, Iterable<FlowBean> values, Context context)
				throws IOException, InterruptedException {
			long upFlowSum = 0;
			long downFlowSum = 0;
			//���������е�bean,�����е��������������������ֱ��ۼӡ�
			for(FlowBean bean : values){
				upFlowSum += bean.getUpFlow();
				downFlowSum += bean.getDownFlow();
			}
			
			FlowBean flowBean = new FlowBean(upFlowSum,downFlowSum);
			context.write(key, flowBean);
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		//������Ӧ�Ĳ���conf
		Configuration conf = new Configuration();
		/*conf.set("HADOOP_USER_NAME", "root");
		conf.set("dfs.permissions.enabled", "false");*/
		
		/*conf.set("mapreduce.framework.name", "yarn");
		conf.set("yarn.resoucemanager.hostname", "master");*/
		
		Job job = Job.getInstance(conf);
		
		//ָ���������jar�����ڵı���·��
		job.setJarByClass(FlowBean.class);
		
		//ָ����ҵ��jobҪʹ�õ�mapper/Reducerҵ����
		job.setMapperClass(FlowCountMapper.class);
		job.setReducerClass(FlowCountReducer.class);
		
		
		//ָ�������Զ�������ݷ�����ProvincePartitioner
		job.setPartitionerClass(ProvincePartitioner.class);
		//ͬʱָ����Ӧ��������������reducetask
		job.setNumReduceTasks(5);
		
		//ָ��mapper������ݵ�key-value����
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);
		
		//ָ��������������ݵ�key-value����
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		//ָ��job������ԭʼ�ļ�����Ŀ¼
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//��job�����õ���ز������Լ�job���õ�java�����ڵ�jar�����ύ��yarnȥ����
		//��ʽһ
		//job.submit();
		
		//��ʽ��,true��ʾ����Ⱥ����Ϣ��ӡ����.
		boolean flag = job.waitForCompletion(true);
		System.exit(flag?0:1);
	}
}
