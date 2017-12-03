package com.bie.mapreduce.lesson04;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/** 
* @author  Author:������ 
* @date Date:2017��12��3�� ����4:57:57 
*
* 1:�൱��һ��yarn��Ⱥ�Ŀͻ���
* 	��Ҫ�ڴ˷�װ���ǵ�mr�����������в�����ָ��jar��
* 	����ύ��yarn
*/
public class WordCountDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		//������Ӧ�Ĳ���conf
		Configuration conf = new Configuration();
		/*conf.set("HADOOP_USER_NAME", "root");
		conf.set("dfs.permissions.enabled", "false");*/
		
		/*conf.set("mapreduce.framework.name", "yarn");
		conf.set("yarn.resoucemanager.hostname", "master");*/
		
		Job job = Job.getInstance(conf);
		
		//ָ���������jar�����ڵı���·��
		job.setJarByClass(WordCountDriver.class);
		
		//ָ����ҵ��jobҪʹ�õ�mapper/Reducerҵ����
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
		//ָ��mapper������ݵ�key-value����
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//ָ��������������ݵ�key-value����
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
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
