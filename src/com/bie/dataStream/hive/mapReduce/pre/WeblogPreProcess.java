package com.bie.dataStream.hive.mapReduce.pre;


import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.bie.dataStream.hive.bean.WebLogBean;
import com.bie.dataStream.hive.bean.WebLogParser;

/** 
* @author  Author:������ 
* @date Date:2017��12��16�� ����2:38:15 
*
* 1:����ԭʼ��־�����˳���ʵpv����
* 2:ת��ʱ���ʽ
* 3:��ȱʧ�ֶ����Ĭ��ֵ
* 4:�Լ�¼���valid��invalid
*/
public class WeblogPreProcess {

	//��̬�ڲ���
	static class WeblogPreProcessMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
		//�����洢��վurl��������
		Set<String> pages = new HashSet<String>();
		Text k = new Text();
		NullWritable v = NullWritable.get();
		
		//���ⲿ������վurl��������
		@Override
		public void setup(Context context){
			pages.add("/about");
			pages.add("/black-ip-list/");
			pages.add("/cassandra-clustor/");
			pages.add("/finance-rhive-repurchase/");
			pages.add("/hadoop-family-roadmap/");
			pages.add("/hadoop-hive-intro/");
			pages.add("/hadoop-zookeeper-intro/");
			pages.add("/hadoop-mahout-roadmap/");
		}
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			//��ȡ��һ��
			String line = value.toString();
			//������־�ֶ�
			WebLogBean webLogBean = WebLogParser.parser(line);
			//����js/ͼƬ/css�Ⱦ�̬��Դ
			WebLogParser.filtStaticResource(webLogBean, pages);
			//�����Ҫ��Щ����ֱ�ӷ���
			/* if (!webLogBean.isValid()) return; */
			//����Щ�ֶ�setһ�£�Ȼ��д��ȥ���ɣ�����Ҫreduce����
			k.set(webLogBean.toString());
			context.write(k, v);
		}
	}
	
	
	/**
	 * ������
	 * @param args
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		//����һ��conf����
		Configuration conf = new Configuration();
		//��ȡ��һ��job
		Job job = Job.getInstance(conf);
		//ָ���������jar�����ڵı���·��
		job.setJarByClass(WeblogPreProcess.class);
		//ָ����ҵ��jobҪʹ�õ�mapper/Reducerҵ����
		job.setMapperClass(WeblogPreProcessMapper.class);
		//ָ��������������ݵ�kv����
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		//ָ��job������ԭʼ�ļ�����Ŀ¼
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//FileInputFormat.setInputPaths(job, new Path("c:/weblog/input"));
		//FileOutputFormat.setOutputPath(job, new Path("c:/weblog/output"));
		
		//����һ��Reduce�ĸ���������û���õ�������Ϊ0
		job.setNumReduceTasks(0);
		//��job�����õ���ز������Լ�job���õ�java�����ڵ�jar�����ύ��yarnȥ����
		job.waitForCompletion(true);
	}
	
	
	
}
