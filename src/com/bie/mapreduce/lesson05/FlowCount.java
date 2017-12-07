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
* @author  Author:别先生 
* @date Date:2017年12月3日 下午9:35:55 
*
* 1:流量统计的练习
* 2:[root@master data_hadoop]# hadoop jar flowBean.jar com.bie.mapreduce.lesson05.FlowCount /flowSum/input /flowSum/output 

*/
public class FlowCount {

	//静态内部类
	static class FlowCountMapper extends Mapper<LongWritable, Text, Text, FlowBean>{
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			//将一行内容转成String类型的.
			String line = value.toString();
			//切分字段
			String[] fields = line.split("\t");
			//String[] fields = line.split("	");
			//取出手机号
			String phone = fields[1];
			//取出上行流量和下行流量
			long upFlow = Long.parseLong(fields[fields.length-3]);
			long downFlow = Long.parseLong(fields[fields.length-2]);
			//输出操作
			context.write(new Text(phone), new FlowBean(upFlow, downFlow));
		}
	}
	
	static class FlowCountReducer extends Reducer<Text, FlowBean, Text, FlowBean>{
		@Override
		protected void reduce(Text key, Iterable<FlowBean> values, Context context)
				throws IOException, InterruptedException {
			long upFlowSum = 0;
			long downFlowSum = 0;
			//遍历锁所有的bean,将其中的上行流量，下行流量分别累加。
			for(FlowBean bean : values){
				upFlowSum += bean.getUpFlow();
				downFlowSum += bean.getDownFlow();
			}
			
			FlowBean flowBean = new FlowBean(upFlowSum,downFlowSum);
			context.write(key, flowBean);
		}
	}
	
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		//配置相应的参数conf
		Configuration conf = new Configuration();
		/*conf.set("HADOOP_USER_NAME", "root");
		conf.set("dfs.permissions.enabled", "false");*/
		
		/*conf.set("mapreduce.framework.name", "yarn");
		conf.set("yarn.resoucemanager.hostname", "master");*/
		
		Job job = Job.getInstance(conf);
		
		//指定本程序的jar包所在的本地路径
		job.setJarByClass(FlowBean.class);
		
		//指定本业务job要使用的mapper/Reducer业务类
		job.setMapperClass(FlowCountMapper.class);
		job.setReducerClass(FlowCountReducer.class);
		
		
		//指定我们自定义的数据分区器ProvincePartitioner
		job.setPartitionerClass(ProvincePartitioner.class);
		//同时指定相应“分区”数量的reducetask
		job.setNumReduceTasks(5);
		
		//指定mapper输出数据的key-value类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(FlowBean.class);
		
		//指定最终输出的数据的key-value类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		//指定job的输入原始文件所在目录
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		//将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行
		//方式一
		//job.submit();
		
		//方式二,true表示将集群的信息打印出来.
		boolean flag = job.waitForCompletion(true);
		System.exit(flag?0:1);
	}
}
