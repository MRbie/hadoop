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
* @author  Author:别先生 
* @date Date:2017年12月3日 下午4:57:57 
*
* 1:相当于一个yarn集群的客户端
* 	需要在此封装我们的mr程序的相关运行参数，指定jar包
* 	最后提交给yarn
*/
public class WordCountDriver {

	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		//配置相应的参数conf
		Configuration conf = new Configuration();
		/*conf.set("HADOOP_USER_NAME", "root");
		conf.set("dfs.permissions.enabled", "false");*/
		
		/*conf.set("mapreduce.framework.name", "yarn");
		conf.set("yarn.resoucemanager.hostname", "master");*/
		
		Job job = Job.getInstance(conf);
		
		//指定本程序的jar包所在的本地路径
		job.setJarByClass(WordCountDriver.class);
		
		//指定本业务job要使用的mapper/Reducer业务类
		job.setMapperClass(WordCountMapper.class);
		job.setReducerClass(WordCountReducer.class);
		
		//指定mapper输出数据的key-value类型
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		
		//指定最终输出的数据的key-value类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
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
