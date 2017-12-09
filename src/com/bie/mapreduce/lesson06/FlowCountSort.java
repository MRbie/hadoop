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
* @author  Author:别先生 
* @date Date:2017年12月9日 上午11:56:50 
*
* 1:流量总量作为key,手机号码作为value
*/
public class FlowCountSort {

	/***
	 * 参数一:数据文件
	 * 参数二:手机号码
	 * 参数三:FlowBean
	 * 参数四:手机号码
	 * @author bhlgo
	 *
	 */
	static class FlowCountSortMapper extends Mapper<LongWritable, Text, FlowBean, Text>{
		
		FlowBean bean = new FlowBean();
		Text value = new Text();
		
		@Override
		protected void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			//拿到上一个汇总统计程序的输出结果
			String line = value.toString();
			//根据tab进行截取数据
			String[] fields = line.split("/t");
			//获取电话号码
			String phoneNumber = fields[0];
			//获取上行流量
			long upFlow = Long.parseLong(fields[1]);
			//获取下行流量
			long downFlow = Long.parseLong(fields[2]);
			
			//此操作只创建一个类,大大节省了资源
			bean.setBean(upFlow, downFlow);
			value.set(phoneNumber);
			
			//将结果输入到Map进行汇总
			context.write(bean, value);
		}
		
	}
	
	/***
	 * 根据key来掉, 传过来的是对象, 每个对象都是不一样的, 所以每个对象都调用一次reduce方法
	 * 参数一:FlowBean,流量
	 * 参数二:手机号码
	 * 参数三:手机号码
	 * 参数四:排序统计
	 * @author bhlgo
	 *
	 */
	static class FlowCountSortReducer extends Reducer<FlowBean, Text, Text, FlowBean>{
		@Override
		protected void reduce(FlowBean bean, Iterable<Text> values, Context context)
				throws IOException, InterruptedException {
			//将结果直接输入到文件里面,即可完成排序操作
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
		//指定本程序的jar包所在的本地路径
		job.setJarByClass(FlowCountSort.class);
		
		//指定本业务job要使用的mapper/Reducer业务类
		job.setMapperClass(FlowCountSortMapper.class);
		job.setReducerClass(FlowCountSortReducer.class);
		
		//指定mapper输出数据的kv类型
		job.setMapOutputKeyClass(FlowBean.class);
		job.setMapOutputValueClass(Text.class);
		
		//指定最终输出的数据的kv类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(FlowBean.class);
		
		//指定job的输入原始文件所在目录
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		
		//指定job的输出结果所在目录
		Path outPath = new Path(args[1]);
		/*FileSystem fs = FileSystem.get(conf);
		if(fs.exists(outPath)){
			fs.delete(outPath, true);
		}*/
		FileOutputFormat.setOutputPath(job, outPath);
		
		//将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行
		/*job.submit();*/
		boolean res = job.waitForCompletion(true);
		System.exit(res?0:1);
	}
}
