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
* @author  Author:别先生 
* @date Date:2017年12月16日 下午2:38:15 
*
* 1:处理原始日志，过滤出真实pv请求
* 2:转换时间格式
* 3:对缺失字段填充默认值
* 4:对记录标记valid和invalid
*/
public class WeblogPreProcess {

	//静态内部类
	static class WeblogPreProcessMapper extends Mapper<LongWritable, Text, Text, NullWritable>{
		//用来存储网站url分类数据
		Set<String> pages = new HashSet<String>();
		Text k = new Text();
		NullWritable v = NullWritable.get();
		
		//从外部加载网站url分类数据
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
			//获取到一行
			String line = value.toString();
			//解析日志字段
			WebLogBean webLogBean = WebLogParser.parser(line);
			//过滤js/图片/css等静态资源
			WebLogParser.filtStaticResource(webLogBean, pages);
			//如果不要这些，就直接返回
			/* if (!webLogBean.isValid()) return; */
			//将这些字段set一下，然后写出去即可，不需要reduce处理。
			k.set(webLogBean.toString());
			context.write(k, v);
		}
	}
	
	
	/**
	 * 主方法
	 * @param args
	 * @throws IOException
	 * @throws ClassNotFoundException
	 * @throws InterruptedException
	 */
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		//创建一个conf对象
		Configuration conf = new Configuration();
		//获取到一个job
		Job job = Job.getInstance(conf);
		//指定本程序的jar包所在的本地路径
		job.setJarByClass(WeblogPreProcess.class);
		//指定本业务job要使用的mapper/Reducer业务类
		job.setMapperClass(WeblogPreProcessMapper.class);
		//指定最终输出的数据的kv类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		//指定job的输入原始文件所在目录
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		//FileInputFormat.setInputPaths(job, new Path("c:/weblog/input"));
		//FileOutputFormat.setOutputPath(job, new Path("c:/weblog/output"));
		
		//设置一下Reduce的个数，这里没有用到，所以为0
		job.setNumReduceTasks(0);
		//将job中配置的相关参数，以及job所用的java类所在的jar包，提交给yarn去运行
		job.waitForCompletion(true);
	}
	
	
	
}
