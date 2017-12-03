package com.bie.mapreduce.lesson04;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/** 
* @author  Author:别先生 
* @date Date:2017年11月27日 下午9:31:22 
*
* 1:导入的包汇总
* 	1):hadoop-2.6.4\share\hadoop\common下的
* 		hadoop-common-2.6.4.jar
* 		及其lib下面的所有
* 	2):hadoop-2.6.4\share\hadoop\hdfs
* 		hadoop-hdfs-2.6.4.jar
* 		及其lib下面的所有
* 	3):hadoop-2.6.4\share\hadoop\mapreduce
* 		除了:hadoop-mapreduce-examples-2.6.4.jar
* 		及其lib下面的所有
* 	4):hadoop-2.6.4\share\hadoop\yarn
* 		除了server的包
* 		及其lib下面的所有。
* 2:Mapreduce是一个分布式运算程序的编程框架,是用户开发“基于hadoop的数据分析应用”的核心框架;
* 	Mapreduce核心功能是将用户编写的业务逻辑代码和自带默认组件整合成一个完整的分布式运算程序,
* 	并发运行在一个hadoop集群上;	
* 3:Mapper类的参数解释:
* 	1):keyin: 默认情况下，是mr框架所读到的一行文本的起始偏移量，Long,
* 			但是在hadoop中有自己的更精简的序列化接口，所以不直接用Long，而用LongWritable
*  	   valuein:默认情况下，是mr框架所读到的一行文本的内容，String，同上，用Text
*  2):keyout：是用户自定义逻辑处理完成之后输出数据中的key，在此处是单词，String，同上，用Text
*  	  valueout：是用户自定义逻辑处理完成之后输出数据中的value，在此处是单词次数，Integer，同上，用IntWritable
* 4:mapreduce框架结构及核心运行机制 
* 	一个完整的mapreduce程序在分布式运行时有三类实例进程;
* 	1)、MRAppMaster：负责整个程序的过程调度及状态协调
* 	2)、mapTask：负责map阶段的整个数据处理流程
* 	3)、ReduceTask：负责reduce阶段的整个数据处理流程
*  
*/
public class WordCountMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	/**
	* map阶段的业务逻辑就写在自定义的map()方法中
	* maptask会对每一行输入数据调用一次我们自定义的map()方法
	*/
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		//将maptask传给我们的文本内容先转换成String
		String line = value.toString();
		//根据空格将这一切切分成单词
		String[] words = line.split(" ");
		//将单词输出为<单词,1>
		for(String word : words){
			//将单词作为key,将次数1作为value,以便后续的数据分发,可以根据单词分发,以便于相同单词会到
			//相同的reduceTesk
			//write的类型就是Text,LongWritable
			context.write(new Text(word), new IntWritable(1));
		}
	}
	
}
