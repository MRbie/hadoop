package com.bie.mapreduce.lesson04;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/** 
* @author  Author:别先生 
* @date Date:2017年11月27日 下午9:31:41 
*
* 1:KEYIN, VALUEIN 对应  mapper输出的KEYOUT,VALUEOUT类型对应
* 2:KEYOUT, VALUEOUT 是自定义reduce逻辑处理结果的输出数据类型
* 3:KEYOUT是单词,VLAUEOUT是总次数
*/
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{ 
 
	/**
	 * 入参key，是一组相同单词kv对的key
	 */
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		int count = 0;
		//迭代器遍历
		/*Iterator<LongWritable> iterator = values.iterator();
		while(iterator.hasNext()){
			LongWritable next = iterator.next();
			long middle = next.get();
			count += middle;
		}*/
		
		for(IntWritable value : values){
			count += value.get();
		}
		context.write(key, new IntWritable(count));
	
	}
}
