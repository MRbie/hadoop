package com.bie.mapreduce.lesson04;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/** 
* @author  Author:������ 
* @date Date:2017��11��27�� ����9:31:41 
*
* 1:KEYIN, VALUEIN ��Ӧ  mapper�����KEYOUT,VALUEOUT���Ͷ�Ӧ
* 2:KEYOUT, VALUEOUT ���Զ���reduce�߼��������������������
* 3:KEYOUT�ǵ���,VLAUEOUT���ܴ���
*/
public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable>{ 
 
	/**
	 * ���key����һ����ͬ����kv�Ե�key
	 */
	@Override
	protected void reduce(Text key, Iterable<IntWritable> values,
			Context context) throws IOException, InterruptedException {
		int count = 0;
		//����������
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
