package com.bie.mapreduce.lesson05;

import java.util.HashMap;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

/** 
* @author  Author:������ 
* @date Date:2017��12��3�� ����10:00:52 
* 
* 1:K2  V2  ��Ӧ����map���kv������
*/
public class ProvincePartitioner extends Partitioner<Text, FlowBean>{

	public static HashMap<String, Integer> proviceDict = new HashMap<String, Integer>();
	static{
		proviceDict.put("136", 0);
		proviceDict.put("137", 1);
		proviceDict.put("138", 2);
		proviceDict.put("139", 3);
	}
	
	
	
	@Override
	public int getPartition(Text key, FlowBean value, int numPartitions) {
		String prefix = key.toString().substring(0, 3);
		Integer provinceId = proviceDict.get(prefix);
		
		return provinceId==null?4:provinceId;
	}


}
