package com.bie.mapreduce.lesson04;

/** 
* @author  Author:������ 
* @date Date:2017��11��27�� ����9:31:22 
*
* 1:����İ�����
* 	1):hadoop-2.6.4\share\hadoop\common�µ�
* 		hadoop-common-2.6.4.jar
* 		����lib���������
* 	2):hadoop-2.6.4\share\hadoop\hdfs
* 		hadoop-hdfs-2.6.4.jar
* 		����lib���������
* 	3):hadoop-2.6.4\share\hadoop\mapreduce
* 		����:hadoop-mapreduce-examples-2.6.4.jar
* 		����lib���������
* 	4):hadoop-2.6.4\share\hadoop\yarn
* 		����server�İ�
* 		����lib��������С�
* 2:Mapreduce��һ���ֲ�ʽ�������ı�̿��,���û�����������hadoop�����ݷ���Ӧ�á��ĺ��Ŀ��;
* 	Mapreduce���Ĺ����ǽ��û���д��ҵ���߼�������Դ�Ĭ��������ϳ�һ�������ķֲ�ʽ�������,
* 	����������һ��hadoop��Ⱥ��;	
* 3:Mapper��Ĳ�������:
* 	1):keyin: Ĭ������£���mr�����������һ���ı�����ʼƫ������Long,
* 			������hadoop�����Լ��ĸ���������л��ӿڣ����Բ�ֱ����Long������LongWritable
*  	   valuein:Ĭ������£���mr�����������һ���ı������ݣ�String��ͬ�ϣ���Text
*  2):keyout�����û��Զ����߼��������֮����������е�key���ڴ˴��ǵ��ʣ�String��ͬ�ϣ���Text
*  	  valueout�����û��Զ����߼��������֮����������е�value���ڴ˴��ǵ��ʴ�����Integer��ͬ�ϣ���IntWritable
* 4:mapreduce��ܽṹ���������л��� 
* 	һ��������mapreduce�����ڷֲ�ʽ����ʱ������ʵ������;
* 	1)��MRAppMaster��������������Ĺ��̵��ȼ�״̬Э��
* 	2)��mapTask������map�׶ε��������ݴ�������
* 	3)��ReduceTask������reduce�׶ε��������ݴ�������
*  
*/
public class WordCountMapper{

	
}
