package com.bie.lesson01;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

/** 
* @author  Author:������ 
* @date Date:2017��11��25�� ����3:10:44 
*
* 1:Ѱ�ҵ�jar������λ��:hadoop-2.6.4\share\hadoop\hdfs
*  	common������ļ���/common/lib:
*  		hadoop-common-2.6.4.jar
*   hdfs�İ�����/hdfs/lib:
*   	hadoop-hdfs-2.6.4.jar
* 2:�ͻ���ȥ����hdfsʱ������һ���û���ݵ�
* 	Ĭ������£�hdfs�ͻ���api���jvm�л�ȡһ����������Ϊ�Լ����û����:
* 		-DHADOOP_USER_NAME=hadoop
* 	Ҳ�����ڹ���ͻ���fs����ʱ��ͨ���������ݽ�ȥ 
* 3:�ǵ����ñ���Hadoop_home��path�Ļ�������;
* 4:������Ҫ�ģ�Ҳ�����鷳���ǰ汾���⡣���ӹ��������ѹ������ѹ���Ժ󣬽���ȷ��bin��lib�滻����
* 	�ײ⣬�ϴ�ò�Ʋ��Ǻ������Ӱ�죬�������ػ���Ӱ�졣
* 5:��Hadoop��binĿ¼�·�winutils.exe���ڻ������������� HADOOP_HOME,
* 	��hadoop.dll������C:\Windows\System32���漴�� 
*/
public class HdfsClientTest {

	Configuration conf = null;
	FileSystem fs = null;
	@Before
	public void init() throws Exception{
		conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		//�õ�һ���ļ�ϵͳ�����Ŀͻ���ʵ������
		//��ʽһ:
		//fs = FileSystem.get(conf);
		
		
		//��ʽ��:Ҳ�����ڹ���ͻ���fs����ʱ��ͨ���������ݽ�ȥ
		//�õ�һ���ļ�ϵͳ�����Ŀͻ���ʵ������
		//����ֱ�Ӵ��� uri���û����
		//���һ������Ϊ�û���
		fs = FileSystem.get(new URI("hdfs://master:9000"),conf,"root");
	}
	
	//�ļ��ϴ�
	@Test
	public void uploadFile() throws IllegalArgumentException, IOException, InterruptedException{
		//����2s
		//Thread.sleep(2000);
		//�����ϴ��ļ�
		fs.copyFromLocalFile(new Path("C:/Users/bhlgo/Desktop/������ѧϰ.txt"), 
				new Path("/������ѧϰ3.txt"));
		//�ر�fs
		fs.close();
	}
	
	@Test
	public void downloadFile() throws IllegalArgumentException, IOException{
		//��hdfs�������ļ�
		fs.copyToLocalFile(new Path("/hello.txt"), new Path("C:/Users/bhlgo/Desktop/"));
		//�ر�fs
		fs.close();
	}
}
