package com.bie.lesson02;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Test;

/** 
* @author  Author:������ 
* @date Date:2017��11��26�� ����11:23:11 
*
* 1:�����ķ�ʽ������hdfs�ϵ��ļ���
* 	����ʵ�ֶ�ȡָ��ƫ������Χ�����ݡ�
* 
*/
public class HdfsStreamAccess {

	Configuration conf = null;
	FileSystem fs = null;
	
	@Before
	public void init() throws IOException, InterruptedException, URISyntaxException{
		// ����һ�����ò�����������һ������������Ҫ���ʵ�hdfs��URI
		// �Ӷ�FileSystem.get()������֪��Ӧ����ȥ����һ������hdfs�ļ�ϵͳ�Ŀͻ��ˣ��Լ�hdfs�ķ��ʵ�ַ
		// new Configuration();��ʱ�����ͻ�ȥ����jar���е�hdfs-default.xml
		// Ȼ���ټ���classpath�µ�hdfs-site.xml
		conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		
		
		//�������ȼ��� 
		//	1���ͻ��˴��������õ�ֵ 
		//	2��classpath�µ��û��Զ��������ļ�
		// 	3��Ȼ���Ƿ�������Ĭ������
		conf.set("dfs.replication", "3");
		
		
		//�õ�һ���ļ�ϵͳ�����Ŀͻ���ʵ������
		// ��ȡһ��hdfs�ķ��ʿͻ��ˣ����ݲ��������ʵ��Ӧ����DistributedFileSystem��ʵ��
		//��ʽһ
		//fs = FileSystem.get(conf);
		
		//��ʽ��
		// �������ȥ��ȡ����conf����Ϳ��Բ�Ҫ��"fs.defaultFS"������
		//���ң�����ͻ��˵���ݱ�ʶ�Ѿ���hadoop�û�
		//Ҳ����ֱ�Ӵ���uri���û�����
		fs = FileSystem.get(new URI("hdfs://master:9000"), conf, "root");
	}
	
	
	//ͨ�����ķ�ʽ�ϴ��ļ�
	@Test
	public void uploadFile() throws IllegalArgumentException, IOException{
		//����2:���ǲ�����
		FSDataOutputStream fos = fs.create(new Path("/stream.txt"), true);
		FileInputStream fis = new FileInputStream("C:/Users/bhlgo/Desktop/������ѧϰ.txt");
		//����1:�������ļ�,input����
		//����2:дhdfs,output��д��
		IOUtils.copy(fis, fos);
	}
	
	//ͨ�����ķ�ʽ��hdfs���������ļ���
	@Test
	public void downloadFile() throws IllegalArgumentException, IOException{
		//�Ȼ�ȡһ���ļ���������----���hdfs�ϵ�
		//input���뼴��������
		FSDataInputStream fis = fs.open(new Path("/stream.txt"));
		
		//�ٹ���һ���ļ��������----��Ա��ص�
		//output�����д
		FileOutputStream fos = new FileOutputStream("C:/Users/bhlgo/Desktop/stream.txt");
		
		//�ٽ������������ݴ��䵽�����
		//��������
		IOUtils.copy(fis, fos);
	}
	
	
	//hdfs֧�������λ�����ļ���ȡ�����ҿ��Է���ض�ȡָ������
	//	�����ϲ�ֲ�ʽ�����ܲ�����������
	//�ļ��������д����
	@Test
	public void randomAccess() throws IllegalArgumentException, IOException{
		//�Ȼ�ȡһ���ļ���������----���hdfs�ϵ�
		//input���뼴��
		FSDataInputStream fis = fs.open(new Path("/������ѧϰ.txt"));
		
		//���Խ�������ʼƫ���������Զ���
		//seek�����Ƕ�����ʼλ�ö�λ5
		fis.seek(5);
		
		//output�����д
		FileOutputStream fos = new FileOutputStream("C:/Users/bhlgo/Desktop/output.txt");
		
		//��������
		IOUtils.copy(fis, fos);
	}

	
	//��ʾhdfs���ļ�������
	@Test
	public void catFile() throws IllegalArgumentException, IOException{
		FSDataInputStream fis = fs.open(new Path("/������ѧϰ.txt"));
		
		IOUtils.copy(fis, System.out);
		
		//IOUtils.copyBytes(fis, System.out, 1024);
	}
	
}
