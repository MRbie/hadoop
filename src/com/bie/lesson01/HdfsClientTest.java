package com.bie.lesson01;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.util.Iterator;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
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
* 6:hdfs dfsadmin -report
* 		��ӡ��Ⱥ��״̬���������ҳ��鿴��׼ȷ��
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
	
	//��ȡ��conf������
	@Test
	public void getConf(){
		Iterator<Entry<String, String>> it = conf.iterator();
		while(it.hasNext()){
			Entry<String, String> entry = it.next();
			//conf���ص�����
			System.out.println(entry.getKey() + " ......" + entry.getValue());
		}
	}
	
	//�����ļ���
	@Test
	public void mkdirFile() throws IllegalArgumentException, IOException{
		boolean mkdirs = fs.mkdirs(new Path("/aaa"));	
		if(mkdirs){
			System.out.println("�ļ��д����ɹ�......");
		}else{
			System.out.println("�ļ��д���ʧ��......");
		}
	}
	
	//�ļ���ɾ��
	@Test
	public void deleteFile() throws IllegalArgumentException, IOException{
		//true�� �ݹ�ɾ��
		boolean delete = fs.delete(new Path("/aaa"),true);
		if(delete){
			System.out.println("�ļ�ɾ���ɹ�......");
		}else{
			System.out.println("�ļ�ɾ��ʧ��......");
		}
	}
	
	//�г��ļ�
	@Test
	public void listFile() throws FileNotFoundException, IllegalArgumentException, IOException{
		//�г��ļ�����Ϣ״̬
		FileStatus[] status = fs.listStatus(new Path("/"));
		for(FileStatus fs : status){
			System.out.println("����:" + fs.getLen() + ",·��:"+ fs.getPath() + ",����:" + fs.toString());
		}
		
		System.out.println("=================================");
		//�������ļ�
		RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path("/"), true);
		while(iterator.hasNext()){
			LocatedFileStatus lfs = iterator.next();
			String name = lfs.getPath().getName();
			Path path = lfs.getPath();
			System.out.println("����:" + name +",·��:" + path);
		}
	}
	
	
	//�ݹ��г�ָ��Ŀ¼��������е����ļ����е��ļ�
	@Test
	public void getFileAndSonFile() throws FileNotFoundException, IllegalArgumentException, IOException{
		RemoteIterator<LocatedFileStatus> listFiles = fs.listFiles(new Path("/"), true);
		while(listFiles.hasNext()){
			LocatedFileStatus fileStatus = listFiles.next();
			System.out.println("blocksize: " +fileStatus.getBlockSize());
			System.out.println("owner: " +fileStatus.getOwner());
			System.out.println("Replication: " +fileStatus.getReplication());
			System.out.println("Permission: " +fileStatus.getPermission());
			System.out.println("Name: " +fileStatus.getPath().getName());
			System.out.println("=================================");
			BlockLocation[] blockLocations = fileStatus.getBlockLocations();
			for(BlockLocation b:blockLocations){
				System.out.println("����ʼƫ����: " +b.getOffset());
				System.out.println("�鳤��:" + b.getLength());
				//�����ڵ�datanode�ڵ�
				String[] datanodes = b.getHosts();
				for(String dn:datanodes){
					System.out.println("datanode:" + dn);
				}
			}
		}
	}
	
	//�ж����ļ��л���Ŀ¼
	@Test
	public void isFileOrDirectory() throws Exception {
		FileStatus[] listStatus = fs.listStatus(new Path("/"));
		for(FileStatus file :listStatus){
			
			System.out.println("name: " + file.getPath().getName());
			System.out.println((file.isFile()?"file":"directory"));
		}
	}
	
	
}
