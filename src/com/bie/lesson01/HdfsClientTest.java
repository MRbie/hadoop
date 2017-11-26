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
* @author  Author:别先生 
* @date Date:2017年11月25日 下午3:10:44 
*
* 1:寻找的jar包所在位置:hadoop-2.6.4\share\hadoop\hdfs
*  	common包下面的及其/common/lib:
*  		hadoop-common-2.6.4.jar
*   hdfs的包及其/hdfs/lib:
*   	hadoop-hdfs-2.6.4.jar
* 2:客户端去操作hdfs时，是有一个用户身份的
* 	默认情况下，hdfs客户端api会从jvm中获取一个参数来作为自己的用户身份:
* 		-DHADOOP_USER_NAME=hadoop
* 	也可以在构造客户端fs对象时，通过参数传递进去 
* 3:记得配置本地Hadoop_home和path的环境变量;
* 4:最最重要的，也是最麻烦就是版本问题。将从官网下面的压缩包解压缩以后，将正确的bin和lib替换掉。
* 	亲测，上传貌似不是很受这个影响，但是下载会受影响。
* 5:在Hadoop的bin目录下放winutils.exe，在环境变量中配置 HADOOP_HOME,
* 	把hadoop.dll拷贝到C:\Windows\System32下面即可 
* 6:hdfs dfsadmin -report
* 		打印集群的状态，比浏览器页面查看的准确。
*/
public class HdfsClientTest {

	Configuration conf = null;
	FileSystem fs = null;
	@Before
	public void init() throws Exception{
		conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		//拿到一个文件系统操作的客户端实例对象
		//方式一:
		//fs = FileSystem.get(conf);
		
		
		//方式二:也可以在构造客户端fs对象时，通过参数传递进去
		//拿到一个文件系统操作的客户端实例对象
		//可以直接传入 uri和用户身份
		//最后一个参数为用户名
		fs = FileSystem.get(new URI("hdfs://master:9000"),conf,"root");
	}
	
	//文件上传
	@Test
	public void uploadFile() throws IllegalArgumentException, IOException, InterruptedException{
		//休眠2s
		//Thread.sleep(2000);
		//本地上传文件
		fs.copyFromLocalFile(new Path("C:/Users/bhlgo/Desktop/大数据学习.txt"), 
				new Path("/大数据学习3.txt"));
		//关闭fs
		fs.close();
	}
	
	@Test
	public void downloadFile() throws IllegalArgumentException, IOException{
		//从hdfs上下载文件
		fs.copyToLocalFile(new Path("/hello.txt"), new Path("C:/Users/bhlgo/Desktop/"));
		//关闭fs
		fs.close();
	}
	
	//获取到conf的内容
	@Test
	public void getConf(){
		Iterator<Entry<String, String>> it = conf.iterator();
		while(it.hasNext()){
			Entry<String, String> entry = it.next();
			//conf加载的内容
			System.out.println(entry.getKey() + " ......" + entry.getValue());
		}
	}
	
	//创建文件夹
	@Test
	public void mkdirFile() throws IllegalArgumentException, IOException{
		boolean mkdirs = fs.mkdirs(new Path("/aaa"));	
		if(mkdirs){
			System.out.println("文件夹创建成功......");
		}else{
			System.out.println("文件夹创建失败......");
		}
	}
	
	//文件的删除
	@Test
	public void deleteFile() throws IllegalArgumentException, IOException{
		//true， 递归删除
		boolean delete = fs.delete(new Path("/aaa"),true);
		if(delete){
			System.out.println("文件删除成功......");
		}else{
			System.out.println("文件删除失败......");
		}
	}
	
	//列出文件
	@Test
	public void listFile() throws FileNotFoundException, IllegalArgumentException, IOException{
		//列出文件的信息状态
		FileStatus[] status = fs.listStatus(new Path("/"));
		for(FileStatus fs : status){
			System.out.println("长度:" + fs.getLen() + ",路径:"+ fs.getPath() + ",内容:" + fs.toString());
		}
		
		System.out.println("=================================");
		//遍历出文件
		RemoteIterator<LocatedFileStatus> iterator = fs.listFiles(new Path("/"), true);
		while(iterator.hasNext()){
			LocatedFileStatus lfs = iterator.next();
			String name = lfs.getPath().getName();
			Path path = lfs.getPath();
			System.out.println("名称:" + name +",路径:" + path);
		}
	}
	
	
	//递归列出指定目录下面的所有的子文件夹中的文件
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
				System.out.println("块起始偏移量: " +b.getOffset());
				System.out.println("块长度:" + b.getLength());
				//块所在的datanode节点
				String[] datanodes = b.getHosts();
				for(String dn:datanodes){
					System.out.println("datanode:" + dn);
				}
			}
		}
	}
	
	//判断是文件夹还是目录
	@Test
	public void isFileOrDirectory() throws Exception {
		FileStatus[] listStatus = fs.listStatus(new Path("/"));
		for(FileStatus file :listStatus){
			
			System.out.println("name: " + file.getPath().getName());
			System.out.println((file.isFile()?"file":"directory"));
		}
	}
	
	
}
