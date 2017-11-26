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
* @author  Author:别先生 
* @date Date:2017年11月26日 上午11:23:11 
*
* 1:用流的方式来操作hdfs上的文件。
* 	可以实现读取指定偏移量范围的数据。
* 
*/
public class HdfsStreamAccess {

	Configuration conf = null;
	FileSystem fs = null;
	
	@Before
	public void init() throws IOException, InterruptedException, URISyntaxException{
		// 构造一个配置参数对象，设置一个参数：我们要访问的hdfs的URI
		// 从而FileSystem.get()方法就知道应该是去构造一个访问hdfs文件系统的客户端，以及hdfs的访问地址
		// new Configuration();的时候，它就会去加载jar包中的hdfs-default.xml
		// 然后再加载classpath下的hdfs-site.xml
		conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://master:9000");
		
		
		//参数优先级： 
		//	1、客户端代码中设置的值 
		//	2、classpath下的用户自定义配置文件
		// 	3、然后是服务器的默认配置
		conf.set("dfs.replication", "3");
		
		
		//拿到一个文件系统操作的客户端实例对象
		// 获取一个hdfs的访问客户端，根据参数，这个实例应该是DistributedFileSystem的实例
		//方式一
		//fs = FileSystem.get(conf);
		
		//方式二
		// 如果这样去获取，那conf里面就可以不要配"fs.defaultFS"参数，
		//而且，这个客户端的身份标识已经是hadoop用户
		//也可以直接传入uri和用户姓名
		fs = FileSystem.get(new URI("hdfs://master:9000"), conf, "root");
	}
	
	
	//通过流的方式上传文件
	@Test
	public void uploadFile() throws IllegalArgumentException, IOException{
		//参数2:覆盖操作。
		FSDataOutputStream fos = fs.create(new Path("/stream.txt"), true);
		FileInputStream fis = new FileInputStream("C:/Users/bhlgo/Desktop/大数据学习.txt");
		//参数1:读本地文件,input即读
		//参数2:写hdfs,output即写。
		IOUtils.copy(fis, fos);
	}
	
	//通过流的方式从hdfs上面下载文件。
	@Test
	public void downloadFile() throws IllegalArgumentException, IOException{
		//先获取一个文件的输入流----针对hdfs上的
		//input输入即读操作。
		FSDataInputStream fis = fs.open(new Path("/stream.txt"));
		
		//再构造一个文件的输出流----针对本地的
		//output输出即写
		FileOutputStream fos = new FileOutputStream("C:/Users/bhlgo/Desktop/stream.txt");
		
		//再将输入流中数据传输到输出流
		//拷贝操作
		IOUtils.copy(fis, fos);
	}
	
	
	//hdfs支持随机定位进行文件读取，而且可以方便地读取指定长度
	//	用于上层分布式运算框架并发处理数据
	//文件的随机读写操作
	@Test
	public void randomAccess() throws IllegalArgumentException, IOException{
		//先获取一个文件的输入流----针对hdfs上的
		//input输入即读
		FSDataInputStream fis = fs.open(new Path("/大数据学习.txt"));
		
		//可以将流的起始偏移量进行自定义
		//seek方法是读的起始位置定位5
		fis.seek(5);
		
		//output输出即写
		FileOutputStream fos = new FileOutputStream("C:/Users/bhlgo/Desktop/output.txt");
		
		//拷贝操作
		IOUtils.copy(fis, fos);
	}

	
	//显示hdfs上文件的内容
	@Test
	public void catFile() throws IllegalArgumentException, IOException{
		FSDataInputStream fis = fs.open(new Path("/大数据学习.txt"));
		
		IOUtils.copy(fis, System.out);
		
		//IOUtils.copyBytes(fis, System.out, 1024);
	}
	
}
