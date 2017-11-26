package com.bie.lesson03.hadoopRpc.client;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;

import com.bie.lesson03.hadoopRpc.protocol.ClientNamenodeProtocol;

/** 
* @author  Author:别先生 
* @date Date:2017年11月26日 下午3:27:00 
*
* 1:hdfs的客户端
* 2:hadoop的rpc像调本地方法一样调用远程的。
*/
public class HdfsClient {
 
	public static void main(String[] args) throws IOException {
		//rpc拿到动态代理对象,然后通过网络请求
		ClientNamenodeProtocol proxy = RPC.getProxy(
				ClientNamenodeProtocol.class, 
				1L, 
				new InetSocketAddress("localhost",8888), 
				new Configuration());
		//模拟获取到副本数量和块block
		//方法，过程有点慢。
		String metaData = proxy.getMetaData("/模拟.txt");
		System.out.println(metaData);
	}
}
